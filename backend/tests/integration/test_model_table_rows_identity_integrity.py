from __future__ import annotations

import asyncio
import importlib
import json
import sys
import types

import pytest

from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


def _ensure_duckdb_stub() -> None:
	if "duckdb" not in sys.modules:
		sys.modules["duckdb"] = types.SimpleNamespace()


@pytest.mark.asyncio
async def test_table_rows_model_single_item_preserves_identity_end_to_end(monkeypatch) -> None:
	"""Conclusive guard for streaming item identity drift:
	- Source emits one JSON list artifact with N distinct items
	- Model consumes as single_item(json_items)
	- Downstream single_item node must consume the exact per-item model artifacts
	"""
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setenv("STRICT_SCHEMA_EDGE_CHECKS", "0")
	monkeypatch.setenv("STRICT_SCHEMA_EDGE_CHECKS_V2", "0")

	row_ids = [f"job_{idx}" for idx in range(6)]
	model_seen_row_ids: list[str] = []
	model_seen_indices: list[int] = []
	sink_upstream_artifact_ids: list[str] = []
	sink_consumed_job_ids: list[str] = []

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={
				"kind": "json",
				"payload": [{"id": rid, "title": f"Title {rid}"} for rid in row_ids],
				"meta": {},
			},
		)

	async def _fake_exec_model_like_tool(run_id, node, context, upstream_artifact_ids=None):
		params = ((node.get("data", {}) or {}).get("params", {}) or {})
		work_item = (params.get("_work_item") or {}) if isinstance(params, dict) else {}
		preview = work_item.get("itemPreview") if isinstance(work_item, dict) else {}
		item_index = int(work_item.get("itemIndex") or 0)
		row_id = str((preview or {}).get("id") or f"unknown_{item_index}")
		model_seen_row_ids.append(row_id)
		model_seen_indices.append(item_index)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={
				"kind": "json",
				"payload": {
					"job": {"id": row_id, "title": f"Title {row_id}", "company_name": f"Co {row_id}"},
					"job_id": row_id,
					"pass": True,
					"score": 85,
					"reason": f"match:{row_id}",
				},
				"meta": {},
			},
		)

	async def _fake_exec_sink_tool(run_id, node, context, upstream_artifact_ids=None):
		upstream_artifact_ids = list(upstream_artifact_ids or [])
		if upstream_artifact_ids:
			aid = str(upstream_artifact_ids[0])
			sink_upstream_artifact_ids.append(aid)
			payload = await context.artifact_store.read(aid)
			obj = json.loads(payload.decode("utf-8", errors="replace"))
			if isinstance(obj, dict) and isinstance(obj.get("payload"), dict):
				sink_consumed_job_ids.append(str((obj.get("payload") or {}).get("job_id") or ""))
			else:
				sink_consumed_job_ids.append(str((obj or {}).get("job_id") or ""))
		# Slow sink slightly to create queue/backpressure and stress identity handling.
		await asyncio.sleep(0.01)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {}},
		)

	async def _dispatching_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "src":
			return await _fake_exec_source(run_id, node, context, upstream_artifact_ids=upstream_artifact_ids)
		if node_id == "model_score":
			return await _fake_exec_model_like_tool(
				run_id, node, context, upstream_artifact_ids=upstream_artifact_ids
			)
		return await _fake_exec_sink_tool(run_id, node, context, upstream_artifact_ids=upstream_artifact_ids)

	monkeypatch.setattr(run_mod, "exec_tool", _dispatching_exec_tool)

	graph = {
		"nodes": [
			{
				"id": "src",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
			{
				"id": "model_score",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
			{
				"id": "job_desc",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
		],
		"edges": [
			{
				"id": "e_src_model",
				"source": "src",
				"target": "model_score",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 16}},
			},
			{
				"id": "e_model_job",
				"source": "model_score",
				"target": "job_desc",
				"data": {"mode": "work"},
			},
		],
	}
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-model-table-identity",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-model-table-identity", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-model-table-identity",
	)

	model_outputs = [
		str(evt.get("artifactId") or "")
		for evt in events
		if evt.get("type") == "node_output" and str(evt.get("nodeId") or "") == "model_score"
	]
	if not model_outputs:
		node_finished = [
			{
				"nodeId": str(evt.get("nodeId") or ""),
				"status": str(evt.get("status") or ""),
				"error": str(evt.get("error") or ""),
			}
			for evt in events
			if evt.get("type") == "node_finished"
		]
		event_types = [str(evt.get("type") or "") for evt in events]
		logs = [str(evt.get("message") or "") for evt in events if evt.get("type") == "log"]
		raise AssertionError(
			f"no model outputs; events={len(events)} types={event_types[:30]} logs={logs[:10]} node_finished={node_finished}"
		)
	assert len(model_outputs) == len(row_ids)
	assert len(set(model_outputs)) == len(row_ids), "Model outputs must be distinct per streamed row"

	assert len(model_seen_row_ids) == len(row_ids)
	assert sorted(model_seen_row_ids) == sorted(row_ids)
	assert sorted(model_seen_indices) == list(range(len(row_ids)))

	assert len(sink_upstream_artifact_ids) == len(row_ids)
	assert len(set(sink_upstream_artifact_ids)) == len(row_ids), (
		"Downstream consumed duplicate model artifact IDs; this indicates identity collapse"
	)
	assert sorted(sink_upstream_artifact_ids) == sorted(model_outputs)
	assert sorted(sink_consumed_job_ids) == sorted(row_ids), (
		"Downstream consumed repeated job_id values; per-item identity was not preserved"
	)
