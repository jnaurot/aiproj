from __future__ import annotations

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
async def test_downstream_soft_fail_does_not_collapse_middle_node_identity(monkeypatch) -> None:
	"""Regression guard for identity collapse observed after downstream failures.

	Flow:
	- source emits one JSON list artifact with N rows
	- model_score consumes json_items(single_item) and emits one JSON object per row
	- job_description consumes single_item artifacts from model_score
	- resume_builder (downstream) fails once with on_error=skip_failed, then continues

	Expectation:
	- job_description must consume all distinct model_score outputs, not repeatedly
	  resolve to the latest upstream binding.
	"""
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setenv("STRICT_SCHEMA_EDGE_CHECKS", "0")
	monkeypatch.setenv("STRICT_SCHEMA_EDGE_CHECKS_V2", "0")

	row_ids = [f"job_{idx}" for idx in range(6)]
	model_output_ids: list[str] = []
	job_desc_seen_ids: list[str] = []
	job_desc_input_artifact_ids: list[str] = []
	resume_builder_calls = {"count": 0}

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		params = ((node.get("data", {}) or {}).get("params", {}) or {})

		if node_id == "source_jobs":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={
					"kind": "json",
					"payload": [{"id": rid, "title": f"title-{rid}"} for rid in row_ids],
					"meta": {},
				},
			)

		if node_id == "model_score":
			work_item = (params.get("_work_item") or {}) if isinstance(params, dict) else {}
			preview = work_item.get("itemPreview") if isinstance(work_item, dict) else {}
			row_id = str((preview or {}).get("id") or "")
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={
					"kind": "json",
					"payload": {
						"job": {"id": row_id, "title": f"title-{row_id}", "company_name": f"co-{row_id}"},
						"job_id": row_id,
						"pass": True,
						"score": 85,
						"reason": f"ok-{row_id}",
					},
					"meta": {},
				},
			)

		if node_id == "job_description":
			ids = [str(aid) for aid in (upstream_artifact_ids or []) if str(aid)]
			if ids:
				job_desc_input_artifact_ids.append(ids[0])
				payload = await context.artifact_store.read(ids[0])
				obj = json.loads(payload.decode("utf-8", errors="replace"))
				root = obj.get("payload") if isinstance(obj, dict) and isinstance(obj.get("payload"), dict) else obj
				job_desc_seen_ids.append(str((root or {}).get("job_id") or ""))
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {"ok": True, "job_id": job_desc_seen_ids[-1] if job_desc_seen_ids else ""}, "meta": {}},
			)

		if node_id == "resume_builder":
			resume_builder_calls["count"] += 1
			if resume_builder_calls["count"] == 1:
				raise RuntimeError("intentional soft-fail")
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {"ok": True}, "meta": {}},
			)

		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	graph = {
		"nodes": [
			{"id": "source_jobs", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "model_score",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
			{
				"id": "job_description",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
			{
				"id": "resume_builder",
				"data": {
					"kind": "tool",
					"processingPolicy": {
						"consume_mode": "single_item",
						"batch_size": 1,
						"max_inflight": 1,
						"on_error": "skip_failed",
					},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
		],
		"edges": [
			{
				"id": "e_src_model",
				"source": "source_jobs",
				"target": "model_score",
				"targetHandle": "in",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 16}},
			},
			{
				"id": "e_model_job",
				"source": "model_score",
				"target": "job_description",
				"targetHandle": "in",
				"data": {"mode": "work"},
			},
			{
				"id": "e_job_resume",
				"source": "job_description",
				"target": "resume_builder",
				"targetHandle": "in",
				"data": {"mode": "work"},
			},
		],
	}

	events: list[dict] = []
	bus = RunEventBus("run-soft-fail-identity", on_emit=lambda evt: events.append(dict(evt)))
	store = MemoryArtifactStore()
	await run_mod.run_graph(
		run_id="run-soft-fail-identity",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=store,
		cache=ExecutionCache(),
		graph_id="graph-soft-fail-identity",
	)

	model_output_ids = [
		str(evt.get("artifactId") or "")
		for evt in events
		if evt.get("type") == "node_output" and str(evt.get("nodeId") or "") == "model_score"
	]
	assert len(model_output_ids) == len(row_ids), "model_score should emit one output per streamed item"
	assert len(set(model_output_ids)) == len(row_ids), "model_score outputs must be unique per item"

	assert len(job_desc_input_artifact_ids) == len(row_ids), (
		"job_description should consume one input per upstream model item even with downstream soft-fail"
	)
	assert len(set(job_desc_input_artifact_ids)) == len(row_ids), (
		"job_description consumed duplicate upstream artifact IDs (identity collapse)"
	)
	assert sorted(job_desc_input_artifact_ids) == sorted(model_output_ids), (
		"job_description did not consume the exact set of model_score outputs"
	)
	assert sorted(job_desc_seen_ids) == sorted(row_ids), (
		"job_description payload identity drifted (repeated/incorrect job_id values)"
	)

