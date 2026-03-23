from __future__ import annotations

import importlib
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
async def test_job_stream_is_gated_single_item_with_rejects(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	select_invocations: list[int] = []
	generate_invocations: list[int] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "jobs":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={
					"kind": "json",
					"payload": [
						{"job_id": "j1"},
						{"job_id": "j2"},
						{"job_id": "j3"},
					],
					"meta": {},
				},
			)
		if node_id == "select":
			select_invocations.append(len(select_invocations) + 1)
			reject = len(select_invocations) % 2 == 0
			return NodeOutput(
				status="failed" if reject else "succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={
					"kind": "json",
					"payload": {"selected": not reject, "index": len(select_invocations)},
					"meta": {"reject": reject},
				},
			)
		if node_id == "generate":
			generate_invocations.append(len(generate_invocations) + 1)
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {"application": len(generate_invocations)}, "meta": {}},
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
			{"id": "jobs", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "select",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
				},
			},
			{
				"id": "generate",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
				},
			},
		],
		"edges": [
			{
				"id": "e_jobs_select",
				"source": "jobs",
				"target": "select",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 16}},
			},
			{
				"id": "e_select_generate",
				"source": "select",
				"target": "generate",
				"data": {"mode": "work", "work": {"item_mode": "artifact", "max_items": 16}},
			},
		],
	}
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-job-gating",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-job-gating", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-job-gating",
	)
	assert len(select_invocations) == 3
	assert len(generate_invocations) >= 1
	reject_events = [evt for evt in events if evt.get("type") == "node_reject" and evt.get("nodeId") == "select"]
	assert reject_events
	decision_events = [evt for evt in events if evt.get("type") == "node_decision" and evt.get("nodeId") == "select"]
	assert len(decision_events) == 3
	decision_values = [str(evt.get("decision") or "") for evt in decision_events]
	assert decision_values.count("reject") == 1
