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
async def test_job_at_a_time_pipeline_golden_deterministic_reject_and_accept_flow(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	select_counter = {"value": 0}

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "jobs":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [{"job_id": "j1"}, {"job_id": "j2"}, {"job_id": "j3"}], "meta": {}},
			)
		if node_id == "select":
			select_counter["value"] += 1
			idx = select_counter["value"]
			reject = idx % 2 == 0
			return NodeOutput(
				status="failed" if reject else "succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {"job_id": f"j{idx}"}, "meta": {"reject": reject}},
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
				"data": {"mode": "work"},
			},
		],
	}
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-job-at-a-time-golden",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-job-at-a-time-golden", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-job-at-a-time-golden",
	)
	decisions = [
		str(evt.get("decision") or "")
		for evt in events
		if evt.get("type") == "node_decision" and str(evt.get("nodeId") or "") == "select"
	]
	assert decisions == ["accept", "reject", "accept"]
	finished = [evt for evt in events if evt.get("type") == "run_finished"]
	assert finished and str(finished[-1].get("status") or "") == "succeeded"
