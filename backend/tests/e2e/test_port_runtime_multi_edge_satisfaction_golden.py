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
async def test_port_runtime_multi_edge_satisfaction_golden_partial_then_all(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	seen_batches: list[int] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "jobs_a":
			payload = [{"job_id": 1}, {"job_id": 2}, {"job_id": 3}]
		elif node_id == "jobs_b":
			payload = [{"job_id": 4}]
		elif node_id == "select":
			work_batch = (((node.get("data", {}) or {}).get("params", {}) or {}).get("_work_batch") or [])
			seen_batches.append(len(work_batch))
			payload = {"selected": len(work_batch)}
		else:
			payload = {"ok": True}
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": payload, "meta": {}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	events: list[dict] = []
	graph = {
		"nodes": [
			{"id": "jobs_a", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "jobs_b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "select",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"processingPolicy": {"consume_mode": "batch", "batch_size": 2, "max_inflight": 1},
				},
			},
		],
		"edges": [
			{"id": "e_a", "source": "jobs_a", "target": "select", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 16}}},
			{"id": "e_b", "source": "jobs_b", "target": "select", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 16}}},
		],
	}
	await run_mod.run_graph(
		run_id="run-port-runtime-sat-golden",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-port-runtime-sat-golden", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-port-runtime-sat-golden",
	)
	assert seen_batches[:2] == [2, 2]
	sat = [evt for evt in events if str(evt.get("type") or "") == "node_handle_satisfaction" and str(evt.get("nodeId") or "") == "select"]
	assert sat
	assert any(str(evt.get("status") or "") == "partial" for evt in sat) or any(
		str(evt.get("status") or "") == "all" for evt in sat
	)
	finished = [evt for evt in events if str(evt.get("type") or "") == "run_finished"]
	assert finished
	assert str(finished[-1].get("status") or "") == "succeeded"
