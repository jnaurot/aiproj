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
async def test_port_runtime_param_control_warning_golden_once_per_edge(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	seen_batches: list[int] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "jobs":
			payload = [{"job_id": 1}, {"job_id": 2}]
		elif node_id in {"param_src", "control_src"}:
			payload = []
		elif node_id == "select":
			work_batch = (((node.get("data", {}) or {}).get("params", {}) or {}).get("_work_batch") or [])
			seen_batches.append(len(work_batch))
			payload = {"ok": True}
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
			{"id": "jobs", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "param_src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "control_src",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"portDeclarations": {"out": {"control_out": {"plane": "control", "required": False, "cardinality": "many"}}},
				},
			},
			{
				"id": "select",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"portDeclarations": {"in": {"control_in": {"plane": "control", "required": False, "cardinality": "many"}}},
				},
			},
		],
		"edges": [
			{"id": "e_work", "source": "jobs", "target": "select", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 16}}},
			{"id": "e_param", "source": "param_src", "target": "select", "targetHandle": "param_filters", "data": {"mode": "param"}},
			{
				"id": "e_control",
				"source": "control_src",
				"sourceHandle": "control_out",
				"target": "select",
				"targetHandle": "control_in",
				"data": {"mode": "control"},
			},
		],
	}
	await run_mod.run_graph(
		run_id="run-port-runtime-param-control-warning",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-port-runtime-param-control-warning", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-port-runtime-param-control-warning",
	)
	assert seen_batches == [1, 1]
	warnings = [evt for evt in events if str(evt.get("type") or "") == "node_input_warning"]
	assert len(warnings) == 2
	warning_edges = sorted([str(evt.get("edgeId") or "") for evt in warnings])
	assert warning_edges == ["e_control", "e_param"]
	finished = [evt for evt in events if str(evt.get("type") or "") == "run_finished"]
	assert finished
	assert str(finished[-1].get("status") or "") == "succeeded"
