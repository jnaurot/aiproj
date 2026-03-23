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
async def test_mixed_plane_golden_flow_succeeds_with_per_port_policies(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"node": str(node.get("id")), "upstream": upstream_artifact_ids or []}, "meta": {}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	graph = {
		"nodes": [
			{"id": "work_src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "param_src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "control_src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "sink",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"processingPolicy": {
						"consume_mode": "once",
						"batch_size": 1,
						"max_inflight": 1,
						"input_handles": {
							"in": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
							"param_filters": {"consume_mode": "once", "batch_size": 1, "max_inflight": 1},
						},
					},
				},
			},
		],
		"edges": [
			{
				"id": "e_work",
				"source": "work_src",
				"sourceHandle": "out",
				"target": "sink",
				"targetHandle": "in",
				"data": {"mode": "work"},
			},
			{
				"id": "e_param",
				"source": "param_src",
				"sourceHandle": "out",
				"target": "sink",
				"targetHandle": "param_filters",
				"data": {
					"mode": "param",
					"contract": {
						"payload": {
							"source": {"keys": ["location", "salary"]},
							"target": {"requiredKeys": ["location"]},
						}
					},
				},
			},
			{
				"id": "e_control",
				"source": "control_src",
				"sourceHandle": "control_out",
				"target": "sink",
				"targetHandle": "control_in",
				"data": {"mode": "control"},
			},
		],
	}

	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-mixed-plane-golden",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-mixed-plane-golden", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-mixed-plane-golden",
	)

	assert any(evt.get("type") == "run_finished" and evt.get("status") == "succeeded" for evt in events)
	assert any(evt.get("type") == "queue_metrics" for evt in events)
	assert not any(
		evt.get("type") == "log" and "TYPE_MISMATCH" in str(evt.get("message") or "")
		for evt in events
	)
