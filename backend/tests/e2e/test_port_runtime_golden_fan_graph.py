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
async def test_mixed_plane_fan_graph_runtime_golden(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "sink":
			if not upstream_artifact_ids:
				return NodeOutput(
					status="failed",
					metadata=None,
					execution_time_ms=1.0,
					error="missing work input",
					data={"kind": "json", "payload": {"reject": True}, "meta": {"reject": True}},
				)
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {"accepted": True}, "meta": {"ok": True}},
			)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"node": node_id}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "work_src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "param_src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "ctl_src",
				"data": {
					"kind": "tool",
					"portDeclarations": {"out": {"control_out": {"plane": "control", "required": False, "cardinality": "many"}}},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
			{
				"id": "sink",
				"data": {
					"kind": "tool",
					"portDeclarations": {
						"in": {
							"in": {"plane": "work", "required": True, "cardinality": "many"},
							"param_filters": {"plane": "param", "required": False, "cardinality": "many"},
							"control_in": {"plane": "control", "required": False, "cardinality": "many"},
						}
					},
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
			{"id": "fan_out", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [
			{"id": "e_work", "source": "work_src", "target": "sink", "targetHandle": "in", "data": {"mode": "work"}},
			{"id": "e_param", "source": "param_src", "target": "sink", "targetHandle": "param_filters", "data": {"mode": "param"}},
			{
				"id": "e_ctl",
				"source": "ctl_src",
				"sourceHandle": "control_out",
				"target": "sink",
				"targetHandle": "control_in",
				"data": {"mode": "control"},
			},
			{"id": "e_fan", "source": "sink", "target": "fan_out", "targetHandle": "in", "data": {"mode": "work"}},
		],
	}
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-port-runtime-golden-fan",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-port-runtime-golden-fan", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-port-runtime-golden-fan",
	)
	finished = [evt for evt in events if evt.get("type") == "run_finished"]
	assert finished and finished[-1].get("status") == "succeeded"
	queue_events = [evt for evt in events if evt.get("type") == "queue_metrics"]
	assert queue_events
	last_q = queue_events[-1]
	rt = (last_q.get("runtimeItemMetrics") or {}) if isinstance(last_q, dict) else {}
	by_plane = rt.get("byPlane") or {}
	assert int(((by_plane.get("work") or {}).get("itemsEnqueued") or 0)) >= 1
	assert int(((by_plane.get("param") or {}).get("itemsEnqueued") or 0)) == 0
	assert int(((by_plane.get("control") or {}).get("itemsEnqueued") or 0)) == 0

