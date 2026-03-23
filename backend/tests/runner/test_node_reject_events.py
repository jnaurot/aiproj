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
async def test_node_reject_emits_node_decision_and_does_not_fail_run(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	executed: list[str] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node["id"])
		executed.append(node_id)
		if node_id == "b":
			return NodeOutput(
				status="failed",
				metadata=None,
				execution_time_ms=1.0,
				error="rejected",
				data={"kind": "json", "payload": {"reject": True}, "meta": {"reject": True}},
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
			{"id": "a", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "c", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [
			{"id": "e1", "source": "a", "target": "b", "data": {"mode": "work"}},
			{"id": "e2", "source": "b", "target": "c", "data": {"mode": "work"}},
		],
	}
	events: list[dict] = []
	bus = RunEventBus("run-qflow-019-reject", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-qflow-019-reject",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-qflow-019-reject",
	)

	decision_events = [evt for evt in events if evt.get("type") == "node_decision" and evt.get("nodeId") == "b"]
	assert decision_events
	assert decision_events[-1].get("decision") == "reject"
	# rejected branch does not continue to downstream node c
	assert "c" not in executed
	finished = [evt for evt in events if evt.get("type") == "run_finished"]
	assert finished and finished[-1].get("status") == "succeeded"
