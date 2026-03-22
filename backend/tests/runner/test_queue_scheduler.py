from __future__ import annotations

import importlib
import sys
import types

import pytest

from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput
from app.runner.scheduler import build_queue_schedule


def _ensure_duckdb_stub() -> None:
	if "duckdb" not in sys.modules:
		sys.modules["duckdb"] = types.SimpleNamespace()


def test_build_queue_schedule_deterministic_ready_and_adjacency() -> None:
	plan = build_queue_schedule(
		nodes=["a", "b", "c", "d"],
		edges=[("a", "c"), ("b", "c"), ("c", "d")],
		order=["a", "b", "c", "d"],
	)
	assert plan.ready == ["a", "b"]
	assert plan.indeg["c"] == 2
	assert plan.adj["c"] == ["d"]


@pytest.mark.asyncio
async def test_run_graph_uses_queue_scheduler_path(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	executed: list[str] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		executed.append(str(node["id"]))
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"node": node["id"]}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "a", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "c", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [
			{"id": "e1", "source": "a", "target": "c"},
			{"id": "e2", "source": "b", "target": "c"},
		],
	}
	events: list[dict] = []
	bus = RunEventBus("run-qflow-001", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-qflow-001",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-qflow-001",
	)

	logs = [str(e.get("message") or "") for e in events if e.get("type") == "log"]
	assert any("[scheduler] queue start" in msg for msg in logs)
	assert not any("[scheduler] level " in msg for msg in logs)
	assert executed[-1] == "c"
