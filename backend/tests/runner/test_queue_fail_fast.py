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
async def test_fatal_fail_fast_stops_fanout_progress(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	executed: list[str] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node["id"])
		executed.append(node_id)
		if node_id == "root":
			return NodeOutput(status="failed", metadata=None, execution_time_ms=1.0, data={"error": "boom"})
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	monkeypatch.setenv("RUNNER_MAX_CONCURRENCY", "1")
	graph = {
		"nodes": [
			{
				"id": "root",
				"data": {
					"kind": "tool",
					"fatal": True,
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
			{"id": "a", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [
			{"id": "e1", "source": "root", "target": "a"},
			{"id": "e2", "source": "root", "target": "b"},
		],
	}
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-fail-fast",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-fail-fast", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-fail-fast",
	)
	assert executed == ["root"]
	finished = [e for e in events if e.get("type") == "run_finished"]
	assert finished and finished[-1].get("status") == "failed"
