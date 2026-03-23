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
async def test_reject_is_fatal_when_enabled(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node["id"])
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
			{
				"id": "b",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}, "reject_fatal": True},
				},
			},
		],
		"edges": [{"id": "e1", "source": "a", "target": "b", "data": {"mode": "work"}}],
	}
	events: list[dict] = []
	bus = RunEventBus("run-reject-fatal", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-reject-fatal",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-reject-fatal",
	)

	finished = [evt for evt in events if evt.get("type") == "run_finished"]
	assert finished
	assert finished[-1].get("status") == "failed"
	assert any("reject treated as fatal" in str(evt.get("message") or "") for evt in events if evt.get("type") == "log")

