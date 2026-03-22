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


def _graph(*, fatal_fail: bool) -> dict:
	fail_data = {
		"kind": "tool",
		"label": "fail_root",
		"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
	}
	if fatal_fail:
		fail_data["fatal"] = True
	return {
		"nodes": [
			{"id": "fail_root", "data": fail_data},
			{
				"id": "fail_down",
				"data": {"kind": "tool", "label": "fail_down", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}},
			},
			{
				"id": "ok_root",
				"data": {"kind": "tool", "label": "ok_root", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}},
			},
			{
				"id": "ok_down",
				"data": {"kind": "tool", "label": "ok_down", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}},
			},
		],
		"edges": [
			{"id": "e_fail", "source": "fail_root", "target": "fail_down"},
			{"id": "e_ok", "source": "ok_root", "target": "ok_down"},
		],
	}


@pytest.mark.asyncio
async def test_localized_failure_allows_other_nodes(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	executed: list[str] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node["id"])
		executed.append(node_id)
		if node_id == "fail_root":
			return NodeOutput(status="failed", metadata=None, execution_time_ms=1.0, data={"error": "boom"})
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	monkeypatch.setenv("RUNNER_MAX_CONCURRENCY", "1")
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-localized",
		graph=_graph(fatal_fail=False),
		run_from=None,
		bus=RunEventBus("run-localized", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-localized",
	)
	assert "fail_root" in executed
	assert "ok_root" in executed
	assert "ok_down" in executed


@pytest.mark.asyncio
async def test_fatal_failure_stops_remaining_nodes(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	executed: list[str] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node["id"])
		executed.append(node_id)
		if node_id == "fail_root":
			return NodeOutput(status="failed", metadata=None, execution_time_ms=1.0, data={"error": "boom"})
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	monkeypatch.setenv("RUNNER_MAX_CONCURRENCY", "1")
	await run_mod.run_graph(
		run_id="run-fatal",
		graph=_graph(fatal_fail=True),
		run_from=None,
		bus=RunEventBus("run-fatal"),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-fatal",
	)
	assert "fail_root" in executed
	assert "fail_down" not in executed
	assert "ok_down" not in executed
