from __future__ import annotations

import importlib
import sys
import types

import pytest

from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus


def _ensure_duckdb_stub() -> None:
	if "duckdb" not in sys.modules:
		sys.modules["duckdb"] = types.SimpleNamespace()


@pytest.mark.asyncio
async def test_control_plane_conflict_fails_fast_before_run() -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	graph = {
		"nodes": [
			{"id": "ctl", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "legacy", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "sink", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [
			{
				"id": "e_control_link",
				"source": "ctl",
				"sourceHandle": "control_out",
				"target": "sink",
				"targetHandle": "control_gate",
				"data": {"mode": "control", "linkKind": "control_link"},
			},
			{
				"id": "e_legacy_control",
				"source": "legacy",
				"sourceHandle": "control_out",
				"target": "sink",
				"targetHandle": "control_gate",
				"data": {"mode": "control", "linkKind": "data_link"},
			},
		],
	}
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-control-safety-conflict",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-control-safety-conflict", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-control-safety-conflict",
	)
	assert any(
		evt.get("type") == "log" and "[CONTROL_LINK_CONFLICT]" in str(evt.get("message") or "")
		for evt in events
	)
	assert any(evt.get("type") == "run_finished" and evt.get("status") == "failed" for evt in events)

