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
async def test_runtime_emits_handle_control_signals_for_work_inputs(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "a", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "b",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
				},
			},
		],
		"edges": [{"id": "e1", "source": "a", "target": "b"}],
	}
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-handle-signals",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-handle-signals", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-handle-signals",
	)
	handle_events = [
		evt
		for evt in events
		if evt.get("type") == "control_signal"
		and str(evt.get("nodeId") or "") == "b"
		and str(evt.get("handle") or "") == "in"
	]
	signals = [str(evt.get("signal") or "") for evt in handle_events]
	assert "busy" in signals
	assert "ready" in signals
