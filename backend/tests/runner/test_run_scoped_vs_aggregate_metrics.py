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
async def test_queue_metrics_events_are_explicitly_run_scoped(monkeypatch) -> None:
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
			{"id": "b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [{"id": "e1", "source": "a", "target": "b"}],
	}
	events_1: list[dict] = []
	await run_mod.run_graph(
		run_id="run-metrics-1",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-metrics-1", on_emit=lambda evt: events_1.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-metrics",
	)
	events_2: list[dict] = []
	await run_mod.run_graph(
		run_id="run-metrics-2",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-metrics-2", on_emit=lambda evt: events_2.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-metrics",
	)
	metrics_1 = [evt for evt in events_1 if evt.get("type") == "queue_metrics"]
	metrics_2 = [evt for evt in events_2 if evt.get("type") == "queue_metrics"]
	assert metrics_1 and metrics_2
	assert all(str(evt.get("scope") or "") == "run" for evt in metrics_1 + metrics_2)
	assert all(str(evt.get("runId") or "") == "run-metrics-1" for evt in metrics_1)
	assert all(str(evt.get("runId") or "") == "run-metrics-2" for evt in metrics_2)
