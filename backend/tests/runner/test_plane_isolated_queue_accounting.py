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
async def test_queue_metrics_include_plane_and_handle_counters(monkeypatch) -> None:
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
					"processingPolicy": {"consume_mode": "single_item"},
				},
			},
		],
		"edges": [{"id": "e_work", "source": "a", "target": "b", "targetHandle": "in", "data": {"mode": "work"}}],
	}
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-plane-accounting",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-plane-accounting", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-plane-accounting",
	)
	queue_events = [evt for evt in events if evt.get("type") == "queue_metrics"]
	assert queue_events, "expected queue_metrics event(s)"
	last = queue_events[-1]
	item_metrics = (last.get("runtimeItemMetrics") or {}) if isinstance(last, dict) else {}
	by_plane = item_metrics.get("byPlane") or {}
	assert int(((by_plane.get("work") or {}).get("itemsEnqueued") or 0)) >= 1
	assert "param" in by_plane and "control" in by_plane
	by_handle = item_metrics.get("byHandle") or {}
	assert "b:in" in by_handle
	assert int(((by_handle.get("b:in") or {}).get("itemsDequeued") or 0)) >= 1
