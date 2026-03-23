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
async def test_queue_metrics_include_runtime_item_metrics(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node["id"])
		if node_id == "a":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [1, 2], "meta": {"ok": True}},
			)
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
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
		],
		"edges": [
			{
				"id": "e1",
				"source": "a",
				"target": "b",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 10}},
			}
		],
	}
	events: list[dict] = []
	bus = RunEventBus("run-qflow-020-metrics", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-qflow-020-metrics",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-qflow-020-metrics",
	)

	queue_metric_events = [evt for evt in events if evt.get("type") == "queue_metrics"]
	assert queue_metric_events
	last_evt = queue_metric_events[-1]
	rt = last_evt.get("runtimeItemMetrics") or {}
	assert int(rt.get("itemsEnqueued") or 0) >= 2
	assert int(rt.get("itemsDequeued") or 0) >= 2
	by_plane = rt.get("byPlane") or {}
	assert int(((by_plane.get("work") or {}).get("itemsEnqueued") or 0)) >= 2
	by_handle = rt.get("byHandle") or {}
	assert any(str(key).endswith(":in") for key in by_handle.keys())
