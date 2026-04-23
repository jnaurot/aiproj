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
async def test_once_mode_consumption_leaves_zero_final_queue_depth(monkeypatch) -> None:
	"""Regression: once-mode downstream should not finish with residual queue depth."""
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "src":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": ["a", "b", "c"], "meta": {"ok": True}},
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
			{
				"id": "src",
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}},
			},
			{
				"id": "once_consumer",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "once", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
		],
		"edges": [
			{
				"id": "e_src_once",
				"source": "src",
				"target": "once_consumer",
				"targetHandle": "in",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 16}},
			}
		],
	}

	events: list[dict] = []
	bus = RunEventBus("run-once-depth", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-once-depth",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-once-depth",
	)

	queue_metrics = [evt for evt in events if str(evt.get("type") or "") == "queue_metrics"]
	assert queue_metrics, "expected queue_metrics events"
	final_metrics = queue_metrics[-1].get("metrics") if isinstance(queue_metrics[-1].get("metrics"), dict) else {}
	assert int(final_metrics.get("globalDepth") or 0) == 0
	edges = final_metrics.get("edges") if isinstance(final_metrics.get("edges"), dict) else {}
	edge_key = "e_src_once:in"
	assert int(((edges.get(edge_key) or {}).get("depth") or 0)) == 0
