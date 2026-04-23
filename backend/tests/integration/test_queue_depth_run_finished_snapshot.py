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
async def test_run_finished_snapshot_reports_zero_queue_depth_for_completed_graph(monkeypatch) -> None:
	"""Integration regression: completed runs should not carry residual queue depth in final metrics."""
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "src":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [10, 20, 30], "meta": {"ok": True}},
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
			{"id": "src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "sink_once",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "once", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
		],
		"edges": [
			{
				"id": "e_int_once",
				"source": "src",
				"target": "sink_once",
				"targetHandle": "in",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 16}},
			}
		],
	}

	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-int-final-depth",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-int-final-depth", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-int-final-depth",
	)

	finished = [evt for evt in events if str(evt.get("type") or "") == "run_finished"]
	assert finished, "expected run_finished event"
	assert str(finished[-1].get("status") or "") == "succeeded"

	queue_metrics = [evt for evt in events if str(evt.get("type") or "") == "queue_metrics"]
	assert queue_metrics, "expected queue_metrics events"
	final_metrics = queue_metrics[-1].get("metrics") if isinstance(queue_metrics[-1].get("metrics"), dict) else {}
	edges = final_metrics.get("edges") if isinstance(final_metrics.get("edges"), dict) else {}

	assert int(final_metrics.get("globalDepth") or 0) == 0
	assert int(((edges.get("e_int_once:in") or {}).get("depth") or 0)) == 0
