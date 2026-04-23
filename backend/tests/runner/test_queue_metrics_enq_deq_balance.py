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
@pytest.mark.parametrize(
	"consume_mode,batch_size,expected_min_dequeued",
	[
		("single_item", 1, 3),
		("batch", 2, 2),
	],
)
async def test_queue_metrics_enq_deq_balance_for_streaming_modes(
	monkeypatch, consume_mode: str, batch_size: int, expected_min_dequeued: int
) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "src":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [1, 2, 3], "meta": {"ok": True}},
			)
		work_batch = ((node.get("data", {}).get("params", {}) or {}).get("_work_batch") or [])
		if isinstance(work_batch, list) and work_batch:
			payload = {"size": len(work_batch)}
		else:
			work_item = ((node.get("data", {}).get("params", {}) or {}).get("_work_item") or {})
			payload = {"item": work_item.get("itemPreview")}
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": payload, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	graph = {
		"nodes": [
			{"id": "src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "sink",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": consume_mode, "batch_size": batch_size, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
		],
		"edges": [
			{
				"id": "e_balance",
				"source": "src",
				"target": "sink",
				"targetHandle": "in",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 16}},
			}
		],
	}

	events: list[dict] = []
	await run_mod.run_graph(
		run_id=f"run-balance-{consume_mode}",
		graph=graph,
		run_from=None,
		bus=RunEventBus(f"run-balance-{consume_mode}", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id=f"graph-balance-{consume_mode}",
	)

	queue_metrics = [evt for evt in events if str(evt.get("type") or "") == "queue_metrics"]
	assert queue_metrics, "expected queue_metrics events"
	last = queue_metrics[-1]
	metrics = last.get("metrics") if isinstance(last.get("metrics"), dict) else {}
	runtime = last.get("runtimeItemMetrics") if isinstance(last.get("runtimeItemMetrics"), dict) else {}
	edges = metrics.get("edges") if isinstance(metrics.get("edges"), dict) else {}

	assert int(metrics.get("globalDepth") or 0) == 0
	assert int(((edges.get("e_balance:in") or {}).get("depth") or 0)) == 0
	assert int(runtime.get("itemsEnqueued") or 0) >= 3
	assert int(runtime.get("itemsDequeued") or 0) >= expected_min_dequeued
