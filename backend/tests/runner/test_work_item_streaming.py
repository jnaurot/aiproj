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
async def test_work_item_streaming_single_item_consumes_each_json_item(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	consumed: list[int] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node["id"])
		if node_id == "a":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [1, 2, 3], "meta": {"ok": True}},
			)
		work_item = ((node.get("data", {}).get("params", {}) or {}).get("_work_item") or {})
		preview = work_item.get("itemPreview")
		if isinstance(preview, int):
			consumed.append(preview)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"seen": preview}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{
				"id": "a",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
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
				"data": {"mode": "work", "queue": {"max": 1000, "overflow": "block"}, "work": {"item_mode": "json_items", "max_items": 10}},
			}
		],
	}
	events: list[dict] = []
	bus = RunEventBus("run-qflow-017-single", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-qflow-017-single",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-qflow-017-single",
	)

	assert sorted(consumed) == [1, 2, 3]
	decisions = [evt for evt in events if evt.get("type") == "node_decision" and evt.get("nodeId") == "b"]
	assert len(decisions) >= 3


@pytest.mark.asyncio
async def test_work_item_streaming_batch_mode_respects_batch_size(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	batches: list[int] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node["id"])
		if node_id == "a":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [1, 2, 3, 4, 5], "meta": {"ok": True}},
			)
		work_batch = ((node.get("data", {}).get("params", {}) or {}).get("_work_batch") or [])
		batches.append(len(work_batch))
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"batch_len": len(work_batch)}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{
				"id": "a",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
			{
				"id": "b",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "batch", "batch_size": 2, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
		],
		"edges": [
			{
				"id": "e1",
				"source": "a",
				"target": "b",
				"data": {"mode": "work", "queue": {"max": 1000, "overflow": "block"}, "work": {"item_mode": "json_items", "max_items": 10}},
			}
		],
	}
	bus = RunEventBus("run-qflow-018-batch")
	await run_mod.run_graph(
		run_id="run-qflow-018-batch",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-qflow-018-batch",
	)

	# 5 items with batch_size=2 => 3 executions: 2,2,1
	assert batches[:3] == [2, 2, 1]


@pytest.mark.asyncio
async def test_work_item_streaming_ignores_param_edges_for_queue_enq(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	consumed: list[int] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node["id"])
		if node_id == "a":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [1, 2, 3], "meta": {"ok": True}},
			)
		if node_id == "p":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {"k": "v"}, "meta": {"ok": True}},
			)
		work_item = ((node.get("data", {}).get("params", {}) or {}).get("_work_item") or {})
		preview = work_item.get("itemPreview")
		if isinstance(preview, int):
			consumed.append(preview)
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
			{"id": "p", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
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
				"id": "e_work",
				"source": "a",
				"target": "b",
				"targetHandle": "in",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 10}},
			},
			{
				"id": "e_param",
				"source": "p",
				"target": "b",
				"targetHandle": "param_config",
				"data": {"mode": "param"},
			},
		],
	}
	events: list[dict] = []
	bus = RunEventBus("run-qflow-018-mixed", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-qflow-018-mixed",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-qflow-018-mixed",
	)

	assert sorted(consumed) == [1, 2, 3]
	queue_events = [evt for evt in events if evt.get("type") == "queue_metrics"]
	assert queue_events
	last_metrics = (queue_events[-1].get("runtimeItemMetrics") or {}) if isinstance(queue_events[-1], dict) else {}
	assert int(last_metrics.get("itemsEnqueued", 0)) == 3


@pytest.mark.asyncio
async def test_work_item_streaming_respects_work_max_items_cap(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	consumed: list[int] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node["id"])
		if node_id == "a":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [1, 2, 3], "meta": {"ok": True}},
			)
		work_item = ((node.get("data", {}).get("params", {}) or {}).get("_work_item") or {})
		preview = work_item.get("itemPreview")
		if isinstance(preview, int):
			consumed.append(preview)
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
				"id": "e_work",
				"source": "a",
				"target": "b",
				"targetHandle": "in",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 2}},
			},
		],
	}
	events: list[dict] = []
	bus = RunEventBus("run-qflow-018-max-items", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-qflow-018-max-items",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-qflow-018-max-items",
	)

	assert sorted(consumed) == [1, 2]
	queue_events = [evt for evt in events if evt.get("type") == "queue_metrics"]
	assert queue_events
	last_metrics = (queue_events[-1].get("runtimeItemMetrics") or {}) if isinstance(queue_events[-1], dict) else {}
	assert int(last_metrics.get("itemsEnqueued", 0)) == 2
