from __future__ import annotations

import asyncio
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
async def test_single_item_downstream_requeues_when_new_items_arrive_without_rebuild(monkeypatch) -> None:
	"""Regression guard:
	Downstream single_item consumers must be re-enqueued when new upstream work items
	arrive, not only during late scheduler ready-rebuild.
	"""
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		work_item = ((node.get("data", {}).get("params", {}) or {}).get("_work_item") or {})
		preview = work_item.get("itemPreview")

		if node_id == "src":
			# One artifact expanded to multiple items for stage_a.
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [1, 2, 3], "meta": {"ok": True}},
			)

		if node_id == "stage_a":
			# Make upstream producer slower than downstream to force downstream into waiting
			# between arrivals; subsequent arrivals should wake downstream immediately.
			await asyncio.sleep(0.03)
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=30.0,
				data={"kind": "json", "payload": {"v": preview}, "meta": {"ok": True}},
			)

		# stage_b
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"seen": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	graph = {
		"nodes": [
			{
				"id": "src",
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}},
			},
			{
				"id": "stage_a",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
			{
				"id": "stage_b",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
		],
		"edges": [
			{
				"id": "e_src_a",
				"source": "src",
				"target": "stage_a",
				"targetHandle": "in",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 8}},
			},
			{
				"id": "e_a_b",
				"source": "stage_a",
				"target": "stage_b",
				"targetHandle": "in",
				"data": {"mode": "work", "work": {"item_mode": "artifact", "max_items": 8}},
			},
		],
	}

	events: list[dict] = []
	bus = RunEventBus("run-control-plane-requeue-regression", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-control-plane-requeue-regression",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-control-plane-requeue-regression",
	)

	blocked_indices = [
		idx
		for idx, evt in enumerate(events)
		if str(evt.get("type") or "") == "node_blocked"
		and str(evt.get("nodeId") or "") == "stage_b"
		and str(evt.get("reasonCode") or "") == "WAITING_REQUIRED_INPUT"
	]
	assert blocked_indices, "expected stage_b to emit WAITING_REQUIRED_INPUT at least once"

	first_blocked_idx = blocked_indices[0]
	later_enqueues = [
		idx
		for idx, evt in enumerate(events)
		if idx > first_blocked_idx
		and str(evt.get("type") or "") == "control_signal"
		and str(evt.get("signal") or "") == "item_enqueued"
		and str(evt.get("nodeId") or "") == "stage_b"
	]
	assert later_enqueues, "expected new stage_b items to enqueue after it was blocked waiting"

	# In this linear flow, downstream should be naturally re-admitted on new enqueue.
	# If scheduler needs "ready rebuild recovered runnable nodes=stage_b", admission missed
	# real-time enqueue signals and is effectively control-plane delayed.
	rebuild_logs = [
		evt
		for evt in events
		if str(evt.get("type") or "") == "log"
		and "ready rebuild recovered runnable nodes=stage_b" in str(evt.get("message") or "")
	]
	assert not rebuild_logs, "stage_b required late scheduler rebuild instead of enqueue-driven requeue"
