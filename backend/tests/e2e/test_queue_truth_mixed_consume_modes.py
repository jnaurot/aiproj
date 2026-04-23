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


class _ForceOffRuntimeRef:
	def get_global_cache_mode(self) -> str:
		return "force_off"


def _last_queue_metrics(events: list[dict]) -> dict:
	queue_events = [evt for evt in events if str(evt.get("type") or "") == "queue_metrics"]
	assert queue_events, "expected queue_metrics events"
	metrics = queue_events[-1].get("metrics") if isinstance(queue_events[-1].get("metrics"), dict) else {}
	assert isinstance(metrics, dict)
	return metrics


@pytest.mark.asyncio
async def test_mixed_consume_modes_force_off_finishes_with_zero_queue_depth(monkeypatch) -> None:
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
		work_item = ((node.get("data", {}).get("params", {}) or {}).get("_work_item") or {})
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={
				"kind": "json",
				"payload": {
					"node": node_id,
					"batch": len(work_batch) if isinstance(work_batch, list) else 0,
					"item": work_item.get("itemPreview"),
				},
				"meta": {"ok": True},
			},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	graph = {
		"nodes": [
			{"id": "src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "once_consumer",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "once", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
			{
				"id": "single_consumer",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
			{
				"id": "batch_consumer",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "batch", "batch_size": 2, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
		],
		"edges": [
			{"id": "e_once", "source": "src", "target": "once_consumer", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items"}}},
			{"id": "e_single", "source": "src", "target": "single_consumer", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items"}}},
			{"id": "e_batch", "source": "src", "target": "batch_consumer", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items"}}},
		],
	}

	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-e2e-mixed-force-off",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-e2e-mixed-force-off", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		runtime_ref=_ForceOffRuntimeRef(),
		graph_id="graph-e2e-mixed-force-off",
	)

	finished = [evt for evt in events if str(evt.get("type") or "") == "run_finished"]
	assert finished and str(finished[-1].get("status") or "") == "succeeded"
	metrics = _last_queue_metrics(events)
	assert int(metrics.get("globalDepth") or 0) == 0
	edges = metrics.get("edges") if isinstance(metrics.get("edges"), dict) else {}
	for edge_key in ("e_once:in", "e_single:in", "e_batch:in"):
		assert int(((edges.get(edge_key) or {}).get("depth") or 0)) == 0
	assert not any(
		str(evt.get("type") or "") == "log"
		and "scheduler_stall_no_runnable_with_pending_queue" in str(evt.get("message") or "")
		for evt in events
	)


@pytest.mark.asyncio
async def test_partial_run_with_cache_hits_preserves_zero_queue_depth(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	cache = ExecutionCache()
	artifact_store = MemoryArtifactStore()

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "src":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": ["x", "y", "z"], "meta": {"ok": True}},
			)
		work_item = ((node.get("data", {}).get("params", {}) or {}).get("_work_item") or {})
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"node": node_id, "item": work_item.get("itemPreview")}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	graph = {
		"nodes": [
			{"id": "src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "branch_a",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
			{
				"id": "branch_b",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
		],
		"edges": [
			{"id": "e_a", "source": "src", "target": "branch_a", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items"}}},
			{"id": "e_b", "source": "src", "target": "branch_b", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items"}}},
		],
	}

	events_seed: list[dict] = []
	await run_mod.run_graph(
		run_id="run-e2e-partial-seed",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-e2e-partial-seed", on_emit=lambda evt: events_seed.append(dict(evt))),
		artifact_store=artifact_store,
		cache=cache,
		graph_id="graph-e2e-partial",
	)
	seed_metrics = _last_queue_metrics(events_seed)
	assert int(seed_metrics.get("globalDepth") or 0) == 0

	events_partial: list[dict] = []
	await run_mod.run_graph(
		run_id="run-e2e-partial-selected",
		graph=graph,
		run_from="branch_a",
		run_mode="from_selected_onward",
		bus=RunEventBus("run-e2e-partial-selected", on_emit=lambda evt: events_partial.append(dict(evt))),
		artifact_store=artifact_store,
		cache=cache,
		graph_id="graph-e2e-partial",
	)

	finished = [evt for evt in events_partial if str(evt.get("type") or "") == "run_finished"]
	assert finished and str(finished[-1].get("status") or "") == "succeeded"
	partial_metrics = _last_queue_metrics(events_partial)
	assert int(partial_metrics.get("globalDepth") or 0) == 0
	assert any(
		str(evt.get("type") or "") == "cache_decision"
		and str(evt.get("decision") or "") == "cache_hit"
		for evt in events_partial
	)
