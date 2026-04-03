from __future__ import annotations

import asyncio
import importlib
import os
import sys
import types
from time import monotonic
from typing import Any

import pytest

from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


def _ensure_duckdb_stub() -> None:
	if "duckdb" not in sys.modules:
		sys.modules["duckdb"] = types.SimpleNamespace()


def _event_index(events: list[dict[str, Any]], event_type: str, node_id: str | None = None) -> int:
	for idx, evt in enumerate(events):
		if str(evt.get("type") or "") != event_type:
			continue
		if node_id is not None and str(evt.get("nodeId") or "") != node_id:
			continue
		return idx
	return -1


@pytest.mark.asyncio
async def test_e2e_cancel_during_inflight_node_terminal_consistency(monkeypatch):
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	started = asyncio.Event()

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		started.set()
		await asyncio.Event().wait()
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{
				"id": "n_tool",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
		],
		"edges": [],
	}
	events: list[dict[str, Any]] = []
	bus = RunEventBus("run-e2e-cancel-model", on_emit=lambda evt: events.append(dict(evt)))
	cancel_event = asyncio.Event()
	task = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-e2e-cancel-model",
			graph=graph,
			run_from=None,
			bus=bus,
			artifact_store=MemoryArtifactStore(),
			cache=ExecutionCache(),
			cancel_event=cancel_event,
			graph_id="graph-e2e-cancel-model",
		)
	)
	await asyncio.wait_for(started.wait(), timeout=2.0)
	cancel_event.set()
	await asyncio.wait_for(task, timeout=5.0)
	assert any(str(evt.get("type") or "") == "run_canceled" for evt in events)
	assert any(
		str(evt.get("type") or "") == "run_finished" and str(evt.get("status") or "") == "canceled"
		for evt in events
	)
	assert not any(
		str(evt.get("type") or "") == "log"
		and "terminality_incomplete" in str(evt.get("message") or "")
		for evt in events
	)


@pytest.mark.asyncio
async def test_e2e_multi_input_required_handle_waits_for_late_upstream(monkeypatch):
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	release_b = asyncio.Event()

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "b":
			await release_b.wait()
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"node": node_id}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "a", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "c",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"processingPolicy": {"consume_mode": "batch", "batch_size": 2},
				},
			},
		],
		"edges": [
			{"id": "e_ac", "source": "a", "target": "c", "targetHandle": "in", "data": {"mode": "work"}},
			{"id": "e_bc", "source": "b", "target": "c", "targetHandle": "in", "data": {"mode": "work"}},
		],
	}
	events: list[dict[str, Any]] = []
	bus = RunEventBus("run-e2e-multi-input-late", on_emit=lambda evt: events.append(dict(evt)))
	task = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-e2e-multi-input-late",
			graph=graph,
			run_from=None,
			bus=bus,
			artifact_store=MemoryArtifactStore(),
			cache=ExecutionCache(),
			graph_id="graph-e2e-multi-input-late",
		)
	)
	await asyncio.sleep(0.1)
	release_b.set()
	await asyncio.wait_for(task, timeout=5.0)
	idx_finished_a = _event_index(events, "node_finished", "a")
	idx_finished_b = _event_index(events, "node_finished", "b")
	idx_started_c = _event_index(events, "node_started", "c")
	assert idx_finished_a >= 0 and idx_finished_b >= 0 and idx_started_c >= 0
	assert idx_started_c > idx_finished_a
	assert idx_started_c > idx_finished_b
	assert any(
		str(evt.get("type") or "") == "run_finished" and str(evt.get("status") or "") == "succeeded"
		for evt in events
	)


@pytest.mark.asyncio
@pytest.mark.skipif(
	str(os.getenv("RUN_SLOW_CONTROL_PLANE_TESTS", "0")).strip() != "1",
	reason="Set RUN_SLOW_CONTROL_PLANE_TESTS=1 to run synthetic control-plane perf regression test.",
)
async def test_perf_control_plane_1k_nodes_5k_edges_synthetic(monkeypatch):
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=0.1,
			data={"kind": "json", "payload": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	nodes = [
		{
			"id": f"n_{i}",
			"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}},
		}
		for i in range(1000)
	]
	edges: list[dict[str, Any]] = []
	for i in range(1, 1000):
		for back in range(1, min(6, i + 1)):
			edges.append(
				{
					"id": f"e_{i}_{back}",
					"source": f"n_{i-back}",
					"target": f"n_{i}",
					"targetHandle": "in",
					"data": {"mode": "work"},
				}
			)
	graph = {"nodes": nodes, "edges": edges}
	events: list[dict[str, Any]] = []
	t0 = monotonic()
	await run_mod.run_graph(
		run_id="run-perf-control-plane-1k-5k",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-perf-control-plane-1k-5k", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-perf-control-plane-1k-5k",
	)
	runtime_s = monotonic() - t0
	assert any(
		str(evt.get("type") or "") == "run_finished" and str(evt.get("status") or "") == "succeeded"
		for evt in events
	)
	assert not any(
		str(evt.get("type") or "") == "log" and "terminality_incomplete" in str(evt.get("message") or "")
		for evt in events
	)
	assert runtime_s < 60.0
