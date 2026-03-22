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
async def test_item_streaming_load_many_items_completes(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	seen = 0

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		nonlocal seen
		node_id = str(node["id"])
		if node_id == "a":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": list(range(100)), "meta": {"ok": True}},
			)
		seen += 1
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
			{"id": "e1", "source": "a", "target": "b", "data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 100}}},
		],
	}
	events: list[dict] = []
	bus = RunEventBus("run-qflow-023-chaos", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-qflow-023-chaos",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-qflow-023-chaos",
	)
	assert seen >= 100
	assert any(evt.get("type") == "run_finished" and evt.get("status") == "succeeded" for evt in events)


@pytest.mark.asyncio
async def test_item_streaming_with_param_edge_does_not_backpressure_work_queue(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	seen = 0

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		nonlocal seen
		node_id = str(node["id"])
		if node_id == "a":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": list(range(25)), "meta": {"ok": True}},
			)
		if node_id == "p":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {"region": "us"}, "meta": {"ok": True}},
			)
		seen += 1
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
			{"id": "e_work", "source": "a", "target": "b", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 25}}},
			{"id": "e_param", "source": "p", "target": "b", "targetHandle": "param_config", "data": {"mode": "param"}},
		],
	}
	events: list[dict] = []
	bus = RunEventBus("run-qflow-023-chaos-mixed", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-qflow-023-chaos-mixed",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-qflow-023-chaos-mixed",
	)
	assert seen >= 25
	assert any(evt.get("type") == "run_finished" and evt.get("status") == "succeeded" for evt in events)


@pytest.mark.asyncio
async def test_item_streaming_load_caps_items_via_work_max_items(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	seen = 0

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		nonlocal seen
		node_id = str(node["id"])
		if node_id == "a":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": list(range(50)), "meta": {"ok": True}},
			)
		seen += 1
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
			{"id": "e1", "source": "a", "target": "b", "data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 7}}},
		],
	}
	bus = RunEventBus("run-qflow-023-chaos-cap")
	await run_mod.run_graph(
		run_id="run-qflow-023-chaos-cap",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-qflow-023-chaos-cap",
	)
	assert seen == 7
