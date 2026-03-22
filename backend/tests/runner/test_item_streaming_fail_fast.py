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
async def test_item_streaming_respects_fatal_stop(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node["id"])
		if node_id == "a":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [1, 2, 3], "meta": {"ok": True}},
			)
		if node_id == "b":
			return NodeOutput(
				status="failed",
				metadata=None,
				execution_time_ms=1.0,
				error="boom",
				data={"kind": "json", "payload": {"ok": False}, "meta": {"ok": False}},
			)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"node": node_id}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "a", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}, "fatal": True}}},
			{"id": "c", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [
			{"id": "e1", "source": "a", "target": "b", "data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 3}}},
			{"id": "e2", "source": "b", "target": "c", "data": {"mode": "work"}},
		],
	}
	events: list[dict] = []
	bus = RunEventBus("run-qflow-023-fatal", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-qflow-023-fatal",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-qflow-023-fatal",
	)
	assert any(evt.get("type") == "run_finished" and evt.get("status") == "failed" for evt in events)


@pytest.mark.asyncio
async def test_item_streaming_fatal_stop_with_param_edges(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	param_calls = 0

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		nonlocal param_calls
		node_id = str(node["id"])
		if node_id == "work_src":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [1, 2, 3], "meta": {"ok": True}},
			)
		if node_id == "param_src":
			param_calls += 1
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {"region": "us"}, "meta": {"ok": True}},
			)
		if node_id == "b":
			return NodeOutput(
				status="failed",
				metadata=None,
				execution_time_ms=1.0,
				error="boom",
				data={"kind": "json", "payload": {"ok": False}, "meta": {"ok": False}},
			)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"node": node_id}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "work_src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "param_src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}, "fatal": True}}},
		],
		"edges": [
			{
				"id": "e_work",
				"source": "work_src",
				"target": "b",
				"targetHandle": "in",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 3}},
			},
			{
				"id": "e_param",
				"source": "param_src",
				"target": "b",
				"targetHandle": "param_config",
				"data": {"mode": "param"},
			},
		],
	}
	events: list[dict] = []
	bus = RunEventBus("run-qflow-023-fatal-mixed", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-qflow-023-fatal-mixed",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-qflow-023-fatal-mixed",
	)
	assert param_calls == 1
	assert any(evt.get("type") == "run_finished" and evt.get("status") == "failed" for evt in events)
