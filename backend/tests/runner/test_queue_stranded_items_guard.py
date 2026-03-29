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
async def test_pending_queue_without_runnable_node_fails_with_explicit_code(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node["id"])
		if node_id == "a_hot":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [1, 2, 3], "meta": {"ok": True}},
			)
		if node_id == "a_cold":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [], "meta": {"ok": True}},
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
			{"id": "a_hot", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "a_cold", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
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
				"id": "e_hot",
				"source": "a_hot",
				"target": "b",
				"targetHandle": "in_hot",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 10}},
			},
			{
				"id": "e_cold",
				"source": "a_cold",
				"target": "b",
				"targetHandle": "in_cold",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 10}},
			},
		],
	}
	events: list[dict] = []
	bus = RunEventBus("run-qflow-stranded-001", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-qflow-stranded-001",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-qflow-stranded-001",
	)

	finished = [evt for evt in events if evt.get("type") == "run_finished"]
	assert finished, "expected terminal run_finished event"
	assert str(finished[-1].get("status") or "") == "failed"
	assert str(finished[-1].get("errorCode") or "") == "QUEUE_STRANDED_ITEMS"
	assert any(
		evt.get("type") == "log"
		and "scheduler_stall_no_runnable_with_pending_queue" in str(evt.get("message") or "")
		for evt in events
	)
