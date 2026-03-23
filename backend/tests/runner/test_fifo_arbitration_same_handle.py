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


def _make_graph(*, queue_policy: str) -> dict:
	return {
		"nodes": [
			{"id": "producer_a", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "producer_b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "dst",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
				},
			},
		],
		"edges": [
			{
				"id": "e_a",
				"source": "producer_a",
				"target": "dst",
				"targetHandle": "in",
				"data": {
					"mode": "work",
					"queue": {"policy": queue_policy},
					"work": {"item_mode": "json_items", "max_items": 4},
				},
			},
			{
				"id": "e_b",
				"source": "producer_b",
				"target": "dst",
				"targetHandle": "in",
				"data": {
					"mode": "work",
					"queue": {"policy": queue_policy},
					"work": {"item_mode": "json_items", "max_items": 4},
				},
			},
		],
	}


@pytest.mark.asyncio
async def test_fifo_arbitration_same_handle_prefers_oldest_enqueued_item(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	seen_order: list[str] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "producer_a":
			await asyncio.sleep(0.02)
			payload = [{"producer": "a"}]
		elif node_id == "producer_b":
			await asyncio.sleep(0.0)
			payload = [{"producer": "b"}]
		elif node_id == "dst":
			work_batch = (((node.get("data", {}) or {}).get("params", {}) or {}).get("_work_batch") or [])
			for item in work_batch:
				producer = str(((item or {}).get("itemPreview") or {}).get("producer") or "")
				if producer:
					seen_order.append(producer)
			payload = {"ok": True}
		else:
			payload = {"ok": True}
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": payload, "meta": {}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	await run_mod.run_graph(
		run_id="run-fifo-arb",
		graph=_make_graph(queue_policy="fifo"),
		run_from=None,
		bus=RunEventBus("run-fifo-arb"),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-fifo-arb",
	)

	assert seen_order == ["b", "a"]


@pytest.mark.asyncio
async def test_round_robin_arbitration_same_handle_keeps_deterministic_edge_order(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	seen_order: list[str] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "producer_a":
			await asyncio.sleep(0.02)
			payload = [{"producer": "a"}]
		elif node_id == "producer_b":
			await asyncio.sleep(0.0)
			payload = [{"producer": "b"}]
		elif node_id == "dst":
			work_batch = (((node.get("data", {}) or {}).get("params", {}) or {}).get("_work_batch") or [])
			for item in work_batch:
				producer = str(((item or {}).get("itemPreview") or {}).get("producer") or "")
				if producer:
					seen_order.append(producer)
			payload = {"ok": True}
		else:
			payload = {"ok": True}
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": payload, "meta": {}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	await run_mod.run_graph(
		run_id="run-round-robin-arb",
		graph=_make_graph(queue_policy="round_robin"),
		run_from=None,
		bus=RunEventBus("run-round-robin-arb"),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-round-robin-arb",
	)

	assert seen_order == ["a", "b"]
