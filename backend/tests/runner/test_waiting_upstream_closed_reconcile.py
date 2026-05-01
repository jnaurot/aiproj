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
async def test_waiting_required_input_reconciles_missing_upstream_closed(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	orig_reduce = run_mod.reduce_edge_control_state
	dropped = {"count": 0}

	def _drop_first_upstream_closed(*args, **kwargs):
		signal = str(kwargs.get("signal") or "").strip().upper()
		if signal == "UPSTREAM_CLOSED" and dropped["count"] == 0:
			dropped["count"] = 1
			return args[0] if args else {}
		return orig_reduce(*args, **kwargs)

	monkeypatch.setattr(run_mod, "reduce_edge_control_state", _drop_first_upstream_closed)

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "producer":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [1], "meta": {"ok": True}},
			)
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
				"id": "producer",
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}},
			},
			{
				"id": "consumer",
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
				"source": "producer",
				"target": "consumer",
				"targetHandle": "in",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 8}},
			}
		],
	}

	events: list[dict] = []
	bus = RunEventBus("run-wait-reconcile", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-wait-reconcile",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-wait-reconcile",
	)

	wait_reconcile_logs = [
		str(evt.get("message") or "")
		for evt in events
		if str(evt.get("type") or "") == "log" and "[wait-reconcile]" in str(evt.get("message") or "")
	]
	assert wait_reconcile_logs, "expected wait-reconcile log when first upstream_closed signal is dropped"
	assert any("node=consumer" in line for line in wait_reconcile_logs)

	node_terminal_events = [
		evt
		for evt in events
		if str(evt.get("type") or "") == "control_signal"
		and str(evt.get("signal") or "") == "node_terminal"
		and str(evt.get("nodeId") or "") == "consumer"
	]
	assert node_terminal_events, "expected consumer to terminalize after closure reconciliation"
