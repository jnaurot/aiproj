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
async def test_single_item_node_never_reports_no_ready_work_while_edge_has_depth(monkeypatch) -> None:
	"""Proves/disproves stalled-readiness hypothesis:
	If downstream queue still has items, single_item node must not report NO_READY_WORK.
	"""
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	consumed: list[int] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "a":
			# Upstream emits six work items in one artifact.
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [1, 2, 3, 4, 5, 6], "meta": {"ok": True}},
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
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}},
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
				"targetHandle": "in",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 16}},
			}
		],
	}

	events: list[dict] = []
	bus = RunEventBus("run-qflow-no-ready-guard", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-qflow-no-ready-guard",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-qflow-no-ready-guard",
	)

	assert sorted(consumed) == [1, 2, 3, 4, 5, 6]

	last_depth_by_edge_handle: dict[str, int] = {}
	violations: list[dict] = []
	for evt in events:
		if str(evt.get("type") or "") == "queue_metrics":
			metrics = evt.get("metrics") if isinstance(evt.get("metrics"), dict) else {}
			edges = metrics.get("edges") if isinstance(metrics.get("edges"), dict) else {}
			for key, value in edges.items():
				if isinstance(value, dict):
					last_depth_by_edge_handle[str(key)] = int(value.get("depth") or 0)
		if str(evt.get("type") or "") == "node_blocked":
			if str(evt.get("nodeId") or "") != "b":
				continue
			if str(evt.get("reasonCode") or "") != "NO_READY_WORK":
				continue
			edge_id = str(evt.get("edgeId") or "").strip()
			handle = str(evt.get("handle") or "in").strip() or "in"
			depth_key = f"{edge_id}:{handle}" if edge_id else "e1:in"
			depth_at_block = int(last_depth_by_edge_handle.get(depth_key, 0))
			if depth_at_block > 0:
				violations.append(
					{
						"edge": depth_key,
						"depth": depth_at_block,
						"event": evt,
					}
				)

	assert not violations, f"NO_READY_WORK emitted while queue still had items: {violations}"

	wait_check_logs = [
		str(evt.get("message") or "")
		for evt in events
		if str(evt.get("type") or "") == "log" and "[wait-check]" in str(evt.get("message") or "")
	]
	assert wait_check_logs, "expected wait-check diagnostic log when node transitions to waiting"
	assert any("queued_any=" in line and "inflight=" in line and "all_upstream_closed=" in line for line in wait_check_logs)
