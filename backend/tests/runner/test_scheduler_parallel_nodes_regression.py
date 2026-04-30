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
async def test_scheduler_runs_more_than_one_node_concurrently(monkeypatch) -> None:
	"""Regression guard: independent ready nodes must overlap when concurrency allows it."""
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	events: list[dict] = []
	state = {"inflight": 0, "max_inflight": 0}
	lock = asyncio.Lock()

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		async with lock:
			state["inflight"] += 1
			state["max_inflight"] = max(state["max_inflight"], state["inflight"])
		try:
			# Keep execution active long enough to observe overlap.
			await asyncio.sleep(0.12)
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {"node": node["id"]}},
			)
		finally:
			async with lock:
				state["inflight"] -= 1

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"seed": True}},
		)

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	monkeypatch.setenv("RUNNER_MAX_CONCURRENCY", "2")
	monkeypatch.setenv("RUNNER_MAX_TOOL", "2")
	monkeypatch.setenv("RUNNER_MAX_MODEL", "1")
	monkeypatch.setenv("RUNNER_MAX_SOURCE", "1")
	monkeypatch.setenv("RUNNER_MAX_TRANSFORM", "1")

	graph = {
		"nodes": [
			{
				"id": "src",
				"data": {
					"kind": "source",
					"sourceKind": "file",
					"label": "Source",
					"params": {"file_path": "dummy.txt", "file_format": "txt"},
					"schema": {"expectedSchema": {"typedSchema": {"type": "json", "fields": []}}},
				},
			},
			{
				"id": "tool_a",
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}},
			},
			{
				"id": "tool_b",
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}},
			},
		],
		"edges": [
			{"id": "e_src_a", "source": "src", "target": "tool_a"},
			{"id": "e_src_b", "source": "src", "target": "tool_b"},
		],
	}

	bus = RunEventBus("run-parallel-regression-001", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-parallel-regression-001",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-parallel-regression-001",
	)

	assert state["max_inflight"] >= 2, f"expected overlapping node execution, max_inflight={state['max_inflight']}"

	start_indices = [
		i
		for i, evt in enumerate(events)
		if evt.get("type") == "node_started" and str(evt.get("nodeId") or "") in {"tool_a", "tool_b"}
	]
	finish_indices = [
		i
		for i, evt in enumerate(events)
		if evt.get("type") == "node_finished" and str(evt.get("nodeId") or "") in {"tool_a", "tool_b"}
	]
	assert len(start_indices) == 2, "expected both independent nodes to start"
	assert finish_indices, "expected node_finished events"
	assert max(start_indices) < min(finish_indices), "both nodes should start before either one finishes"
