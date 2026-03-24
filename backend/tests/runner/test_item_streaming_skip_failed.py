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
async def test_item_streaming_skip_failed_continues_other_items(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node["id"])
		params = ((node.get("data") or {}).get("params") or {}) if isinstance(node, dict) else {}
		if node_id == "a":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [1, 2, 3], "meta": {"ok": True}},
			)
		if node_id == "b":
			work_item = params.get("_work_item") if isinstance(params, dict) else {}
			idx = int((work_item or {}).get("itemIndex") or 0)
			if idx == 1:
				return NodeOutput(
					status="failed",
					metadata=None,
					execution_time_ms=1.0,
					error="simulated timeout",
					data={"kind": "json", "payload": {"ok": False, "idx": idx}, "meta": {"ok": False}},
				)
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {"ok": True, "idx": idx}, "meta": {"ok": True}},
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
			{
				"id": "b",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1, "on_error": "skip_failed"},
				},
			},
			{"id": "c", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [
			{"id": "e1", "source": "a", "target": "b", "data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 3}}},
			{"id": "e2", "source": "b", "target": "c", "data": {"mode": "work"}},
		],
	}
	events: list[dict] = []
	bus = RunEventBus("run-qflow-skip-failed", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-qflow-skip-failed",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-qflow-skip-failed",
	)

	assert any(evt.get("type") == "run_finished" and evt.get("status") == "succeeded" for evt in events)
	b_started = sum(1 for evt in events if evt.get("type") == "node_started" and evt.get("nodeId") == "b")
	assert b_started == 3
	assert any(evt.get("type") == "node_started" and evt.get("nodeId") == "c" for evt in events)
	assert any(
		evt.get("type") == "log" and "soft-fail skip node=b" in str(evt.get("message") or "")
		for evt in events
	)
