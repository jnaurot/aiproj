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
async def test_mixed_work_and_param_inputs_consume_only_work_queue(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	consumed: list[int] = []
	upstream_counts: list[int] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node["id"])
		if node_id == "work_src":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [10, 20, 30], "meta": {"ok": True}},
			)
		if node_id == "param_src":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {"location": "remote"}, "meta": {"ok": True}},
			)
		work_item = ((node.get("data", {}).get("params", {}) or {}).get("_work_item") or {})
		preview = work_item.get("itemPreview")
		if isinstance(preview, int):
			consumed.append(preview)
		upstream_counts.append(len(upstream_artifact_ids or []))
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"seen": preview}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "work_src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "param_src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
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
				"id": "e_work",
				"source": "work_src",
				"target": "consumer",
				"targetHandle": "in",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 10}},
			},
			{
				"id": "e_param",
				"source": "param_src",
				"target": "consumer",
				"targetHandle": "param_config",
				"data": {"mode": "param"},
			},
		],
	}
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-mixed-inputs",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-mixed-inputs", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-mixed-inputs",
	)

	assert sorted(consumed) == [10, 20, 30]
	assert upstream_counts and all(count >= 2 for count in upstream_counts)
	assert any(evt.get("type") == "run_finished" and evt.get("status") == "succeeded" for evt in events)
