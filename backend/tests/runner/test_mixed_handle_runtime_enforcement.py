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
async def test_runtime_enforces_declared_input_contract_per_handle(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	seen_work_items: list[int] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node["id"])
		if node_id == "work_src":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [1, 2], "meta": {"ok": True}},
			)
		if node_id == "param_src":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "text", "payload": "region=us", "meta": {"ok": True}},
			)
		work_item = ((node.get("data", {}).get("params", {}) or {}).get("_work_item") or {})
		preview = work_item.get("itemPreview")
		if isinstance(preview, int):
			seen_work_items.append(preview)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "work_src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "param_src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "sink",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"schema": {
						"expectedInputSchemas": {
							"in": {
								"typedSchema": {"type": "json", "fields": []},
								"source": "declared",
								"state": "fresh",
							},
							"param_config": {
								"typedSchema": {"type": "text", "fields": []},
								"source": "declared",
								"state": "fresh",
							},
						},
					},
				},
			},
		],
		"edges": [
			{
				"id": "e_work",
				"source": "work_src",
				"target": "sink",
				"targetHandle": "in",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 10}},
			},
			{
				"id": "e_param",
				"source": "param_src",
				"target": "sink",
				"targetHandle": "param_config",
				"data": {"mode": "param"},
			},
		],
	}
	events: list[dict] = []
	bus = RunEventBus("run-schema-port-008-mixed", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-schema-port-008-mixed",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-schema-port-008-mixed",
	)

	assert sorted(seen_work_items) == [1, 2]
	assert any(evt.get("type") == "run_finished" and evt.get("status") == "succeeded" for evt in events)
	assert not any("CONTRACT_EDGE_PAYLOAD_TYPE_MISMATCH" in str(evt) for evt in events)
