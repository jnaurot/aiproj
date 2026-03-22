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


def _graph(runtime_mode: str | None = None) -> dict:
	b_data: dict = {
		"kind": "tool",
		"label": "b",
		"params": {"provider": "builtin", "builtin": {"toolId": "noop"}, "tag": "orig"},
	}
	if runtime_mode:
		b_data["runtimeParamMode"] = runtime_mode
	return {
		"nodes": [
			{"id": "a", "data": {"kind": "tool", "label": "a", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "b", "data": b_data},
		],
		"edges": [{"id": "e1", "source": "a", "target": "b"}],
	}


@pytest.mark.asyncio
async def test_read_once_param_snapshot(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	seen = {"b_tag": None}
	node_refs: dict = {}
	orig_node_map = run_mod.node_map

	def _node_map_capture(g):
		mapped = orig_node_map(g)
		node_refs["nodes"] = mapped
		return mapped

	monkeypatch.setattr(run_mod, "node_map", _node_map_capture)

	graph = _graph()

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		if node["id"] == "a":
			# Mutate downstream node params at runtime. read_once should ignore this.
			node_refs["nodes"]["b"]["data"]["params"]["tag"] = "mutated"
		if node["id"] == "b":
			seen["b_tag"] = ((node.get("data") or {}).get("params") or {}).get("tag")
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	await run_mod.run_graph(
		run_id="run-snapshot",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-snapshot"),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-snapshot",
	)
	assert seen["b_tag"] == "orig"


@pytest.mark.asyncio
async def test_dynamic_param_mode_reads_live_values(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	seen = {"b_tag": None}
	node_refs: dict = {}
	orig_node_map = run_mod.node_map

	def _node_map_capture(g):
		mapped = orig_node_map(g)
		node_refs["nodes"] = mapped
		return mapped

	monkeypatch.setattr(run_mod, "node_map", _node_map_capture)

	graph = _graph(runtime_mode="dynamic")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		if node["id"] == "a":
			node_refs["nodes"]["b"]["data"]["params"]["tag"] = "mutated"
		if node["id"] == "b":
			seen["b_tag"] = ((node.get("data") or {}).get("params") or {}).get("tag")
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	await run_mod.run_graph(
		run_id="run-dynamic",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-dynamic"),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-dynamic",
	)
	assert seen["b_tag"] == "mutated"


@pytest.mark.asyncio
async def test_param_handle_skips_work_payload_type_preflight(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	events: list[dict] = []
	graph = {
		"nodes": [
			{"id": "a", "data": {"kind": "tool", "label": "a", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "b", "data": {"kind": "tool", "label": "b", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [
			{
				"id": "e_param",
				"source": "a",
				"sourceHandle": "out",
				"target": "b",
				"targetHandle": "param_config",
				"data": {
					"mode": "param",
					"contract": {
						"payload": {
							"source": {"type": "text", "keys": ["foo"]},
							"target": {"type": "json", "requiredKeys": ["foo"]},
						}
					},
				},
			}
		],
	}

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	await run_mod.run_graph(
		run_id="run-param-handle",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-param-handle", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-param-handle",
	)
	assert any(evt.get("type") == "run_finished" and evt.get("status") == "succeeded" for evt in events)
	assert not any(
		evt.get("type") == "log" and "CONTRACT_EDGE_PAYLOAD_TYPE_MISMATCH" in str(evt.get("message") or "")
		for evt in events
	)
