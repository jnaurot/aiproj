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


def _graph_with_control_gate() -> dict:
	return {
		"nodes": [
			{
				"id": "work_src",
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}},
			},
			{
				"id": "control_src",
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}},
			},
			{
				"id": "sink",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"processingPolicy": {"consumeMode": "single_item", "readOnce": False, "maxInflight": 1},
					"portDeclarations": {
						"in": {
							"in": {"plane": "work", "required": True, "cardinality": "many"},
							"control_gate": {"plane": "control", "required": True, "cardinality": "many"},
						}
					},
				},
			},
		],
		"edges": [
			{
				"id": "e_work",
				"source": "work_src",
				"sourceHandle": "out",
				"target": "sink",
				"targetHandle": "in",
				"data": {"mode": "work", "linkKind": "data_link"},
			},
			{
				"id": "e_control",
				"source": "control_src",
				"sourceHandle": "control_out",
				"target": "sink",
				"targetHandle": "control_gate",
				"data": {"mode": "control", "linkKind": "control_link"},
			},
		],
	}


@pytest.mark.asyncio
async def test_control_gate_blocks_when_control_link_has_no_payload(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "control_src":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {}, "meta": {"ok": True}},
			)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-control-gated-blocked",
		graph=_graph_with_control_gate(),
		run_from=None,
		bus=RunEventBus("run-control-gated-blocked", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-control-gated-blocked",
	)
	blocked_events = [
		evt
		for evt in events
		if evt.get("type") == "node_blocked"
		and str(evt.get("nodeId") or "") == "sink"
		and str(evt.get("reasonCode") or "") == "CONTROL_GATE_BLOCKED"
	]
	assert blocked_events, "expected CONTROL_GATE_BLOCKED for sink when control_link payload is empty"
	assert not any(
		evt.get("type") == "node_started" and str(evt.get("nodeId") or "") == "sink"
		for evt in events
	)


@pytest.mark.asyncio
async def test_control_gate_release_allows_sink_execution(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "control_src":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": {"allow": True}, "meta": {"ok": True}},
			)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-control-gated-release",
		graph=_graph_with_control_gate(),
		run_from=None,
		bus=RunEventBus("run-control-gated-release", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-control-gated-release",
	)
	assert any(
		evt.get("type") == "node_started" and str(evt.get("nodeId") or "") == "sink"
		for evt in events
	), "expected sink to start once control_link queue receives payload"
	assert any(evt.get("type") == "run_finished" and evt.get("status") == "succeeded" for evt in events)

