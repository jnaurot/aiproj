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
async def test_control_signals_and_queue_metrics_are_emitted(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "a", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [{"id": "e1", "source": "a", "target": "b"}],
	}
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-signals",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-signals", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-signals",
	)
	signals = [str(e.get("signal") or "") for e in events if e.get("type") == "control_signal"]
	assert "ready" in signals
	assert "busy" in signals
	assert "drain" in signals
	assert "node_active" in signals
	assert "node_quiescent" in signals
	assert "node_terminal" in signals
	control_events = [e for e in events if e.get("type") == "control_signal"]
	seqs = [int(e.get("seq") or 0) for e in control_events]
	assert all(seq > 0 for seq in seqs)
	assert seqs == sorted(seqs)
	assert all(int(e.get("event_version") or 0) == 1 for e in control_events)
	assert all(str(e.get("payload_type") or "") == "control_signal.v1" for e in control_events)
	queue_events = [e for e in events if e.get("type") == "queue_metrics"]
	assert queue_events
	assert isinstance(queue_events[-1].get("controlPlaneEdgeState"), dict)
	assert int(queue_events[-1].get("lastControlSeq") or 0) > 0


@pytest.mark.asyncio
async def test_control_edge_runtime_ignores_payload_type_mismatch(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "a", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [
			{
				"id": "e_control",
				"source": "a",
				"sourceHandle": "control_out",
				"target": "b",
				"targetHandle": "control_in",
				"data": {
					"mode": "control",
					"contract": {"payload": {"source": {"type": "text"}, "target": {"type": "json"}}},
				},
			}
		],
	}
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-control-mismatch",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-control-mismatch", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-control-mismatch",
	)
	assert any(evt.get("type") == "run_finished" and evt.get("status") == "succeeded" for evt in events)
	assert not any(
		evt.get("type") == "log" and "CONTRACT_EDGE_PAYLOAD_TYPE_MISMATCH" in str(evt.get("message") or "")
		for evt in events
	)


@pytest.mark.asyncio
async def test_node_terminal_control_signal_emits_once_per_node(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "a", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [{"id": "e1", "source": "a", "target": "b"}],
	}
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-terminal-once",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-terminal-once", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-terminal-once",
	)
	terminal = [
		e
		for e in events
		if e.get("type") == "control_signal" and str(e.get("signal") or "") == "node_terminal"
	]
	seen: dict[str, int] = {}
	for evt in terminal:
		node_id = str(evt.get("nodeId") or "")
		if not node_id:
			continue
		seen[node_id] = int(seen.get(node_id, 0)) + 1
	assert seen
	assert all(count == 1 for count in seen.values())


@pytest.mark.asyncio
async def test_node_terminal_emits_after_edge_drain_and_close(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "a", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [{"id": "e1", "source": "a", "target": "b"}],
	}
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-terminal-order",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-terminal-order", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-terminal-order",
	)
	control = [e for e in events if e.get("type") == "control_signal"]
	node_terminal_index = next(
		(i for i, evt in enumerate(control) if str(evt.get("signal") or "") == "node_terminal" and str(evt.get("nodeId") or "") == "b"),
		-1,
	)
	input_drained_index = next(
		(i for i, evt in enumerate(control) if str(evt.get("signal") or "") == "input_drained" and str(evt.get("edgeId") or "") == "e1"),
		-1,
	)
	upstream_closed_index = next(
		(i for i, evt in enumerate(control) if str(evt.get("signal") or "") == "upstream_closed" and str(evt.get("edgeId") or "") == "e1"),
		-1,
	)
	assert node_terminal_index >= 0
	assert input_drained_index >= 0
	assert upstream_closed_index >= 0
	assert node_terminal_index > input_drained_index
	assert node_terminal_index > upstream_closed_index
