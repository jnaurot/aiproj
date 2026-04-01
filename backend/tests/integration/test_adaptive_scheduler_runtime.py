import asyncio
import importlib
import os
import sys
import types

import pytest

if "duckdb" not in sys.modules:
	sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


def _fanout_tool_graph() -> dict:
	return {
		"nodes": [
			{
				"id": "tool_src",
				"data": {
					"kind": "tool",
					"label": "Tool Source",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
				},
			},
			{
				"id": "tool_a",
				"data": {"kind": "tool", "label": "Tool A", "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}}},
			},
			{
				"id": "tool_b",
				"data": {"kind": "tool", "label": "Tool B", "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}}},
			},
		],
		"edges": [
			{"id": "e1", "source": "tool_src", "target": "tool_a"},
			{"id": "e2", "source": "tool_src", "target": "tool_b"},
		],
	}


@pytest.mark.asyncio
async def test_adaptive_observe_emits_decisions_without_enforcement(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	events = []
	lock = asyncio.Lock()
	state = {"tool": 0, "max_tool": 0}

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"seed": "src"}, "meta": {"status": "ok"}},
		)

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		async with lock:
			state["tool"] += 1
			state["max_tool"] = max(state["max_tool"], state["tool"])
		try:
			await asyncio.sleep(0.1)
			return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data={"kind": "json", "payload": {"ok": True}})
		finally:
			async with lock:
				state["tool"] -= 1

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	monkeypatch.setattr(run_mod, "get_env", lambda name, default=None: os.getenv(name, default))
	monkeypatch.setattr(
		run_mod,
		"apply_adaptive_policy",
		lambda **kwargs: {
			"nextCaps": {"global": 2, "source": 2, "transform": 2, "model": 1, "tool": 1},
			"changedCaps": {"global": {"from": 3, "to": 2}, "tool": {"from": 2, "to": 1}},
			"changed": True,
			"reasons": ["pressure"],
			"inputs": {"queueDepth": 1, "readyCount": 1, "avgLatencyMs": 5.0, "failureRate": 0.0, "leaseWaitMs": 0.0},
		},
	)
	monkeypatch.setenv("RUNNER_MAX_CONCURRENCY", "3")
	monkeypatch.setenv("RUNNER_MAX_TOOL", "2")
	monkeypatch.setenv("RUNNER_ADAPTIVE_MODE", "observe")
	monkeypatch.setenv("RUNNER_ADAPTIVE_FAILURE_HIGH", "0")
	monkeypatch.setenv("RUNNER_ADAPTIVE_EVAL_INTERVAL_MS", "100")
	monkeypatch.setenv("RUNNER_ADAPTIVE_COOLDOWN_MS", "0")

	artifact_root = tmp_path / "adaptive-observe"
	await run_mod.run_graph(
		run_id="run-adaptive-observe",
		graph=_fanout_tool_graph(),
		run_from=None,
		bus=RunEventBus("run-adaptive-observe", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-adaptive-observe",
	)

	adaptive_events = [evt for evt in events if evt.get("type") == "scheduler_adaptive_decision"]
	assert adaptive_events, "expected adaptive decision events in observe mode"
	assert any(str(evt.get("mode") or "") == "observe" for evt in adaptive_events)
	assert all(str(evt.get("modeSource") or "") == "env" for evt in adaptive_events)
	assert any(int(((evt.get("effectiveCaps") or {}).get("tool") or 0)) == 2 for evt in adaptive_events)
	assert state["max_tool"] >= 2, "observe mode should not enforce reduced tool cap"


@pytest.mark.asyncio
async def test_adaptive_enforce_reduces_tool_concurrency(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	events = []
	lock = asyncio.Lock()
	state = {"tool": 0, "max_tool": 0}

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"seed": "src"}, "meta": {"status": "ok"}},
		)

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		async with lock:
			state["tool"] += 1
			state["max_tool"] = max(state["max_tool"], state["tool"])
		try:
			await asyncio.sleep(0.1)
			return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data={"kind": "json", "payload": {"ok": True}})
		finally:
			async with lock:
				state["tool"] -= 1

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	monkeypatch.setattr(run_mod, "get_env", lambda name, default=None: os.getenv(name, default))
	monkeypatch.setattr(
		run_mod,
		"apply_adaptive_policy",
		lambda **kwargs: {
			"nextCaps": {"global": 2, "source": 2, "transform": 2, "model": 1, "tool": 1},
			"changedCaps": {"global": {"from": 3, "to": 2}, "tool": {"from": 2, "to": 1}},
			"changed": True,
			"reasons": ["pressure"],
			"inputs": {"queueDepth": 1, "readyCount": 1, "avgLatencyMs": 5.0, "failureRate": 0.0, "leaseWaitMs": 0.0},
		},
	)
	monkeypatch.setenv("RUNNER_MAX_CONCURRENCY", "3")
	monkeypatch.setenv("RUNNER_MAX_TOOL", "2")
	monkeypatch.setenv("RUNNER_ADAPTIVE_MODE", "enforce")
	monkeypatch.setenv("RUNNER_ADAPTIVE_FAILURE_HIGH", "0")
	monkeypatch.setenv("RUNNER_ADAPTIVE_EVAL_INTERVAL_MS", "100")
	monkeypatch.setenv("RUNNER_ADAPTIVE_COOLDOWN_MS", "0")
	monkeypatch.setenv("RUNNER_ADAPTIVE_MIN_TOOL", "1")

	artifact_root = tmp_path / "adaptive-enforce"
	await run_mod.run_graph(
		run_id="run-adaptive-enforce",
		graph=_fanout_tool_graph(),
		run_from=None,
		bus=RunEventBus("run-adaptive-enforce", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-adaptive-enforce",
	)

	adaptive_events = [evt for evt in events if evt.get("type") == "scheduler_adaptive_decision"]
	assert adaptive_events, "expected adaptive decision events in enforce mode"
	assert any(str(evt.get("mode") or "") == "enforce" for evt in adaptive_events)
	assert all(str(evt.get("modeSource") or "") == "env" for evt in adaptive_events)
	assert any(int(((evt.get("effectiveCaps") or {}).get("tool") or 0)) <= 1 for evt in adaptive_events)
	assert state["max_tool"] <= 1, "enforce mode should throttle tool concurrency"


@pytest.mark.asyncio
async def test_adaptive_off_emits_no_decision_events(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	events = []

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"seed": "src"}, "meta": {"status": "ok"}},
		)

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data={"kind": "json", "payload": {"ok": True}})

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	monkeypatch.setattr(run_mod, "get_env", lambda name, default=None: os.getenv(name, default))
	monkeypatch.setenv("RUNNER_ADAPTIVE_MODE", "off")
	monkeypatch.setenv("RUNNER_ADAPTIVE_FAILURE_HIGH", "0")

	artifact_root = tmp_path / "adaptive-off"
	await run_mod.run_graph(
		run_id="run-adaptive-off",
		graph=_fanout_tool_graph(),
		run_from=None,
		bus=RunEventBus("run-adaptive-off", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-adaptive-off",
	)

	assert not [evt for evt in events if evt.get("type") == "scheduler_adaptive_decision"]


@pytest.mark.asyncio
async def test_adaptive_override_mode_source_is_run_override(monkeypatch, tmp_path):
	run_mod = importlib.import_module("app.runner.run")
	events = []

	async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"seed": "src"}, "meta": {"status": "ok"}},
		)

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data={"kind": "json", "payload": {"ok": True}})

	monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	monkeypatch.setattr(run_mod, "get_env", lambda name, default=None: os.getenv(name, default))
	monkeypatch.setattr(
		run_mod,
		"apply_adaptive_policy",
		lambda **kwargs: {
			"nextCaps": {"global": 2, "source": 2, "transform": 2, "model": 1, "tool": 1},
			"changedCaps": {"global": {"from": 3, "to": 2}},
			"changed": True,
			"reasons": ["pressure"],
			"inputs": {"queueDepth": 2, "readyCount": 1, "avgLatencyMs": 5.0, "failureRate": 0.0, "leaseWaitMs": 0.0},
		},
	)
	monkeypatch.setenv("RUNNER_ADAPTIVE_MODE", "off")
	monkeypatch.setenv("RUNNER_MAX_CONCURRENCY", "3")

	artifact_root = tmp_path / "adaptive-override-source"
	await run_mod.run_graph(
		run_id="run-adaptive-override-source",
		graph=_fanout_tool_graph(),
		run_from=None,
		bus=RunEventBus("run-adaptive-override-source", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=DiskArtifactStore(artifact_root),
		cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
		graph_id="graph-adaptive-override-source",
		adaptive_override={"mode": "observe"},
	)

	adaptive_events = [evt for evt in events if evt.get("type") == "scheduler_adaptive_decision"]
	assert adaptive_events, "expected adaptive decision events with run override"
	assert any(str(evt.get("mode") or "") == "observe" for evt in adaptive_events)
	assert all(str(evt.get("modeSource") or "") == "run_override" for evt in adaptive_events)
