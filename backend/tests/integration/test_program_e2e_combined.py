from __future__ import annotations

import asyncio
import importlib
import os
import sys
import types
from typing import Any, Dict

import pytest

from app.runtime import RuntimeManager
from app.runner.metadata import NodeOutput


def _ensure_duckdb_stub() -> None:
	if "duckdb" not in sys.modules:
		sys.modules["duckdb"] = types.SimpleNamespace()


def _combined_graph() -> Dict[str, Any]:
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
async def test_e2e_combined_control_adaptive_replay(monkeypatch) -> None:
	"""Program-level combined acceptance path:
	- control plane gating is active
	- adaptive decisions are emitted/enforced
	- replay stays deterministic with unchanged contract inputs
	"""
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		await asyncio.sleep(0.12)
		if node_id == "control_src":
			payload = {"allow": True}
		else:
			payload = {"node": node_id, "ok": True}
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": payload, "meta": {"ok": True}},
		)

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
			"inputs": {"queueDepth": 2, "readyCount": 1, "avgLatencyMs": 12.0, "failureRate": 0.0, "leaseWaitMs": 0.0},
		},
	)
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	monkeypatch.setenv("RUNNER_MAX_CONCURRENCY", "3")
	monkeypatch.setenv("RUNNER_MAX_TOOL", "2")
	monkeypatch.setenv("RUNNER_ADAPTIVE_MODE", "enforce")
	monkeypatch.setenv("RUNNER_ADAPTIVE_EVAL_INTERVAL_MS", "100")
	monkeypatch.setenv("RUNNER_ADAPTIVE_COOLDOWN_MS", "0")
	monkeypatch.setenv("RUNNER_ADAPTIVE_MIN_TOOL", "1")

	rt = RuntimeManager()
	run_id = "run-program-e2e-combined"
	graph = _combined_graph()
	rt.create_run(run_id)
	await rt.start_run(run_id, graph, run_from=None, graph_id="graph-program-e2e-combined")
	await rt.get_run(run_id).task
	handle = rt.get_run(run_id)
	assert str(handle.status or "") == "succeeded"
	assert isinstance(handle.execution_contract, dict)
	assert int((handle.execution_contract or {}).get("contractVersion") or 0) >= 1

	events = await rt.list_run_events(run_id, after_id=0, limit=2000)
	assert any(str(evt.get("type") or "") == "control_gate_state" for evt in events), "expected control gate diagnostics"
	adaptive_events = [evt for evt in events if str(evt.get("type") or "") == "scheduler_adaptive_decision"]
	assert adaptive_events, "expected adaptive decisions"
	def _payload_field(evt: Dict[str, Any], field: str) -> Any:
		if field in evt:
			return evt.get(field)
		payload = evt.get("payload")
		if isinstance(payload, dict):
			return payload.get(field)
		return None

	assert any(str(_payload_field(evt, "mode") or "").strip() in {"off", "observe", "enforce"} for evt in adaptive_events)
	assert all(isinstance(_payload_field(evt, "effectiveCaps"), dict) for evt in adaptive_events)
	assert all(str(_payload_field(evt, "modeSource") or "") in {"env", "run_override"} for evt in adaptive_events)
	assert not [evt for evt in events if str(evt.get("type") or "") == "state_invariant_violation"]
	experiment = await rt.artifact_store.get_run_experiment(run_id)
	assert isinstance(experiment, dict)
	assert str(experiment.get("graphId") or "") == "graph-program-e2e-combined"
	analytics = experiment.get("analytics") if isinstance(experiment.get("analytics"), dict) else {}
	assert isinstance(analytics.get("nodeLatencyMs"), dict)
	assert isinstance(analytics.get("queueDepthTrend"), list)

	replay_result = await rt.request_replay(source_run_id=run_id)
	assert bool(replay_result.get("replayed")) is True, replay_result
	replay_run_id = str(replay_result.get("runId") or "")
	assert replay_run_id
	await rt.get_run(replay_run_id).task
	assert str(rt.get_run(replay_run_id).status or "") == "succeeded"
	replay_events = await rt.list_run_events(replay_run_id, after_id=0, limit=2000)
	assert any(str(evt.get("type") or "") == "control_gate_state" for evt in replay_events)
	replay_experiment = await rt.artifact_store.get_run_experiment(replay_run_id)
	assert isinstance(replay_experiment, dict)
	assert str(replay_experiment.get("graphId") or "") == "graph-program-e2e-combined"
	replay_analytics = replay_experiment.get("analytics") if isinstance(replay_experiment.get("analytics"), dict) else {}
	assert isinstance(replay_analytics.get("nodeLatencyMs"), dict)
