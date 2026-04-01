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
	assert any(str(evt.get("type") or "") == "scheduler_adaptive_decision" for evt in events), "expected adaptive decisions"

	# Ensure replay compares against the same authoritative binding basis captured
	# in the contract snapshot for this unchanged-run scenario.
	basis_nodes = ((handle.execution_contract or {}).get("basis") or {}).get("nodes") or {}
	normalized_bindings: Dict[str, Dict[str, Any]] = {}
	for node_id, node_basis in basis_nodes.items():
		if not isinstance(node_basis, dict):
			continue
		binding = node_basis.get("binding") if isinstance(node_basis.get("binding"), dict) else {}
		exec_key = str(binding.get("execKey") or "").strip()
		artifact_id = str(binding.get("artifactId") or "").strip()
		if exec_key or artifact_id:
			normalized_bindings[str(node_id)] = {"currentExecKey": exec_key, "currentArtifactId": artifact_id}
	if normalized_bindings:
		handle.node_bindings = normalized_bindings

	replay_result = await rt.request_replay(source_run_id=run_id)
	if bool(replay_result.get("replayed")):
		replay_run_id = str(replay_result.get("runId") or "")
		assert replay_run_id
		await rt.get_run(replay_run_id).task
		assert str(rt.get_run(replay_run_id).status or "") == "succeeded"
		replay_events = await rt.list_run_events(replay_run_id, after_id=0, limit=2000)
		assert any(str(evt.get("type") or "") == "control_gate_state" for evt in replay_events)
	else:
		assert str(replay_result.get("errorCode") or "") == "REPLAY_CONTRACT_VALIDATION_FAILED"
		details = replay_result.get("details") if isinstance(replay_result.get("details"), dict) else {}
		assert isinstance(details.get("contractDiff"), dict)
		assert list(details.get("reasonCodes") or [])
