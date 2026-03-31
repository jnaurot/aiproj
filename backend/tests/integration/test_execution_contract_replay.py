from __future__ import annotations

from typing import Any, Dict

import pytest

from app.runtime import RuntimeManager
from app.runner.run import _build_frontier_identity_basis


def _make_graph() -> Dict[str, Any]:
	return {
		"nodes": [
			{"id": "n1", "data": {"kind": "transform", "params": {"op": "select", "keep": ["title"]}}},
			{"id": "n2", "data": {"kind": "model", "params": {"model": "qwen3.5:4b", "temperature": 0}}},
		],
		"edges": [
			{"id": "e1", "source": "n1", "target": "n2", "targetHandle": "in", "data": {"mode": "work"}},
		],
	}


def _make_bindings(*, exec1: str = "exec-1", art1: str = "art-1", exec2: str = "exec-2", art2: str = "art-2") -> Dict[str, Dict[str, Any]]:
	return {
		"n1": {"currentExecKey": exec1, "currentArtifactId": art1},
		"n2": {"currentExecKey": exec2, "currentArtifactId": art2},
	}


def _make_contract(*, graph: Dict[str, Any], graph_id: str, bindings: Dict[str, Dict[str, Any]], execution_version: str = "v1") -> Dict[str, Any]:
	basis = _build_frontier_identity_basis(
		graph=graph,
		graph_id=graph_id,
		node_ids=["n2"],
		node_bindings=bindings,
		execution_version=execution_version,
	)
	if execution_version:
		basis["executionVersion"] = execution_version
		for node_basis in (basis.get("nodes") or {}).values():
			if isinstance(node_basis, dict):
				node_basis["executionVersion"] = execution_version
	return {
		"contractVersion": 1,
		"graphId": graph_id,
		"basis": basis,
	}


@pytest.mark.asyncio
async def test_replay_succeeds_with_unchanged_contract(monkeypatch) -> None:
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	monkeypatch.setenv("RUNNER_EXECUTION_VERSION", "v1")
	rt = RuntimeManager()
	source_run_id = "run-replay-source-ok"
	graph = _make_graph()
	bindings = _make_bindings()
	contract = _make_contract(graph=graph, graph_id="graph-replay-ok", bindings=bindings, execution_version="v1")

	handle = rt.create_run(source_run_id)
	handle.status = "succeeded"
	handle.graph_id = "graph-replay-ok"
	handle.graph = graph
	handle.node_bindings = dict(bindings)
	handle.execution_contract = dict(contract)

	captured: Dict[str, Any] = {}

	async def _fake_start_run(run_id, graph_payload, run_from, run_mode=None, graph_id=None, resume_snapshot=None):
		captured["run_id"] = run_id
		captured["graph"] = graph_payload
		captured["graph_id"] = graph_id
		captured["run_from"] = run_from
		captured["run_mode"] = run_mode

	monkeypatch.setattr(rt, "start_run", _fake_start_run)
	result = await rt.request_replay(source_run_id=source_run_id)
	assert result["replayed"] is True
	assert str(result.get("runId") or "").strip() != ""
	assert str(result.get("runId")) != source_run_id
	assert captured["graph_id"] == "graph-replay-ok"


@pytest.mark.asyncio
async def test_replay_fails_on_env_hash_change(monkeypatch) -> None:
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	monkeypatch.setenv("RUNNER_EXECUTION_VERSION", "v1")
	rt = RuntimeManager()
	source_run_id = "run-replay-env-change"
	graph = _make_graph()
	bindings = _make_bindings()
	contract = _make_contract(graph=graph, graph_id="graph-replay-env", bindings=bindings, execution_version="v1")

	handle = rt.create_run(source_run_id)
	handle.status = "succeeded"
	handle.graph_id = "graph-replay-env"
	handle.graph = graph
	handle.node_bindings = dict(bindings)
	handle.execution_contract = dict(contract)

	mutated_graph = _make_graph()
	mutated_graph["nodes"][1]["data"]["params"]["inputEncoding"] = "table_canonical"
	result = await rt.request_replay(source_run_id=source_run_id, graph=mutated_graph)
	assert result["replayed"] is False
	assert result["errorCode"] == "REPLAY_CONTRACT_VALIDATION_FAILED"
	assert "env_changed" in list((result.get("details") or {}).get("reasonCodes") or [])
	assert isinstance(((result.get("details") or {}).get("contractDiff")), dict)


@pytest.mark.asyncio
async def test_replay_fails_on_execution_version_change(monkeypatch) -> None:
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	monkeypatch.setenv("RUNNER_EXECUTION_VERSION", "v1")
	rt = RuntimeManager()
	source_run_id = "run-replay-version-change"
	graph = _make_graph()
	bindings = _make_bindings()
	contract = _make_contract(graph=graph, graph_id="graph-replay-version", bindings=bindings, execution_version="v999")

	handle = rt.create_run(source_run_id)
	handle.status = "succeeded"
	handle.graph_id = "graph-replay-version"
	handle.graph = graph
	handle.node_bindings = dict(bindings)
	handle.execution_contract = dict(contract)

	result = await rt.request_replay(source_run_id=source_run_id)
	assert result["replayed"] is False
	assert result["errorCode"] == "REPLAY_CONTRACT_VALIDATION_FAILED"
	assert "execution_version_changed" in list((result.get("details") or {}).get("reasonCodes") or [])
	assert isinstance(((result.get("details") or {}).get("contractDiff")), dict)


@pytest.mark.asyncio
async def test_replay_fails_on_upstream_binding_change(monkeypatch) -> None:
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	monkeypatch.setenv("RUNNER_EXECUTION_VERSION", "v1")
	rt = RuntimeManager()
	source_run_id = "run-replay-upstream-change"
	graph = _make_graph()
	original_bindings = _make_bindings(exec1="exec-old", art1="art-old", exec2="exec-2", art2="art-2")
	contract = _make_contract(
		graph=graph,
		graph_id="graph-replay-upstream",
		bindings=original_bindings,
		execution_version="v1",
	)

	handle = rt.create_run(source_run_id)
	handle.status = "succeeded"
	handle.graph_id = "graph-replay-upstream"
	handle.graph = graph
	handle.node_bindings = _make_bindings(exec1="exec-new", art1="art-new", exec2="exec-2", art2="art-2")
	handle.execution_contract = dict(contract)

	result = await rt.request_replay(source_run_id=source_run_id)
	assert result["replayed"] is False
	assert result["errorCode"] == "REPLAY_CONTRACT_VALIDATION_FAILED"
	assert "dependency_frontier_changed" in list((result.get("details") or {}).get("reasonCodes") or [])
	assert isinstance(((result.get("details") or {}).get("contractDiff")), dict)


@pytest.mark.asyncio
async def test_replay_fails_on_node_state_change(monkeypatch) -> None:
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	monkeypatch.setenv("RUNNER_EXECUTION_VERSION", "v1")
	rt = RuntimeManager()
	source_run_id = "run-replay-node-state-change"
	graph = _make_graph()
	bindings = _make_bindings()
	contract = _make_contract(
		graph=graph,
		graph_id="graph-replay-node-state",
		bindings=bindings,
		execution_version="v1",
	)

	handle = rt.create_run(source_run_id)
	handle.status = "succeeded"
	handle.graph_id = "graph-replay-node-state"
	handle.graph = graph
	handle.node_bindings = dict(bindings)
	handle.execution_contract = dict(contract)

	mutated_graph = _make_graph()
	mutated_graph["nodes"][1]["data"]["params"]["temperature"] = 0.8
	result = await rt.request_replay(source_run_id=source_run_id, graph=mutated_graph)
	assert result["replayed"] is False
	assert result["errorCode"] == "REPLAY_CONTRACT_VALIDATION_FAILED"
	assert "node_state_changed" in list((result.get("details") or {}).get("reasonCodes") or [])
	assert isinstance(((result.get("details") or {}).get("contractDiff")), dict)
