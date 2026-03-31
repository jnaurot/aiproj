from __future__ import annotations

from typing import Any, Dict

import pytest

from app.runtime import RuntimeManager
from app.runner.run import _build_frontier_identity_basis
from app.runner.pause_resume import validate_resume_identity_basis


def _make_graph() -> Dict[str, Any]:
	return {
		"nodes": [
			{"id": "n1", "data": {"kind": "transform", "params": {"op": "select", "keep": ["title"]}}},
			{"id": "n2", "data": {"kind": "model", "params": {"model": "qwen3.5:4b"}}},
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


def _make_snapshot(*, run_id: str, graph_id: str, graph: Dict[str, Any], basis: Dict[str, Any], execution_version: str = "v1") -> Dict[str, Any]:
	return {
		"schemaVersion": 2,
		"runId": run_id,
		"graphId": graph_id,
		"graph": graph,
		"runFrom": None,
		"runMode": "from_start",
		"lifecycleState": "paused",
		"executionVersion": execution_version,
		"pausedAt": "2026-03-30T00:00:00Z",
		"state": {"ready": ["n2"]},
		"completedNodeIds": ["n1"],
		"readyNodeIds": ["n2"],
		"blockedNodeIds": [],
		"failedNodeIds": [],
		"resumabilityByNode": {"n1": "safe_boundary_resumable", "n2": "safe_boundary_resumable"},
		"nodeCheckpoints": {
			"n1": {"started": True, "inflightCount": 0, "resumability": "safe_boundary_resumable"},
			"n2": {"started": False, "inflightCount": 0, "resumability": "safe_boundary_resumable"},
		},
		"frontierValidationBasis": basis,
		"leaseState": {"released": True, "activeLeases": 0},
	}


@pytest.mark.asyncio
async def test_resume_validation_uses_full_identity_basis(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-resume-full-basis"
	graph = _make_graph()
	bindings = _make_bindings()
	basis = _build_frontier_identity_basis(
		graph=graph,
		graph_id="graph-full-basis",
		node_ids=["n2"],
		node_bindings=bindings,
		execution_version="v1",
	)
	handle = rt.create_run(run_id)
	handle.status = "paused"
	handle.graph_id = "graph-full-basis"
	handle.graph = graph
	handle.node_bindings = dict(bindings)

	captured: Dict[str, Any] = {}

	def _fake_validate_resume_identity_basis(*, expected_basis, current_basis):
		captured["expected"] = expected_basis
		captured["current"] = current_basis
		return {"ok": False, "reasonCodes": ["node_state_changed"], "nodeIds": ["n2"], "mismatches": []}

	monkeypatch.setattr("app.runtime.validate_resume_identity_basis", _fake_validate_resume_identity_basis)
	await rt.artifact_store.upsert_run_pause_snapshot(
		run_id,
		_make_snapshot(run_id=run_id, graph_id="graph-full-basis", graph=graph, basis=basis),
	)
	result = await rt.request_resume(run_id)
	assert result["resumed"] is False
	assert result["errorCode"] == "RESUME_FRONTIER_VALIDATION_FAILED"
	assert isinstance(captured.get("expected"), dict)
	assert isinstance(captured.get("current"), dict)
	assert isinstance(((captured["current"] or {}).get("nodes") or {}).get("n2"), dict)
	assert str(((captured["current"]["nodes"]["n2"] or {}).get("nodeStateHash") or "")).strip() != ""
	assert str(((captured["current"]["nodes"]["n2"] or {}).get("determinismEnvHash") or "")).strip() != ""


@pytest.mark.asyncio
async def test_resume_fails_on_env_change(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-resume-env-change"
	graph = _make_graph()
	bindings = _make_bindings()
	basis = _build_frontier_identity_basis(
		graph=graph,
		graph_id="graph-env-change",
		node_ids=["n2"],
		node_bindings=bindings,
		execution_version="v1",
	)
	# Mutate determinism-env-driven field.
	graph["nodes"][1]["data"]["params"]["inputEncoding"] = "table_canonical"
	handle = rt.create_run(run_id)
	handle.status = "paused"
	handle.graph_id = "graph-env-change"
	handle.graph = graph
	handle.node_bindings = bindings
	await rt.artifact_store.upsert_run_pause_snapshot(
		run_id,
		_make_snapshot(run_id=run_id, graph_id="graph-env-change", graph=graph, basis=basis),
	)
	result = await rt.request_resume(run_id)
	assert result["resumed"] is False
	assert result["errorCode"] == "RESUME_FRONTIER_VALIDATION_FAILED"
	reasons = list((result.get("details") or {}).get("reasonCodes") or [])
	assert "env_changed" in reasons


@pytest.mark.asyncio
async def test_resume_fails_on_execution_version_change(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-resume-version-change"
	graph = _make_graph()
	bindings = _make_bindings()
	basis = _build_frontier_identity_basis(
		graph=graph,
		graph_id="graph-version-change",
		node_ids=["n2"],
		node_bindings=bindings,
		execution_version="v1",
	)
	handle = rt.create_run(run_id)
	handle.status = "paused"
	handle.graph_id = "graph-version-change"
	handle.graph = graph
	handle.node_bindings = bindings
	await rt.artifact_store.upsert_run_pause_snapshot(
		run_id,
		_make_snapshot(
			run_id=run_id,
			graph_id="graph-version-change",
			graph=graph,
			basis={**basis, "executionVersion": "v999"},
			execution_version="v999",
		),
	)
	result = await rt.request_resume(run_id)
	assert result["resumed"] is False
	assert result["errorCode"] == "RESUME_FRONTIER_VALIDATION_FAILED"
	assert "execution_version_changed" in list((result.get("details") or {}).get("reasonCodes") or [])


@pytest.mark.asyncio
async def test_resume_fails_on_upstream_change(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-resume-upstream-change"
	graph = _make_graph()
	original_bindings = _make_bindings(exec1="exec-old", art1="art-old", exec2="exec-2", art2="art-2")
	basis = _build_frontier_identity_basis(
		graph=graph,
		graph_id="graph-upstream-change",
		node_ids=["n2"],
		node_bindings=original_bindings,
		execution_version="v1",
	)
	handle = rt.create_run(run_id)
	handle.status = "paused"
	handle.graph_id = "graph-upstream-change"
	handle.graph = graph
	handle.node_bindings = _make_bindings(exec1="exec-new", art1="art-new", exec2="exec-2", art2="art-2")
	await rt.artifact_store.upsert_run_pause_snapshot(
		run_id,
		_make_snapshot(run_id=run_id, graph_id="graph-upstream-change", graph=graph, basis=basis),
	)
	result = await rt.request_resume(run_id)
	assert result["resumed"] is False
	assert result["errorCode"] == "RESUME_FRONTIER_VALIDATION_FAILED"
	assert "dependency_frontier_changed" in list((result.get("details") or {}).get("reasonCodes") or [])


@pytest.mark.asyncio
async def test_resume_fails_on_node_state_change(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-resume-node-state-change"
	graph = _make_graph()
	bindings = _make_bindings()
	basis = _build_frontier_identity_basis(
		graph=graph,
		graph_id="graph-node-state-change",
		node_ids=["n2"],
		node_bindings=bindings,
		execution_version="v1",
	)
	handle = rt.create_run(run_id)
	handle.status = "paused"
	handle.graph_id = "graph-node-state-change"
	handle.graph = graph
	handle.node_bindings = bindings
	# Change frontier node config to modify node state hash.
	graph["nodes"][1]["data"]["params"]["temperature"] = 0.1
	await rt.artifact_store.upsert_run_pause_snapshot(
		run_id,
		_make_snapshot(run_id=run_id, graph_id="graph-node-state-change", graph=graph, basis=basis),
	)
	result = await rt.request_resume(run_id)
	assert result["resumed"] is False
	assert result["errorCode"] == "RESUME_FRONTIER_VALIDATION_FAILED"
	assert "node_state_changed" in list((result.get("details") or {}).get("reasonCodes") or [])


@pytest.mark.asyncio
async def test_resume_failure_returns_structured_reason(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-resume-structured-reason"
	graph = _make_graph()
	bindings = _make_bindings()
	basis = _build_frontier_identity_basis(
		graph=graph,
		graph_id="graph-structured-reason",
		node_ids=["n2"],
		node_bindings=bindings,
		execution_version="v1",
	)
	handle = rt.create_run(run_id)
	handle.status = "paused"
	handle.graph_id = "graph-structured-reason"
	handle.graph = graph
	handle.node_bindings = _make_bindings(exec1="exec-x", art1="art-x")
	await rt.artifact_store.upsert_run_pause_snapshot(
		run_id,
		_make_snapshot(run_id=run_id, graph_id="graph-structured-reason", graph=graph, basis=basis),
	)
	result = await rt.request_resume(run_id)
	assert result["resumed"] is False
	assert isinstance(result.get("details"), dict)
	assert isinstance((result.get("details") or {}).get("reasonCodes"), list)
	assert isinstance((result.get("details") or {}).get("nodeIds"), list)
	assert isinstance((result.get("details") or {}).get("mismatches"), list)
	assert isinstance((result.get("details") or {}).get("contractDiff"), dict)


@pytest.mark.asyncio
async def test_pause_snapshot_persists_full_basis(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-pause-persist-basis"
	graph = _make_graph()
	handle = rt.create_run(run_id)
	handle.status = "paused"
	handle.graph_id = "graph-pause-persist-basis"
	handle.graph = graph
	handle.node_bindings = _make_bindings()
	basis = _build_frontier_identity_basis(
		graph=graph,
		graph_id="graph-pause-persist-basis",
		node_ids=["n2"],
		node_bindings=handle.node_bindings,
		execution_version="v1",
	)
	snapshot = _make_snapshot(run_id=run_id, graph_id="graph-pause-persist-basis", graph=graph, basis=basis)
	await rt.artifact_store.upsert_run_pause_snapshot(run_id, snapshot)
	loaded = await rt.artifact_store.get_run_pause_snapshot(run_id)
	assert isinstance(loaded, dict)
	frontier = loaded.get("frontierValidationBasis") if isinstance(loaded.get("frontierValidationBasis"), dict) else {}
	node_basis = (frontier.get("nodes") if isinstance(frontier.get("nodes"), dict) else {}).get("n2")
	assert isinstance(node_basis, dict)
	assert str(node_basis.get("nodeStateHash") or "").strip() != ""
	assert str(node_basis.get("determinismEnvHash") or "").strip() != ""
	assert isinstance(node_basis.get("upstreamBindings"), dict)


@pytest.mark.asyncio
async def test_snapshot_load_validates_schema(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-resume-invalid-schema"
	handle = rt.create_run(run_id)
	handle.status = "paused"
	handle.graph_id = "graph-invalid-schema"
	handle.graph = _make_graph()
	handle.node_bindings = _make_bindings()
	await rt.artifact_store.upsert_run_pause_snapshot(
		run_id,
		{"schemaVersion": 999, "runId": run_id, "graphId": "graph-invalid-schema"},
	)
	result = await rt.request_resume(run_id)
	assert result["resumed"] is False
	assert result["errorCode"] == "PAUSE_SNAPSHOT_SCHEMA_INVALID"


def test_resume_fails_only_on_real_frontier_change_not_snapshot_bug():
	expected_basis = {
		"graphId": "graph-x",
		"executionVersion": "v1",
		"nodes": {
			"n1": {
				"nodeId": "n1",
				"nodeStateHash": "hash-a",
				"determinismEnvHash": "env-a",
				"binding": {"currentExecKey": "", "currentArtifactId": ""},
				"upstreamBindings": {"u1": {"currentExecKey": "", "currentArtifactId": ""}},
				"executionVersion": "v1",
			}
		},
	}
	current_basis = {
		"graphId": "graph-x",
		"executionVersion": "v1",
		"nodes": {
			"n1": {
				"nodeId": "n1",
				"nodeStateHash": "hash-a",
				"determinismEnvHash": "env-a",
				"binding": {"currentExecKey": "exec-real", "currentArtifactId": "art-real"},
				"upstreamBindings": {"u1": {"currentExecKey": "exec-up", "currentArtifactId": "art-up"}},
				"executionVersion": "v1",
			}
		},
	}
	validation = validate_resume_identity_basis(expected_basis=expected_basis, current_basis=current_basis)
	assert validation["ok"] is False
	reasons = set(validation.get("reasonCodes") or [])
	assert "snapshot_binding_empty_mismatch" in reasons
	assert "snapshot_upstream_binding_empty_mismatch" in reasons
