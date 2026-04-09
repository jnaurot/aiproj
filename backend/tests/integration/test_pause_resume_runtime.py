from __future__ import annotations

from typing import Any, Dict

import pytest

from app.runtime import RuntimeManager
from app.runner.run import _build_frontier_identity_basis
from app.runner.execution_contract import compare_execution_contracts


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
		"state": {
			"ready": ["n2"],
			"controlPlane": {
				"edgeControlState": {
					"e1": {
						"edgeId": "e1",
						"open": False,
						"closed": True,
						"depth": 0,
						"blocked": False,
						"lastSeq": 12,
						"updatedAt": "2026-03-30T00:00:00Z",
					}
				},
				"lastSeq": 12,
				"activeLeaseNodeIds": [],
			},
		},
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

	def _fake_compare_execution_contracts(*, expected_contract, current_contract):
		captured["expected"] = expected_contract
		captured["current"] = current_contract
		return {"ok": False, "reasonCodes": ["node_state_changed"], "nodeIds": ["n2"], "mismatches": []}

	monkeypatch.setattr("app.runtime.compare_execution_contracts", _fake_compare_execution_contracts)
	await rt.artifact_store.upsert_run_pause_snapshot(
		run_id,
		_make_snapshot(run_id=run_id, graph_id="graph-full-basis", graph=graph, basis=basis),
	)
	result = await rt.request_resume(run_id)
	assert result["resumed"] is False
	assert result["errorCode"] == "RESUME_FRONTIER_VALIDATION_FAILED"
	assert isinstance(captured.get("expected"), dict)
	assert isinstance(captured.get("current"), dict)
	basis = (captured["current"] or {}).get("basis") if isinstance((captured["current"] or {}).get("basis"), dict) else {}
	assert isinstance((basis.get("nodes") or {}).get("n2"), dict)
	assert str((((basis.get("nodes") or {}).get("n2") or {}).get("nodeStateHash") or "")).strip() != ""
	assert str((((basis.get("nodes") or {}).get("n2") or {}).get("determinismEnvHash") or "")).strip() != ""


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
async def test_run_paused_snapshot_merge_preserves_binding_status_fields(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-pause-merge-preserves-status"
	graph = _make_graph()
	handle = rt.create_run(run_id)
	handle.graph_id = "graph-pause-merge-preserves-status"
	handle.graph = graph
	handle.status = "pausing"
	handle.node_bindings = {
		"n1": {
			"status": "succeeded_up_to_date",
			"isUpToDate": True,
			"cacheValid": True,
			"currentRunId": run_id,
			"currentExecKey": "exec-pre-1",
			"currentArtifactId": "art-pre-1",
			"lastExecKey": "exec-pre-1",
			"lastArtifactId": "art-pre-1",
			"staleReason": None,
		},
		"n2": {
			"status": "running",
			"isUpToDate": False,
			"cacheValid": False,
			"currentRunId": run_id,
			"currentExecKey": "exec-pre-2",
			"currentArtifactId": "art-pre-2",
			"lastExecKey": "exec-old-2",
			"lastArtifactId": "art-old-2",
			"staleReason": "RUN_PENDING",
		},
	}
	basis = _build_frontier_identity_basis(
		graph=graph,
		graph_id="graph-pause-merge-preserves-status",
		node_ids=["n1", "n2"],
		node_bindings=_make_bindings(exec1="exec-new-1", art1="art-new-1", exec2="exec-new-2", art2="art-new-2"),
		execution_version="v1",
	)
	snapshot = _make_snapshot(
		run_id=run_id,
		graph_id="graph-pause-merge-preserves-status",
		graph=graph,
		basis=basis,
	)

	await handle.bus.emit({"type": "run_paused", "runId": run_id, "at": "2026-04-08T20:00:00Z", "snapshot": snapshot})

	assert handle.status == "paused"
	assert handle.node_bindings["n1"]["status"] == "succeeded_up_to_date"
	assert bool(handle.node_bindings["n1"]["isUpToDate"]) is True
	assert bool(handle.node_bindings["n1"]["cacheValid"]) is True
	assert str(handle.node_bindings["n1"]["currentExecKey"] or "") == "exec-new-1"
	assert str(handle.node_bindings["n1"]["currentArtifactId"] or "") == "art-new-1"
	assert handle.node_bindings["n2"]["status"] == "running"
	assert bool(handle.node_bindings["n2"]["isUpToDate"]) is False
	assert str(handle.node_bindings["n2"]["staleReason"] or "") == "RUN_PENDING"
	assert str(handle.node_bindings["n2"]["currentExecKey"] or "") == "exec-new-2"
	assert str(handle.node_bindings["n2"]["currentArtifactId"] or "") == "art-new-2"


@pytest.mark.asyncio
async def test_run_paused_snapshot_merge_preserves_component_parent_boundary_binding(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-pause-merge-component-boundary"
	graph = {
		"nodes": [
			{"id": "n_component", "data": {"kind": "component", "params": {}}},
			{"id": "n_internal", "data": {"kind": "tool", "params": {"provider": "builtin"}}},
			{"id": "n_down", "data": {"kind": "tool", "params": {"provider": "builtin"}}},
		],
		"edges": [
			{"id": "e_internal_to_component", "source": "n_internal", "target": "n_component", "targetHandle": "summary", "data": {"mode": "work"}},
			{"id": "e_component_to_down", "source": "n_component", "sourceHandle": "summary", "target": "n_down", "targetHandle": "in", "data": {"mode": "work"}},
		],
	}
	handle = rt.create_run(run_id)
	handle.graph_id = "graph-pause-merge-component-boundary"
	handle.graph = graph
	handle.status = "pausing"
	handle.node_bindings = {
		"n_component": {
			"status": "running",
			"isUpToDate": False,
			"cacheValid": False,
			"currentRunId": run_id,
			"currentExecKey": None,
			"currentArtifactId": None,
			"lastExecKey": "exec-old-component",
			"lastArtifactId": "art-old-component",
			"staleReason": None,
		},
	}
	snapshot = _make_snapshot(
		run_id=run_id,
		graph_id="graph-pause-merge-component-boundary",
		graph=graph,
		basis={
			"schemaVersion": 1,
			"graphId": "graph-pause-merge-component-boundary",
			"executionVersion": "v1",
			"environmentHash": "env-hash",
			"nodes": {
				"n_component": {
					"nodeId": "n_component",
					"nodeStateHash": "state-component",
					"determinismEnvHash": "env-component",
					"binding": {"currentExecKey": "exec-component", "currentArtifactId": "art-component"},
					"upstreamBindings": {
						"n_internal": {"currentExecKey": "exec-internal", "currentArtifactId": "art-internal"},
					},
					"executionVersion": "v1",
				},
				"n_down": {
					"nodeId": "n_down",
					"nodeStateHash": "state-down",
					"determinismEnvHash": "env-down",
					"binding": {"currentExecKey": "", "currentArtifactId": ""},
					"upstreamBindings": {
						"n_component": {"currentExecKey": "exec-component", "currentArtifactId": "art-component"},
					},
					"executionVersion": "v1",
				},
			},
		},
	)

	await handle.bus.emit({"type": "run_paused", "runId": run_id, "at": "2026-04-08T20:20:00Z", "snapshot": snapshot})

	assert handle.status == "paused"
	assert str(handle.node_bindings["n_component"]["currentExecKey"] or "") == "exec-component"
	assert str(handle.node_bindings["n_component"]["currentArtifactId"] or "") == "art-component"
	assert str(handle.node_bindings["n_component"]["status"] or "") == "running"
	assert str((handle.node_bindings.get("n_internal") or {}).get("currentExecKey") or "") == "exec-internal"
	assert str((handle.node_bindings.get("n_internal") or {}).get("currentArtifactId") or "") == "art-internal"


def test_frontier_basis_binding_hydration_marks_artifact_lineage_as_runnable_state(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	graph = _make_graph()
	basis = _build_frontier_identity_basis(
		graph=graph,
		graph_id="graph-frontier-bindings",
		node_ids=["n2"],
		node_bindings=_make_bindings(exec2="exec-frontier-2", art2="art-frontier-2"),
		execution_version="v1",
	)
	hydrated = rt._node_bindings_from_frontier_basis(basis)  # pylint: disable=protected-access
	assert isinstance(hydrated, dict)
	n2 = hydrated.get("n2") or {}
	assert str(n2.get("currentExecKey") or "") == "exec-frontier-2"
	assert str(n2.get("currentArtifactId") or "") == "art-frontier-2"
	assert str(n2.get("lastExecKey") or "") == "exec-frontier-2"
	assert str(n2.get("lastArtifactId") or "") == "art-frontier-2"
	assert str(n2.get("status") or "") == "succeeded_up_to_date"
	assert bool(n2.get("isUpToDate")) is True
	assert bool(n2.get("cacheValid")) is True


@pytest.mark.asyncio
async def test_request_resume_rehydrates_graph_from_experiment_params_when_snapshot_graph_missing(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-resume-fallback-graph"
	graph = _make_graph()
	bindings = _make_bindings()
	basis = _build_frontier_identity_basis(
		graph=graph,
		graph_id="graph-resume-fallback-graph",
		node_ids=["n2"],
		node_bindings=bindings,
		execution_version="v1",
	)
	snapshot = _make_snapshot(
		run_id=run_id,
		graph_id="graph-resume-fallback-graph",
		graph={"nodes": [], "edges": []},
		basis=basis,
	)

	await rt.artifact_store.update_run_status(run_id, "paused")
	await rt.artifact_store.upsert_run_pause_snapshot(run_id, snapshot)
	await rt.artifact_store.upsert_run_experiment(
		{
			"runId": run_id,
			"graphId": "graph-resume-fallback-graph",
			"status": "paused",
			"params": {"graph": graph},
		}
	)

	monkeypatch.setattr(
		"app.runtime.compare_execution_contracts",
		lambda *, expected_contract, current_contract: {"ok": True, "mismatches": [], "reasonCodes": [], "nodeIds": []},
	)

	captured: Dict[str, Any] = {}

	async def _fake_start_run(
		run_id_arg: str,
		graph_arg: Dict[str, Any],
		run_from_arg: Any,
		*,
		run_mode: str | None = None,
		graph_id: str = "",
		resume_snapshot: Dict[str, Any] | None = None,
	) -> Any:
		captured["runId"] = run_id_arg
		captured["graph"] = graph_arg
		captured["graphId"] = graph_id
		captured["runMode"] = run_mode
		captured["resumeSnapshot"] = resume_snapshot
		return {"runId": run_id_arg}

	monkeypatch.setattr(rt, "start_run", _fake_start_run)
	result = await rt.request_resume(run_id)
	assert result["resumed"] is True
	assert str(captured.get("runId") or "") == run_id
	rehydrated_graph = captured.get("graph") if isinstance(captured.get("graph"), dict) else {}
	assert len(rehydrated_graph.get("nodes") or []) > 0
	assert str(captured.get("graphId") or "") == "graph-resume-fallback-graph"


@pytest.mark.asyncio
async def test_request_resume_prefers_snapshot_graph_when_live_handle_graph_is_empty(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-resume-live-handle-empty-graph"
	graph = _make_graph()
	bindings = _make_bindings()
	basis = _build_frontier_identity_basis(
		graph=graph,
		graph_id="graph-resume-live-handle-empty-graph",
		node_ids=["n2"],
		node_bindings=bindings,
		execution_version="v1",
	)
	snapshot = _make_snapshot(
		run_id=run_id,
		graph_id="graph-resume-live-handle-empty-graph",
		graph=graph,
		basis=basis,
	)
	handle = rt.create_run(run_id)
	handle.status = "paused"
	handle.graph_id = "graph-resume-live-handle-empty-graph"
	handle.graph = {"nodes": [], "edges": []}
	handle.node_bindings = dict(bindings)
	await rt.artifact_store.upsert_run_pause_snapshot(run_id, snapshot)

	monkeypatch.setattr(
		"app.runtime.compare_execution_contracts",
		lambda *, expected_contract, current_contract: {"ok": True, "mismatches": [], "reasonCodes": [], "nodeIds": []},
	)

	captured: Dict[str, Any] = {}

	async def _fake_start_run(
		run_id_arg: str,
		graph_arg: Dict[str, Any],
		run_from_arg: Any,
		*,
		run_mode: str | None = None,
		graph_id: str = "",
		resume_snapshot: Dict[str, Any] | None = None,
	) -> Any:
		captured["runId"] = run_id_arg
		captured["graph"] = graph_arg
		captured["graphId"] = graph_id
		captured["runMode"] = run_mode
		captured["resumeSnapshot"] = resume_snapshot
		return {"runId": run_id_arg}

	monkeypatch.setattr(rt, "start_run", _fake_start_run)
	result = await rt.request_resume(run_id)
	assert result["resumed"] is True
	rehydrated_graph = captured.get("graph") if isinstance(captured.get("graph"), dict) else {}
	assert len(rehydrated_graph.get("nodes") or []) > 0
	assert str(captured.get("graphId") or "") == "graph-resume-live-handle-empty-graph"


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


@pytest.mark.asyncio
async def test_snapshot_load_rejects_malformed_control_plane_state(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-resume-invalid-control-plane"
	handle = rt.create_run(run_id)
	handle.status = "paused"
	handle.graph_id = "graph-invalid-control-plane"
	handle.graph = _make_graph()
	handle.node_bindings = _make_bindings()
	basis = _build_frontier_identity_basis(
		graph=handle.graph,
		graph_id="graph-invalid-control-plane",
		node_ids=["n2"],
		node_bindings=handle.node_bindings,
		execution_version="v1",
	)
	snapshot = _make_snapshot(
		run_id=run_id,
		graph_id="graph-invalid-control-plane",
		graph=handle.graph,
		basis=basis,
	)
	state = snapshot.get("state") if isinstance(snapshot.get("state"), dict) else {}
	state["controlPlane"] = {"edgeControlState": [], "lastSeq": -1}
	snapshot["state"] = state
	await rt.artifact_store.upsert_run_pause_snapshot(run_id, snapshot)
	result = await rt.request_resume(run_id)
	assert result["resumed"] is False
	assert result["errorCode"] == "PAUSE_SNAPSHOT_SCHEMA_INVALID"


@pytest.mark.asyncio
async def test_snapshot_load_rejects_malformed_runtime_metrics_state(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-resume-invalid-runtime-metrics"
	handle = rt.create_run(run_id)
	handle.status = "paused"
	handle.graph_id = "graph-invalid-runtime-metrics"
	handle.graph = _make_graph()
	handle.node_bindings = _make_bindings()
	basis = _build_frontier_identity_basis(
		graph=handle.graph,
		graph_id="graph-invalid-runtime-metrics",
		node_ids=["n2"],
		node_bindings=handle.node_bindings,
		execution_version="v1",
	)
	snapshot = _make_snapshot(
		run_id=run_id,
		graph_id="graph-invalid-runtime-metrics",
		graph=handle.graph,
		basis=basis,
	)
	state = snapshot.get("state") if isinstance(snapshot.get("state"), dict) else {}
	state["runtimeItemMetrics"] = {"nodeCounters": []}
	state["runtimeTotals"] = {"cached": "x"}
	snapshot["state"] = state
	await rt.artifact_store.upsert_run_pause_snapshot(run_id, snapshot)
	result = await rt.request_resume(run_id)
	assert result["resumed"] is False
	assert result["errorCode"] == "PAUSE_SNAPSHOT_SCHEMA_INVALID"


def test_resume_contract_compare_fails_on_frontier_binding_change():
	expected_contract = {
		"contractVersion": 1,
		"graphId": "graph-x",
		"basis": {
			"schemaVersion": 1,
			"graphId": "graph-x",
			"executionVersion": "v1",
			"environmentHash": "env-hash",
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
		},
	}
	current_contract = {
		"contractVersion": 1,
		"graphId": "graph-x",
		"basis": {
			"schemaVersion": 1,
			"graphId": "graph-x",
			"executionVersion": "v1",
			"environmentHash": "env-hash",
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
		},
	}
	validation = compare_execution_contracts(
		expected_contract=expected_contract,
		current_contract=current_contract,
	)
	assert validation["ok"] is False
	reasons = set(validation.get("reasonCodes") or [])
	assert "dependency_frontier_changed" in reasons
