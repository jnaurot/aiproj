from __future__ import annotations

from typing import Any, Dict

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.runtime import RuntimeManager
from app.runner.run import _build_frontier_identity_basis


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


def _make_bindings() -> Dict[str, Dict[str, Any]]:
	return {
		"n1": {"currentExecKey": "exec-1", "currentArtifactId": "art-1"},
		"n2": {"currentExecKey": "exec-2", "currentArtifactId": "art-2"},
	}


def _make_contract(graph: Dict[str, Any], graph_id: str, bindings: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
	basis = _build_frontier_identity_basis(
		graph=graph,
		graph_id=graph_id,
		node_ids=["n2"],
		node_bindings=bindings,
		execution_version="v1",
	)
	return {"contractVersion": 1, "graphId": graph_id, "basis": basis}


@pytest.mark.asyncio
async def test_e2e_export_import_replay_consistency(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt_export = RuntimeManager()
	run_id = "run-portable-export-1"
	graph = _make_graph()
	bindings = _make_bindings()
	contract = _make_contract(graph, "graph-portable-1", bindings)

	handle = rt_export.create_run(run_id)
	handle.status = "succeeded"
	handle.graph_id = "graph-portable-1"
	handle.graph = graph
	handle.node_bindings = dict(bindings)
	handle.execution_contract = dict(contract)
	await rt_export.artifact_store.update_run_status(run_id, "succeeded")
	await rt_export.event_store.append_event(
		{
			"type": "run_started",
			"runId": run_id,
			"graphId": "graph-portable-1",
			"executionContract": contract,
			"at": "2026-03-31T00:00:00Z",
		}
	)
	await rt_export.event_store.append_event(
		{
			"type": "run_finished",
			"runId": run_id,
			"status": "succeeded",
			"at": "2026-03-31T00:00:10Z",
		}
	)

	package = await rt_export.export_run_package(run_id)
	rt_import = RuntimeManager()
	import_result = await rt_import.import_run_package(package=package)
	assert import_result["imported"] is True
	imported_run_id = import_result["runId"]

	captured: Dict[str, Any] = {}

	async def _fake_start_run(run_id, graph_payload, run_from, run_mode=None, graph_id=None, resume_snapshot=None):
		captured["run_id"] = run_id
		captured["graph"] = graph_payload
		captured["graph_id"] = graph_id

	monkeypatch.setattr(rt_import, "start_run", _fake_start_run)
	replay_result = await rt_import.request_replay(source_run_id=imported_run_id)
	assert replay_result["replayed"] is True, replay_result
	assert captured["graph_id"] == "graph-portable-1"
	assert isinstance(captured["graph"], dict)


def test_run_package_integrity_tamper_fails_import():
	with TestClient(app) as client:
		rt = app.state.runtime
		fake_package = {
			"schemaVersion": 1,
			"runId": "run-portable-tamper-1",
			"graphId": "graph-portable-tamper",
			"status": "succeeded",
			"createdAt": "2026-03-31T00:00:00Z",
			"runFrom": None,
			"runMode": None,
			"graph": {"nodes": [], "edges": []},
			"executionContract": {},
			"events": [],
			"artifactRefs": [],
		}
		checksum = rt._run_package_checksum(dict(fake_package))  # type: ignore[attr-defined]
		fake_package["integrity"] = {"algorithm": "sha256", "checksum": checksum}
		fake_package["status"] = "failed"  # tamper after checksum
		res = client.post("/runs/package/import", json={"package": fake_package})
		assert res.status_code == 409, res.text
		detail = res.json().get("detail") or {}
		assert detail.get("errorCode") == "RUN_PACKAGE_INTEGRITY_FAILED"
