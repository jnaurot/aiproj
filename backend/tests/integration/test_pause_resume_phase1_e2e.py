from __future__ import annotations

import asyncio
import importlib
import sys
import types
from typing import Any

import pytest

from app.runtime import RuntimeManager
from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput
from app.runner.run import _build_frontier_identity_basis


def _ensure_duckdb_stub() -> None:
	if "duckdb" not in sys.modules:
		sys.modules["duckdb"] = types.SimpleNamespace()


def _make_graph() -> dict[str, Any]:
	return {
		"nodes": [
			{"id": "n1", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "n2", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [{"id": "e_work", "source": "n1", "target": "n2", "targetHandle": "in", "data": {"mode": "work"}}],
	}


@pytest.mark.asyncio
async def test_e2e_pause_resume_valid(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	first_started = asyncio.Event()
	call_counts = {"n1": 0, "n2": 0}

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		call_counts[node_id] = int(call_counts.get(node_id, 0)) + 1
		if node_id == "n1":
			first_started.set()
			await asyncio.sleep(0.05)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=2.0,
			data={"kind": "json", "payload": {"node": node_id}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = _make_graph()
	events_pause: list[dict[str, Any]] = []
	pause_event = asyncio.Event()
	artifact_store = MemoryArtifactStore()
	cache = ExecutionCache()
	bus_pause = RunEventBus("run-e2e-valid", on_emit=lambda evt: events_pause.append(dict(evt)))
	task = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-e2e-valid",
			graph=graph,
			run_from=None,
			bus=bus_pause,
			artifact_store=artifact_store,
			cache=cache,
			graph_id="graph-e2e-valid",
			pause_event=pause_event,
		)
	)
	await asyncio.wait_for(first_started.wait(), timeout=2.0)
	pause_event.set()
	await asyncio.wait_for(task, timeout=5.0)
	paused_evt = next((evt for evt in events_pause if str(evt.get("type") or "") == "run_paused"), None)
	assert isinstance(paused_evt, dict)
	snapshot = paused_evt.get("snapshot")
	assert isinstance(snapshot, dict)

	events_resume: list[dict[str, Any]] = []
	bus_resume = RunEventBus("run-e2e-valid", on_emit=lambda evt: events_resume.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-e2e-valid",
		graph=graph,
		run_from=None,
		bus=bus_resume,
		artifact_store=artifact_store,
		cache=cache,
		graph_id="graph-e2e-valid",
		resume_snapshot=snapshot,
	)
	assert any(str(evt.get("type") or "") == "run_resumed" for evt in events_resume)
	assert any(str(evt.get("type") or "") == "run_finished" and str(evt.get("status") or "") == "succeeded" for evt in events_resume)


@pytest.mark.asyncio
async def test_e2e_resume_fails_after_change(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-e2e-fail-after-change"
	graph = _make_graph()
	bindings = {
		"n1": {"currentExecKey": "exec-1", "currentArtifactId": "art-1"},
		"n2": {"currentExecKey": "exec-2", "currentArtifactId": "art-2"},
	}
	basis = _build_frontier_identity_basis(
		graph=graph,
		graph_id="graph-e2e-fail-after-change",
		node_ids=["n2"],
		node_bindings=bindings,
		execution_version="v1",
	)
	handle = rt.create_run(run_id)
	handle.status = "paused"
	handle.graph_id = "graph-e2e-fail-after-change"
	handle.graph = graph
	handle.node_bindings = {
		"n1": {"currentExecKey": "exec-new", "currentArtifactId": "art-new"},
		"n2": {"currentExecKey": "exec-2", "currentArtifactId": "art-2"},
	}
	await rt.artifact_store.upsert_run_pause_snapshot(
		run_id,
		{
			"schemaVersion": 2,
			"runId": run_id,
			"graphId": "graph-e2e-fail-after-change",
			"graph": graph,
			"runFrom": None,
			"runMode": "from_start",
			"lifecycleState": "paused",
			"executionVersion": "v1",
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
		},
	)
	result = await rt.request_resume(run_id)
	assert result["resumed"] is False
	assert result["errorCode"] == "RESUME_FRONTIER_VALIDATION_FAILED"


@pytest.mark.asyncio
async def test_e2e_pause_waits_for_non_resumable(monkeypatch):
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	started = asyncio.Event()
	release = asyncio.Event()
	events: list[dict[str, Any]] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		started.set()
		await release.wait()
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=2.0,
			data={"kind": "json", "payload": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{
				"id": "n_tool",
				"data": {
					"kind": "tool",
					"params": {
						"provider": "builtin",
						"builtin": {"toolId": "noop"},
						"side_effect_mode": "effectful",
						"armed": True,
					},
				},
			}
		],
		"edges": [],
	}
	pause_event = asyncio.Event()
	bus = RunEventBus("run-e2e-nonres", on_emit=lambda evt: events.append(dict(evt)))
	task = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-e2e-nonres",
			graph=graph,
			run_from=None,
			bus=bus,
			artifact_store=MemoryArtifactStore(),
			cache=ExecutionCache(),
			graph_id="graph-e2e-nonres",
			pause_event=pause_event,
		)
	)
	await asyncio.wait_for(started.wait(), timeout=2.0)
	pause_event.set()
	await asyncio.sleep(0.05)
	assert not any(str(evt.get("type") or "") == "run_paused" for evt in events)
	release.set()
	await asyncio.wait_for(task, timeout=5.0)
	assert any(str(evt.get("type") or "") == "run_paused" for evt in events)


@pytest.mark.asyncio
async def test_e2e_snapshot_survives_restart(monkeypatch, tmp_path):
	artifact_dir = tmp_path / "artifacts"
	monkeypatch.setenv("ARTIFACT_STORE", "disk")
	monkeypatch.setenv("ARTIFACT_DIR", str(artifact_dir))
	run_id = "run-e2e-survive-restart"
	graph = _make_graph()
	bindings = {
		"n1": {"currentExecKey": "exec-1", "currentArtifactId": "art-1"},
		"n2": {"currentExecKey": "exec-2", "currentArtifactId": "art-2"},
	}
	basis = _build_frontier_identity_basis(
		graph=graph,
		graph_id="graph-e2e-survive-restart",
		node_ids=["n2"],
		node_bindings=bindings,
		execution_version="v1",
	)
	rt1 = RuntimeManager()
	handle = rt1.create_run(run_id)
	handle.status = "paused"
	handle.graph_id = "graph-e2e-survive-restart"
	handle.graph = graph
	handle.node_bindings = bindings
	await rt1.artifact_store.update_run_status(run_id, "paused")
	await rt1.artifact_store.upsert_run_pause_snapshot(
		run_id,
		{
			"schemaVersion": 2,
			"runId": run_id,
			"graphId": "graph-e2e-survive-restart",
			"graph": graph,
			"runFrom": None,
			"runMode": "from_start",
			"lifecycleState": "paused",
			"executionVersion": "v1",
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
		},
	)

	rt2 = RuntimeManager()
	result = await rt2.request_resume(run_id)
	assert result["resumed"] is True
	assert result["status"] == "resuming"


@pytest.mark.asyncio
async def test_e2e_model_node_pause_resume_binding_consistency(monkeypatch):
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	started = asyncio.Event()
	release = asyncio.Event()

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "n1":
			started.set()
			await release.wait()
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=5.0,
			data={"kind": "json", "payload": {"pass": True, "score": 90}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "n1", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "n_model", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [{"id": "e_work", "source": "n1", "target": "n_model", "targetHandle": "in", "data": {"mode": "work"}}],
	}
	events: list[dict[str, Any]] = []
	pause_event = asyncio.Event()
	artifact_store = MemoryArtifactStore()
	cache = ExecutionCache()
	bus_pause = RunEventBus("run-e2e-model-binding", on_emit=lambda evt: events.append(dict(evt)))
	task = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-e2e-model-binding",
			graph=graph,
			run_from=None,
			bus=bus_pause,
			artifact_store=artifact_store,
			cache=cache,
			graph_id="graph-e2e-model-binding",
			pause_event=pause_event,
		)
	)
	await asyncio.wait_for(started.wait(), timeout=2.0)
	pause_event.set()
	await asyncio.sleep(0.05)
	release.set()
	await asyncio.wait_for(task, timeout=5.0)
	paused_evt = next((evt for evt in events if str(evt.get("type") or "") == "run_paused"), None)
	assert isinstance(paused_evt, dict)
	snapshot = paused_evt.get("snapshot") if isinstance(paused_evt.get("snapshot"), dict) else {}
	basis = snapshot.get("frontierValidationBasis") if isinstance(snapshot.get("frontierValidationBasis"), dict) else {}
	nodes = basis.get("nodes") if isinstance(basis.get("nodes"), dict) else {}
	model_basis = nodes.get("n_model") if isinstance(nodes.get("n_model"), dict) else {}
	upstream = model_basis.get("upstreamBindings") if isinstance(model_basis.get("upstreamBindings"), dict) else {}
	n1_upstream = upstream.get("n1") if isinstance(upstream.get("n1"), dict) else {}
	assert str(n1_upstream.get("currentExecKey") or "").strip() != ""
	assert str(n1_upstream.get("currentArtifactId") or "").strip() != ""
