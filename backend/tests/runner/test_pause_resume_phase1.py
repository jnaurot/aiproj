from __future__ import annotations

import asyncio
import importlib
import sys
import types
from typing import Any

import pytest

from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.components import ExpandedComponentGraph
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


def _ensure_duckdb_stub() -> None:
	if "duckdb" not in sys.modules:
		sys.modules["duckdb"] = types.SimpleNamespace()


def _event_index(events: list[dict[str, Any]], event_type: str, node_id: str | None = None) -> int:
	for idx, evt in enumerate(events):
		if str(evt.get("type") or "") != event_type:
			continue
		if node_id is not None and str(evt.get("nodeId") or "") != node_id:
			continue
		return idx
	return -1


@pytest.mark.asyncio
async def test_pause_safe_boundary_emits_pausing_then_paused_without_run_finished(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	graph = {
		"nodes": [{"id": "n_tool", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}}],
		"edges": [],
	}
	events: list[dict[str, Any]] = []
	bus = RunEventBus("run-pause-safe", on_emit=lambda evt: events.append(dict(evt)))
	pause_event = asyncio.Event()
	pause_event.set()
	await run_mod.run_graph(
		run_id="run-pause-safe",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-pause-safe",
		pause_event=pause_event,
	)

	assert _event_index(events, "run_pause_requested") >= 0
	assert _event_index(events, "run_pausing") >= 0
	assert _event_index(events, "run_paused") >= 0
	assert _event_index(events, "run_finished") < 0


@pytest.mark.asyncio
async def test_pause_waits_for_inflight_atomic_work_before_paused(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	started = asyncio.Event()
	release = asyncio.Event()

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		started.set()
		await release.wait()
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=10.0,
			data={"kind": "json", "payload": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	graph = {
		"nodes": [{"id": "n_tool", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}}],
		"edges": [],
	}
	events: list[dict[str, Any]] = []
	bus = RunEventBus("run-pause-inflight", on_emit=lambda evt: events.append(dict(evt)))
	pause_event = asyncio.Event()
	task = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-pause-inflight",
			graph=graph,
			run_from=None,
			bus=bus,
			artifact_store=MemoryArtifactStore(),
			cache=ExecutionCache(),
			graph_id="graph-pause-inflight",
			pause_event=pause_event,
		)
	)
	await asyncio.wait_for(started.wait(), timeout=2.0)
	pause_event.set()
	await asyncio.sleep(0.05)
	assert _event_index(events, "run_paused") < 0
	release.set()
	await asyncio.wait_for(task, timeout=5.0)
	idx_finished = _event_index(events, "node_finished", "n_tool")
	idx_paused = _event_index(events, "run_paused")
	assert idx_finished >= 0
	assert idx_paused > idx_finished


@pytest.mark.asyncio
async def test_resume_uses_pause_snapshot_and_skips_already_completed_node(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	pause_after_first_started = asyncio.Event()
	first_started = asyncio.Event()
	call_counts: dict[str, int] = {"n1": 0, "n2": 0}

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		call_counts[node_id] = int(call_counts.get(node_id, 0)) + 1
		if node_id == "n1":
			first_started.set()
			await asyncio.sleep(0.05)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=5.0,
			data={"kind": "json", "payload": {"node": node_id}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	graph = {
		"nodes": [
			{"id": "n1", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "n2", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [{"id": "e_work", "source": "n1", "target": "n2", "targetHandle": "in", "data": {"mode": "work"}}],
	}

	artifact_store = MemoryArtifactStore()
	cache = ExecutionCache()
	pause_events: list[dict[str, Any]] = []
	pause_bus = RunEventBus("run-resume-phase1", on_emit=lambda evt: pause_events.append(dict(evt)))
	pause_event = asyncio.Event()

	first_run = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-resume-phase1",
			graph=graph,
			run_from=None,
			bus=pause_bus,
			artifact_store=artifact_store,
			cache=cache,
			graph_id="graph-resume-phase1",
			pause_event=pause_event,
		)
	)
	await asyncio.wait_for(first_started.wait(), timeout=2.0)
	pause_event.set()
	await asyncio.wait_for(first_run, timeout=5.0)

	run_paused_evt = next((evt for evt in pause_events if str(evt.get("type") or "") == "run_paused"), None)
	assert isinstance(run_paused_evt, dict)
	snapshot = run_paused_evt.get("snapshot") if isinstance(run_paused_evt.get("snapshot"), dict) else None
	assert isinstance(snapshot, dict)
	assert _event_index(pause_events, "node_started", "n2") < 0

	resume_events: list[dict[str, Any]] = []
	resume_bus = RunEventBus("run-resume-phase1", on_emit=lambda evt: resume_events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-resume-phase1",
		graph=graph,
		run_from=None,
		bus=resume_bus,
		artifact_store=artifact_store,
		cache=cache,
		graph_id="graph-resume-phase1",
		resume_snapshot=snapshot,
	)

	assert _event_index(resume_events, "run_resumed") >= 0
	assert _event_index(resume_events, "node_started", "n1") < 0
	assert _event_index(resume_events, "node_started", "n2") >= 0
	assert call_counts["n1"] == 1
	assert call_counts["n2"] == 1


@pytest.mark.asyncio
async def test_resume_hydrates_bindings_for_once_node_upstream_inputs(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	first_started = asyncio.Event()
	call_counts: dict[str, int] = {"n1": 0, "n2": 0}
	n2_upstream_lengths: list[int] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		call_counts[node_id] = int(call_counts.get(node_id, 0)) + 1
		if node_id == "n1":
			first_started.set()
			await asyncio.sleep(0.05)
		if node_id == "n2":
			n2_upstream_lengths.append(len(list(upstream_artifact_ids or [])))
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=5.0,
			data={"kind": "json", "payload": {"node": node_id}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	graph = {
		"nodes": [
			{"id": "n1", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "n2", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [{"id": "e_work", "source": "n1", "target": "n2", "targetHandle": "in", "data": {"mode": "work"}}],
	}

	artifact_store = MemoryArtifactStore()
	cache = ExecutionCache()
	pause_events: list[dict[str, Any]] = []
	pause_bus = RunEventBus("run-resume-bindings-hydrate", on_emit=lambda evt: pause_events.append(dict(evt)))
	pause_event = asyncio.Event()

	first_run = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-resume-bindings-hydrate",
			graph=graph,
			run_from=None,
			bus=pause_bus,
			artifact_store=artifact_store,
			cache=cache,
			graph_id="graph-resume-bindings-hydrate",
			pause_event=pause_event,
		)
	)
	await asyncio.wait_for(first_started.wait(), timeout=2.0)
	pause_event.set()
	await asyncio.wait_for(first_run, timeout=5.0)
	paused_evt = next((evt for evt in pause_events if str(evt.get("type") or "") == "run_paused"), None)
	assert isinstance(paused_evt, dict)
	snapshot = paused_evt.get("snapshot")
	assert isinstance(snapshot, dict)

	# On the paused run, only n1 should have executed.
	assert call_counts["n1"] == 1
	assert call_counts["n2"] == 0

	resume_events: list[dict[str, Any]] = []
	resume_bus = RunEventBus("run-resume-bindings-hydrate", on_emit=lambda evt: resume_events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-resume-bindings-hydrate",
		graph=graph,
		run_from=None,
		bus=resume_bus,
		artifact_store=artifact_store,
		cache=cache,
		graph_id="graph-resume-bindings-hydrate",
		resume_snapshot=snapshot,
	)

	assert _event_index(resume_events, "run_resumed") >= 0
	assert _event_index(resume_events, "node_started", "n1") < 0
	assert _event_index(resume_events, "node_started", "n2") >= 0
	assert call_counts["n1"] == 1
	assert call_counts["n2"] == 1
	# The resumed n2 execution must receive its upstream artifact binding.
	assert n2_upstream_lengths and n2_upstream_lengths[-1] >= 1


@pytest.mark.asyncio
async def test_resume_skips_component_reexpand_for_preexpanded_pause_graph(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"node": str(node.get("id") or "")}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	preexpanded_graph = {
		"nodes": [
			{
				"id": "n_component",
				"data": {
					"kind": "component",
					"params": {
						"componentRef": {"componentId": "c1", "revisionId": "r1", "apiVersion": "v1"},
						"api": {
							"outputs": [
								{"name": "summary", "required": True, "typedSchema": {"type": "json"}}
							]
						},
					},
				},
			},
			{
				"id": "cmp:n_component:n_internal",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"meta": {
						"component": {
							"componentId": "c1",
							"componentRevisionId": "r1",
							"instanceNodeId": "n_component",
						}
					},
				},
			},
			{
				"id": "n_down",
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}},
			},
		],
		"edges": [
			{
				"id": "e_internal_parent",
				"source": "cmp:n_component:n_internal",
				"target": "n_component",
				"targetHandle": "summary",
				"data": {"mode": "work"},
			},
			{
				"id": "e_parent_down",
				"source": "n_component",
				"sourceHandle": "summary",
				"target": "n_down",
				"targetHandle": "in",
				"data": {"mode": "work"},
			},
		],
	}
	basis = run_mod._build_frontier_identity_basis(  # pylint: disable=protected-access
		graph=preexpanded_graph,
		graph_id="graph-resume-preexpanded",
		node_ids=["n_down"],
		node_bindings={"n_component": {"currentExecKey": "exec-parent", "currentArtifactId": "art-parent"}},
		execution_version="v1",
	)
	resume_snapshot = {
		"schemaVersion": 2,
		"runId": "run-resume-preexpanded",
		"graphId": "graph-resume-preexpanded",
		"graph": preexpanded_graph,
		"runFrom": None,
		"runMode": "from_start",
		"lifecycleState": "paused",
		"executionVersion": "v1",
		"pausedAt": "2026-04-08T23:05:00Z",
		"state": {
			"ready": ["n_down"],
			"blockedDescendants": [],
			"indeg": {"n_component": 1, "cmp:n_component:n_internal": 0, "n_down": 1},
			"depsReleased": {"n_component": True, "cmp:n_component:n_internal": True, "n_down": True},
			"edgeDependencyReleased": {"e_internal_parent": True, "e_parent_down": True},
			"nodeStartedOnce": {"n_component": True, "cmp:n_component:n_internal": True, "n_down": False},
			"nodeInflightCounts": {"n_component": 0, "cmp:n_component:n_internal": 0, "n_down": 0},
			"providedWorkEdgesByHandle": {},
			"providedNonworkEdgesByHandle": {},
			"queueRegistry": {},
			"controlPlane": {"edgeControlState": {}, "lastSeq": 0, "activeLeaseNodeIds": []},
			"runtimeItemMetrics": {},
			"nodeRuntimeMetrics": {},
			"runtimeTotals": {"cached": 0, "succeeded": 0, "failed": 0, "softFailed": 0, "peakConcurrency": 0},
		},
		"completedNodeIds": ["cmp:n_component:n_internal", "n_component"],
		"readyNodeIds": ["n_down"],
		"blockedNodeIds": [],
		"failedNodeIds": [],
		"resumabilityByNode": {
			"n_component": "safe_boundary_resumable",
			"cmp:n_component:n_internal": "safe_boundary_resumable",
			"n_down": "safe_boundary_resumable",
		},
		"nodeCheckpoints": {
			"n_component": {"started": True, "inflightCount": 0, "resumability": "safe_boundary_resumable"},
			"cmp:n_component:n_internal": {"started": True, "inflightCount": 0, "resumability": "safe_boundary_resumable"},
			"n_down": {"started": False, "inflightCount": 0, "resumability": "safe_boundary_resumable"},
		},
		"frontierValidationBasis": basis,
		"executionContract": {
			"contractVersion": 1,
			"graphId": "graph-resume-preexpanded",
			"basis": basis,
		},
		"leaseState": {"released": True, "activeLeases": 0},
	}

	events: list[dict[str, Any]] = []
	bus = RunEventBus("run-resume-preexpanded", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-resume-preexpanded",
		graph=preexpanded_graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-resume-preexpanded",
		resume_snapshot=resume_snapshot,
	)

	queue_start_logs = [
		str(evt.get("message") or "")
		for evt in events
		if str(evt.get("type") or "") == "log" and "[scheduler] queue start" in str(evt.get("message") or "")
	]
	assert queue_start_logs
	assert any("nodes=0" not in msg for msg in queue_start_logs)
	finished_evt = next((evt for evt in events if str(evt.get("type") or "") == "run_finished"), None)
	assert isinstance(finished_evt, dict)
	assert str(finished_evt.get("errorCode") or "") != "COMPONENT_STORE_UNAVAILABLE"


@pytest.mark.asyncio
async def test_missing_resumability_declaration_fails(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [{"id": "n_unknown", "data": {"kind": "mystery_kind", "params": {}}}],
		"edges": [],
	}
	events: list[dict[str, Any]] = []
	bus = RunEventBus("run-missing-resumability", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-missing-resumability",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-missing-resumability",
	)
	finished = next((evt for evt in events if str(evt.get("type") or "") == "run_finished"), None)
	assert isinstance(finished, dict)
	assert str(finished.get("status") or "") == "failed"
	assert str(finished.get("errorCode") or "") == "RESUMABILITY_DECLARATION_MISSING"


@pytest.mark.asyncio
async def test_non_resumable_blocks_pause_until_complete(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	started = asyncio.Event()
	release = asyncio.Event()

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
	events: list[dict[str, Any]] = []
	bus = RunEventBus("run-nonres-pause", on_emit=lambda evt: events.append(dict(evt)))
	pause_event = asyncio.Event()
	task = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-nonres-pause",
			graph=graph,
			run_from=None,
			bus=bus,
			artifact_store=MemoryArtifactStore(),
			cache=ExecutionCache(),
			graph_id="graph-nonres-pause",
			pause_event=pause_event,
		)
	)
	await asyncio.wait_for(started.wait(), timeout=2.0)
	pause_event.set()
	await asyncio.sleep(0.05)
	assert _event_index(events, "run_paused") < 0
	assert _event_index(events, "node_not_resumable", "n_tool") >= 0
	release.set()
	await asyncio.wait_for(task, timeout=5.0)
	assert _event_index(events, "run_paused") > _event_index(events, "node_finished", "n_tool")


@pytest.mark.asyncio
async def test_pausing_does_not_admit_new_work_plane_execution(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	monkeypatch.setenv("RUNNER_MAX_CONCURRENCY", "1")

	call_counts: dict[str, int] = {"n1": 0, "n2": 0}

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		call_counts[node_id] = int(call_counts.get(node_id, 0)) + 1
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"node": node_id}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "n1", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "n2", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [{"id": "e12", "source": "n1", "target": "n2", "targetHandle": "in", "data": {"mode": "work"}}],
	}
	events: list[dict[str, Any]] = []
	bus = RunEventBus("run-pausing-admission-stop", on_emit=lambda evt: events.append(dict(evt)))
	pause_event = asyncio.Event()
	pause_event.set()
	task = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-pausing-admission-stop",
			graph=graph,
			run_from=None,
			bus=bus,
			artifact_store=MemoryArtifactStore(),
			cache=ExecutionCache(),
			graph_id="graph-pausing-admission-stop",
			pause_event=pause_event,
		)
	)
	await asyncio.wait_for(task, timeout=5.0)
	assert _event_index(events, "run_paused") >= 0
	started_events = [evt for evt in events if str(evt.get("type") or "") == "node_started"]
	assert len(started_events) == 0
	assert call_counts["n1"] + call_counts["n2"] == 0


@pytest.mark.asyncio
async def test_pause_terminalization_requires_snapshot_persisted(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	class _FailingPauseStore(MemoryArtifactStore):
		async def upsert_run_pause_snapshot(self, run_id: str, snapshot: dict[str, Any]) -> None:  # type: ignore[override]
			raise RuntimeError("pause snapshot persist failed")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [{"id": "n1", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}}],
		"edges": [],
	}
	events: list[dict[str, Any]] = []
	bus = RunEventBus("run-pause-snapshot-fail", on_emit=lambda evt: events.append(dict(evt)))
	pause_event = asyncio.Event()
	pause_event.set()
	await run_mod.run_graph(
		run_id="run-pause-snapshot-fail",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=_FailingPauseStore(),
		cache=ExecutionCache(),
		graph_id="graph-pause-snapshot-fail",
		pause_event=pause_event,
	)
	assert _event_index(events, "run_paused") < 0
	finished = next((evt for evt in events if str(evt.get("type") or "") == "run_finished"), None)
	assert isinstance(finished, dict)
	assert str(finished.get("status") or "") == "failed"
	assert str(finished.get("errorCode") or "") == "PAUSE_SNAPSHOT_PERSIST_FAILED"


@pytest.mark.asyncio
async def test_pause_snapshot_not_empty_when_runtime_binding_exists(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	started = asyncio.Event()
	release = asyncio.Event()

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		started.set()
		await release.wait()
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "n1", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "n2", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [{"id": "e12", "source": "n1", "target": "n2", "targetHandle": "in", "data": {"mode": "work"}}],
	}
	events: list[dict[str, Any]] = []
	bus = RunEventBus("run-pause-nonempty-basis", on_emit=lambda evt: events.append(dict(evt)))
	pause_event = asyncio.Event()
	task = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-pause-nonempty-basis",
			graph=graph,
			run_from=None,
			bus=bus,
			artifact_store=MemoryArtifactStore(),
			cache=ExecutionCache(),
			graph_id="graph-pause-nonempty-basis",
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
	node_basis = nodes.get("n2") if isinstance(nodes.get("n2"), dict) else {}
	upstream = node_basis.get("upstreamBindings") if isinstance(node_basis.get("upstreamBindings"), dict) else {}
	n1_upstream = upstream.get("n1") if isinstance(upstream.get("n1"), dict) else {}
	assert str(n1_upstream.get("currentExecKey") or "").strip() != ""
	assert str(n1_upstream.get("currentArtifactId") or "").strip() != ""


@pytest.mark.asyncio
async def test_pause_snapshot_preserves_component_parent_boundary_binding_after_internal_finish(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	parent_node_id = "n_component"
	internal_source_id = "cmp:n_component:n_source"
	internal_model_id = "cmp:n_component:n_model"
	downstream_id = "n_downstream"
	model_started = asyncio.Event()
	model_release = asyncio.Event()

	def _fake_expand_graph_components(graph, component_store=None, max_depth=5):
		expanded_graph = {
			"nodes": [
				{"id": parent_node_id, "data": {"kind": "component", "params": {}}},
				{"id": internal_source_id, "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
				{"id": internal_model_id, "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
				{"id": downstream_id, "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			],
			"edges": [
				{"id": "e_internal_chain", "source": internal_source_id, "target": internal_model_id, "targetHandle": "in", "data": {"mode": "work"}},
				{"id": "e_component_summary", "source": internal_model_id, "target": parent_node_id, "targetHandle": "summary", "data": {"mode": "work"}},
				{"id": "e_downstream", "source": parent_node_id, "sourceHandle": "summary", "target": downstream_id, "targetHandle": "in", "data": {"mode": "work"}},
			],
		}
		return ExpandedComponentGraph(
			graph=expanded_graph,
			internal_to_parent={internal_source_id: parent_node_id, internal_model_id: parent_node_id},
			parent_to_internal={parent_node_id: [internal_source_id, internal_model_id]},
			parent_component_meta={parent_node_id: {"componentId": "c1", "componentRevisionId": "r1"}},
		)

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == internal_model_id:
			model_started.set()
			await model_release.wait()
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"node": node_id}},
		)

	monkeypatch.setattr(run_mod, "expand_graph_components", _fake_expand_graph_components)
	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	graph = {
		"nodes": [{"id": parent_node_id, "data": {"kind": "component", "params": {"componentRef": {"componentId": "c1", "revisionId": "r1"}}}}],
		"edges": [],
	}
	events: list[dict[str, Any]] = []
	bus = RunEventBus(
		"run-pause-component-boundary-binding",
		on_emit=lambda evt: events.append(dict(evt)),
	)
	pause_event = asyncio.Event()
	task = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-pause-component-boundary-binding",
			graph=graph,
			run_from=None,
			bus=bus,
			artifact_store=MemoryArtifactStore(),
			cache=ExecutionCache(),
			graph_id="graph-pause-component-boundary-binding",
			pause_event=pause_event,
		)
	)
	await asyncio.wait_for(model_started.wait(), timeout=2.0)
	pause_event.set()
	await asyncio.sleep(0.05)
	model_release.set()
	await asyncio.wait_for(task, timeout=5.0)
	paused_evt = next((evt for evt in events if str(evt.get("type") or "") == "run_paused"), None)
	assert isinstance(paused_evt, dict)
	snapshot = paused_evt.get("snapshot") if isinstance(paused_evt.get("snapshot"), dict) else {}
	basis = snapshot.get("frontierValidationBasis") if isinstance(snapshot.get("frontierValidationBasis"), dict) else {}
	basis_nodes = basis.get("nodes") if isinstance(basis.get("nodes"), dict) else {}
	parent_basis = basis_nodes.get(parent_node_id) if isinstance(basis_nodes.get(parent_node_id), dict) else {}
	parent_binding = parent_basis.get("binding") if isinstance(parent_basis.get("binding"), dict) else {}
	parent_upstream = parent_basis.get("upstreamBindings") if isinstance(parent_basis.get("upstreamBindings"), dict) else {}
	model_pair = parent_upstream.get(internal_model_id) if isinstance(parent_upstream.get(internal_model_id), dict) else {}
	downstream_basis = basis_nodes.get(downstream_id) if isinstance(basis_nodes.get(downstream_id), dict) else {}
	downstream_upstream = downstream_basis.get("upstreamBindings") if isinstance(downstream_basis.get("upstreamBindings"), dict) else {}
	downstream_parent_pair = downstream_upstream.get(parent_node_id) if isinstance(downstream_upstream.get(parent_node_id), dict) else {}
	assert str(parent_binding.get("currentExecKey") or "").strip() != ""
	assert str(parent_binding.get("currentArtifactId") or "").strip() != ""
	assert parent_binding == model_pair
	assert downstream_parent_pair == parent_binding


@pytest.mark.asyncio
async def test_pause_snapshot_includes_control_plane_state(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	started = asyncio.Event()
	release = asyncio.Event()

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		started.set()
		await release.wait()
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "n1", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "n2", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [{"id": "e12", "source": "n1", "target": "n2", "targetHandle": "in", "data": {"mode": "work"}}],
	}
	events: list[dict[str, Any]] = []
	bus = RunEventBus("run-pause-control-plane-state", on_emit=lambda evt: events.append(dict(evt)))
	pause_event = asyncio.Event()
	task = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-pause-control-plane-state",
			graph=graph,
			run_from=None,
			bus=bus,
			artifact_store=MemoryArtifactStore(),
			cache=ExecutionCache(),
			graph_id="graph-pause-control-plane-state",
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
	state = snapshot.get("state") if isinstance(snapshot.get("state"), dict) else {}
	control_plane = state.get("controlPlane") if isinstance(state.get("controlPlane"), dict) else {}
	assert isinstance(control_plane.get("edgeControlState"), dict)
	assert "e12" in (control_plane.get("edgeControlState") or {})
	assert int(control_plane.get("lastSeq") or 0) > 0
	assert isinstance(control_plane.get("activeLeaseNodeIds"), list)


@pytest.mark.asyncio
async def test_pause_snapshot_includes_runtime_item_metrics(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	first_started = asyncio.Event()

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "n1":
			first_started.set()
			await asyncio.sleep(0.05)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"node": node_id}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "n1", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "n2", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [{"id": "e12", "source": "n1", "target": "n2", "targetHandle": "in", "data": {"mode": "work"}}],
	}
	events: list[dict[str, Any]] = []
	bus = RunEventBus("run-pause-runtime-metrics", on_emit=lambda evt: events.append(dict(evt)))
	pause_event = asyncio.Event()
	task = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-pause-runtime-metrics",
			graph=graph,
			run_from=None,
			bus=bus,
			artifact_store=MemoryArtifactStore(),
			cache=ExecutionCache(),
			graph_id="graph-pause-runtime-metrics",
			pause_event=pause_event,
		)
	)
	await asyncio.wait_for(first_started.wait(), timeout=2.0)
	pause_event.set()
	await asyncio.wait_for(task, timeout=5.0)
	paused_evt = next((evt for evt in events if str(evt.get("type") or "") == "run_paused"), None)
	assert isinstance(paused_evt, dict)
	snapshot = paused_evt.get("snapshot") if isinstance(paused_evt.get("snapshot"), dict) else {}
	state = snapshot.get("state") if isinstance(snapshot.get("state"), dict) else {}
	runtime_item_metrics = state.get("runtimeItemMetrics") if isinstance(state.get("runtimeItemMetrics"), dict) else {}
	node_counters = runtime_item_metrics.get("nodeCounters") if isinstance(runtime_item_metrics.get("nodeCounters"), dict) else {}
	n1 = node_counters.get("n1") if isinstance(node_counters.get("n1"), dict) else {}
	assert int(n1.get("accepted") or 0) >= 1


@pytest.mark.asyncio
async def test_resume_preserves_runtime_item_counters_across_pause_boundary(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	first_started = asyncio.Event()
	call_counts: dict[str, int] = {"n1": 0, "n2": 0}

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		call_counts[node_id] = int(call_counts.get(node_id, 0)) + 1
		if node_id == "n1":
			first_started.set()
			await asyncio.sleep(0.05)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"node": node_id}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "n1", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "n2", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [{"id": "e12", "source": "n1", "target": "n2", "targetHandle": "in", "data": {"mode": "work"}}],
	}
	artifact_store = MemoryArtifactStore()
	cache = ExecutionCache()
	pause_events: list[dict[str, Any]] = []
	pause_bus = RunEventBus("run-resume-runtime-counters", on_emit=lambda evt: pause_events.append(dict(evt)))
	pause_event = asyncio.Event()
	first_run = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-resume-runtime-counters",
			graph=graph,
			run_from=None,
			bus=pause_bus,
			artifact_store=artifact_store,
			cache=cache,
			graph_id="graph-resume-runtime-counters",
			pause_event=pause_event,
		)
	)
	await asyncio.wait_for(first_started.wait(), timeout=2.0)
	pause_event.set()
	await asyncio.wait_for(first_run, timeout=5.0)
	paused_evt = next((evt for evt in pause_events if str(evt.get("type") or "") == "run_paused"), None)
	assert isinstance(paused_evt, dict)
	snapshot = paused_evt.get("snapshot")
	assert isinstance(snapshot, dict)

	resume_events: list[dict[str, Any]] = []
	resume_bus = RunEventBus("run-resume-runtime-counters", on_emit=lambda evt: resume_events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-resume-runtime-counters",
		graph=graph,
		run_from=None,
		bus=resume_bus,
		artifact_store=artifact_store,
		cache=cache,
		graph_id="graph-resume-runtime-counters",
		resume_snapshot=snapshot,
	)
	last_queue = next(
		(
			evt
			for evt in reversed(resume_events)
			if str(evt.get("type") or "") == "queue_metrics" and isinstance(evt.get("runtimeItemMetrics"), dict)
		),
		None,
	)
	assert isinstance(last_queue, dict)
	node_counters = (
		(last_queue.get("runtimeItemMetrics") or {}).get("nodeCounters")
		if isinstance(last_queue.get("runtimeItemMetrics"), dict)
		else {}
	)
	assert isinstance(node_counters, dict)
	n1 = node_counters.get("n1") if isinstance(node_counters.get("n1"), dict) else {}
	n2 = node_counters.get("n2") if isinstance(node_counters.get("n2"), dict) else {}
	assert int(n1.get("accepted") or 0) == 1
	assert int(n2.get("accepted") or 0) == 1


@pytest.mark.asyncio
async def test_resume_rehydrates_control_plane_state_from_snapshot(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	first_started = asyncio.Event()
	call_counts: dict[str, int] = {"n1": 0, "n2": 0}

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		call_counts[node_id] = int(call_counts.get(node_id, 0)) + 1
		if node_id == "n1":
			first_started.set()
			await asyncio.sleep(0.05)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"node": node_id}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "n1", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "n2", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [{"id": "e12", "source": "n1", "target": "n2", "targetHandle": "in", "data": {"mode": "work"}}],
	}

	artifact_store = MemoryArtifactStore()
	cache = ExecutionCache()
	pause_events: list[dict[str, Any]] = []
	pause_bus = RunEventBus("run-resume-control-plane-state", on_emit=lambda evt: pause_events.append(dict(evt)))
	pause_event = asyncio.Event()
	first_run = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-resume-control-plane-state",
			graph=graph,
			run_from=None,
			bus=pause_bus,
			artifact_store=artifact_store,
			cache=cache,
			graph_id="graph-resume-control-plane-state",
			pause_event=pause_event,
		)
	)
	await asyncio.wait_for(first_started.wait(), timeout=2.0)
	pause_event.set()
	await asyncio.wait_for(first_run, timeout=5.0)
	paused_evt = next((evt for evt in pause_events if str(evt.get("type") or "") == "run_paused"), None)
	assert isinstance(paused_evt, dict)
	snapshot = paused_evt.get("snapshot") if isinstance(paused_evt.get("snapshot"), dict) else {}
	state = snapshot.get("state") if isinstance(snapshot.get("state"), dict) else {}
	control_plane = state.get("controlPlane") if isinstance(state.get("controlPlane"), dict) else {}
	control_plane["lastSeq"] = 777
	control_plane["edgeControlState"] = {
		"e12": {
			"edgeId": "e12",
			"open": False,
			"closed": True,
			"depth": 0,
			"blocked": False,
			"lastSeq": 777,
			"updatedAt": "2026-04-03T00:00:00+00:00",
		}
	}
	state["controlPlane"] = control_plane
	snapshot["state"] = state

	resume_events: list[dict[str, Any]] = []
	resume_bus = RunEventBus("run-resume-control-plane-state", on_emit=lambda evt: resume_events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-resume-control-plane-state",
		graph=graph,
		run_from=None,
		bus=resume_bus,
		artifact_store=artifact_store,
		cache=cache,
		graph_id="graph-resume-control-plane-state",
		resume_snapshot=snapshot,
	)
	assert _event_index(resume_events, "run_resumed") >= 0
	assert any(
		str(evt.get("type") or "") == "scheduler_snapshot"
		and int(evt.get("lastControlSeq") or 0) >= 777
		and isinstance(evt.get("controlPlaneEdgeState"), dict)
		and "e12" in (evt.get("controlPlaneEdgeState") or {})
		for evt in resume_events
	)


@pytest.mark.asyncio
async def test_pause_snapshot_order_is_after_binding_commit(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	started = asyncio.Event()
	release = asyncio.Event()

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		started.set()
		await release.wait()
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [{"id": "n1", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}}],
		"edges": [],
	}
	events: list[dict[str, Any]] = []
	bus = RunEventBus("run-pause-order", on_emit=lambda evt: events.append(dict(evt)))
	pause_event = asyncio.Event()
	task = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-pause-order",
			graph=graph,
			run_from=None,
			bus=bus,
			artifact_store=MemoryArtifactStore(),
			cache=ExecutionCache(),
			graph_id="graph-pause-order",
			pause_event=pause_event,
		)
	)
	await asyncio.wait_for(started.wait(), timeout=2.0)
	pause_event.set()
	await asyncio.sleep(0.05)
	release.set()
	await asyncio.wait_for(task, timeout=5.0)
	idx_node_finished = _event_index(events, "node_finished", "n1")
	idx_pause_terminalized_log = next(
		(i for i, evt in enumerate(events) if str(evt.get("type") or "") == "log" and "[pause] terminalized status=paused" in str(evt.get("message") or "")),
		-1,
	)
	idx_run_paused = _event_index(events, "run_paused")
	assert idx_node_finished >= 0
	assert idx_pause_terminalized_log > idx_node_finished
	assert idx_run_paused > idx_node_finished


@pytest.mark.asyncio
async def test_pause_and_resume_use_same_frontier_basis_builder(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	calls = {"pause_builder": 0}
	original_builder = run_mod._build_frontier_identity_basis

	def _wrapped_builder(*, graph, graph_id, node_ids, node_bindings, execution_version):
		calls["pause_builder"] += 1
		return original_builder(
			graph=graph,
			graph_id=graph_id,
			node_ids=node_ids,
			node_bindings=node_bindings,
			execution_version=execution_version,
		)

	monkeypatch.setattr(run_mod, "_build_frontier_identity_basis", _wrapped_builder)
	started = asyncio.Event()
	release = asyncio.Event()

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		started.set()
		await release.wait()
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [{"id": "n1", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}}],
		"edges": [],
	}
	events: list[dict[str, Any]] = []
	bus = RunEventBus("run-same-frontier-builder", on_emit=lambda evt: events.append(dict(evt)))
	pause_event = asyncio.Event()
	task = asyncio.create_task(
		run_mod.run_graph(
			run_id="run-same-frontier-builder",
			graph=graph,
			run_from=None,
			bus=bus,
			artifact_store=MemoryArtifactStore(),
			cache=ExecutionCache(),
			graph_id="graph-same-frontier-builder",
			pause_event=pause_event,
		)
	)
	await asyncio.wait_for(started.wait(), timeout=2.0)
	pause_event.set()
	await asyncio.sleep(0.05)
	release.set()
	await asyncio.wait_for(task, timeout=5.0)
	assert calls["pause_builder"] > 0
