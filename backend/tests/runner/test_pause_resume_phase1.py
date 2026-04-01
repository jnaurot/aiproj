from __future__ import annotations

import asyncio
import importlib
import sys
import types
from typing import Any

import pytest

from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
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
