from __future__ import annotations

import asyncio

import pytest

from app.runtime import RuntimeManager


@pytest.mark.asyncio
async def test_runtime_emits_invariant_violation_event(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-invariant-violation"
	handle = rt.create_run(run_id)

	await handle.bus.emit({"type": "run_started"})
	await handle.bus.emit({"type": "node_started", "nodeId": "n1"})
	await handle.bus.emit({"type": "run_pausing"})
	await handle.bus.emit({"type": "run_paused"})
	await asyncio.sleep(0)

	events = await rt.list_run_events(run_id, after_id=0, limit=500)
	violations = [e for e in events if str(e.get("type") or "") == "state_invariant_violation"]
	assert violations
	payload = violations[-1].get("payload") if isinstance(violations[-1].get("payload"), dict) else {}
	assert payload.get("code") == "RUN_PAUSED_HAS_ACTIVE_NODES"


@pytest.mark.asyncio
async def test_runtime_emits_invariant_summary_on_finish(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-invariant-summary"
	handle = rt.create_run(run_id)

	await handle.bus.emit({"type": "run_started"})
	await handle.bus.emit({"type": "run_finished", "status": "succeeded"})
	await asyncio.sleep(0)

	events = await rt.list_run_events(run_id, after_id=0, limit=500)
	summaries = [e for e in events if str(e.get("type") or "") == "invariant_summary"]
	assert summaries
	payload = summaries[-1].get("payload") if isinstance(summaries[-1].get("payload"), dict) else {}
	assert payload.get("status") == "succeeded"
	assert isinstance(payload.get("violations"), int)


@pytest.mark.asyncio
async def test_runtime_emits_control_plane_invariant_violations(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-control-plane-invariant"
	handle = rt.create_run(run_id)

	await handle.bus.emit({"type": "run_started"})
	await handle.bus.emit(
		{
			"type": "run_telemetry",
			"controlPlane": {
				"monotonicViolation": True,
				"terminalWithInflightNodeIds": ["n1"],
			},
		}
	)
	await asyncio.sleep(0)

	events = await rt.list_run_events(run_id, after_id=0, limit=500)
	violations = [e for e in events if str(e.get("type") or "") == "state_invariant_violation"]
	assert violations
	codes = {
		str((evt.get("payload") or {}).get("code") or "")
		for evt in violations
		if isinstance(evt.get("payload"), dict)
	}
	assert "CONTROL_SIGNAL_SEQ_NON_MONOTONIC" in codes
	assert "NODE_TERMINAL_WITH_INFLIGHT" in codes


@pytest.mark.asyncio
async def test_runtime_terminal_active_nodes_warn_only_and_no_strict_raise(monkeypatch, caplog):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	monkeypatch.setenv("RUNTIME_INVARIANTS_STRICT", "1")
	rt = RuntimeManager()
	run_id = "run-terminal-active-warn"
	handle = rt.create_run(run_id)
	handle.status = "failed"
	handle.node_status["n1"] = "running"

	rt._run_invariants(handle, trigger="test_terminal_warn_only")
	await asyncio.sleep(0)

	events = await rt.list_run_events(run_id, after_id=0, limit=500)
	violations = [e for e in events if str(e.get("type") or "") == "state_invariant_violation"]
	assert violations
	payload = violations[-1].get("payload") if isinstance(violations[-1].get("payload"), dict) else {}
	assert payload.get("code") == "RUN_TERMINAL_HAS_ACTIVE_NODES"

	warn_messages = [record.message for record in caplog.records if record.levelname == "WARNING"]
	error_messages = [record.message for record in caplog.records if record.levelname == "ERROR"]
	assert any("RUN_TERMINAL_HAS_ACTIVE_NODES" in message for message in warn_messages)
	assert not any("RUN_TERMINAL_HAS_ACTIVE_NODES" in message for message in error_messages)


@pytest.mark.asyncio
async def test_runtime_strict_still_raises_for_non_transient_invariant(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	monkeypatch.setenv("RUNTIME_INVARIANTS_STRICT", "1")
	rt = RuntimeManager()
	run_id = "run-paused-active-strict"
	handle = rt.create_run(run_id)
	handle.status = "paused"
	handle.node_status["n1"] = "running"

	with pytest.raises(RuntimeError) as excinfo:
		rt._run_invariants(handle, trigger="test_strict_non_transient")

	assert "RUN_PAUSED_HAS_ACTIVE_NODES" in str(excinfo.value)
