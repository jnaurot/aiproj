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
