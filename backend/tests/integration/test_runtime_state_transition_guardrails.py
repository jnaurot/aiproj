from __future__ import annotations

import asyncio

import pytest

from app.runtime import RuntimeManager


@pytest.mark.asyncio
async def test_illegal_run_transition_is_blocked_and_emits_violation(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-illegal-transition"
	handle = rt.create_run(run_id)

	assert handle.status == "pending"
	await handle.bus.emit({"type": "run_resume_requested"})
	await asyncio.sleep(0)

	assert handle.status == "pending"
	events = await rt.list_run_events(run_id, after_id=0, limit=200)
	violations = [e for e in events if str(e.get("type") or "") == "state_transition_violation"]
	assert violations
	payload = violations[-1].get("payload") if isinstance(violations[-1].get("payload"), dict) else {}
	assert payload.get("entity") == "run"
	assert payload.get("source") == "pending"
	assert payload.get("target") == "resuming"
	assert payload.get("code") == "illegal_transition"


@pytest.mark.asyncio
async def test_legal_pause_resume_cancel_transition_chain_still_succeeds(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-legal-transition-chain"
	handle = rt.create_run(run_id)

	await handle.bus.emit({"type": "run_started"})
	assert handle.status == "running"
	await handle.bus.emit({"type": "run_pause_requested"})
	assert handle.status == "pausing"
	await handle.bus.emit({"type": "run_paused"})
	assert handle.status == "paused"
	await handle.bus.emit({"type": "run_resume_requested"})
	assert handle.status == "resuming"
	await handle.bus.emit({"type": "run_resumed"})
	assert handle.status == "running"
	await handle.bus.emit({"type": "run_cancel_requested"})
	assert handle.status == "cancel_requested"
	await handle.bus.emit({"type": "run_canceled"})
	assert handle.status == "canceled"

	events = await rt.list_run_events(run_id, after_id=0, limit=500)
	violation_count = sum(1 for e in events if str(e.get("type") or "") == "state_transition_violation")
	assert violation_count == 0


@pytest.mark.asyncio
async def test_duplicate_pause_resume_transitional_events_are_idempotent(monkeypatch):
	monkeypatch.setenv("ARTIFACT_STORE", "memory")
	rt = RuntimeManager()
	run_id = "run-duplicate-transitional-events"
	handle = rt.create_run(run_id)

	await handle.bus.emit({"type": "run_started"})
	assert handle.status == "running"
	await handle.bus.emit({"type": "run_pause_requested"})
	assert handle.status == "pausing"
	# duplicate transition events should be ignored (not violations)
	await handle.bus.emit({"type": "run_pause_requested"})
	await handle.bus.emit({"type": "run_pausing"})
	assert handle.status == "pausing"
	await handle.bus.emit({"type": "run_paused"})
	assert handle.status == "paused"
	await handle.bus.emit({"type": "run_resume_requested"})
	assert handle.status == "resuming"
	await handle.bus.emit({"type": "run_resume_requested"})
	await handle.bus.emit({"type": "run_resuming"})
	assert handle.status == "resuming"
	await handle.bus.emit({"type": "run_resumed"})
	assert handle.status == "running"

	events = await rt.list_run_events(run_id, after_id=0, limit=500)
	violations = [e for e in events if str(e.get("type") or "") == "state_transition_violation"]
	assert not violations
