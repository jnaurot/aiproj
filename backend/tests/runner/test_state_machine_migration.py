from app.runner.state_machine_migration import (
	canonicalize_event_payload,
	canonicalize_node_status,
	canonicalize_run_status,
	validate_migrated_event_payload,
)


def test_canonicalize_run_status_maps_legacy_values():
	outcome = canonicalize_run_status("cancelled")
	assert outcome.changed is True
	assert outcome.value == "canceled"
	assert outcome.reason == "ok"

	done = canonicalize_run_status("done")
	assert done.changed is True
	assert done.value == "succeeded"


def test_canonicalize_node_status_maps_legacy_values():
	outcome = canonicalize_node_status("error")
	assert outcome.changed is True
	assert outcome.value == "failed"


def test_canonicalize_event_payload_maps_legacy_types_and_statuses():
	payload = {"type": "run_cancelled", "runId": "run-1", "status": "cancelled"}
	migrated, changed, notes = canonicalize_event_payload(payload)
	assert changed is True
	assert migrated["type"] == "run_canceled"
	assert migrated["status"] == "canceled"
	assert notes


def test_validate_migrated_event_payload_rejects_invalid_terminal_status():
	ok, reason = validate_migrated_event_payload({"type": "run_finished", "status": "mystery"})
	assert ok is False
	assert reason == "invalid_run_finished_status"
