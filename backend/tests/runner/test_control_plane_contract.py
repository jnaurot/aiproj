from __future__ import annotations

from app.runner.control_plane import (
	CONTROL_SIGNAL_V1,
	enrich_control_signal_event,
	normalize_control_signal_type,
)


def test_control_signal_normalization_accepts_known_values() -> None:
	assert normalize_control_signal_type("ready") == "READY"
	assert normalize_control_signal_type("LLM_RELEASED") == "LLM_RELEASED"
	assert normalize_control_signal_type("input_drained") == "INPUT_DRAINED"


def test_control_signal_normalization_rejects_unknown_values() -> None:
	assert normalize_control_signal_type("made_up_signal") is None
	assert normalize_control_signal_type("") is None
	assert normalize_control_signal_type(None) is None


def test_enrich_control_signal_event_adds_v1_envelope() -> None:
	evt = {
		"type": "control_signal",
		"runId": "run-1",
		"graphId": "graph-1",
		"nodeId": "node-1",
		"signal": "ready",
		"at": "2026-04-01T00:00:00Z",
	}
	enriched = enrich_control_signal_event(evt)
	assert isinstance(enriched, dict)
	assert enriched["signal"] == "ready"
	assert enriched["event_version"] == int(CONTROL_SIGNAL_V1)
	assert enriched["payload_type"] == "control_signal.v1"
	cp = enriched.get("control_signal")
	assert isinstance(cp, dict)
	assert cp.get("version") == int(CONTROL_SIGNAL_V1)
	assert cp.get("signalType") == "READY"


def test_enrich_control_signal_event_rejects_unknown_signal() -> None:
	evt = {"type": "control_signal", "runId": "run-1", "signal": "unknown_signal"}
	assert enrich_control_signal_event(evt) is None


def test_enrich_non_control_event_passthrough() -> None:
	evt = {"type": "node_started", "runId": "run-1", "nodeId": "n1"}
	out = enrich_control_signal_event(evt)
	assert out == evt
