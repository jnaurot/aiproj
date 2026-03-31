from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from .execution_state import NODE_STATES, RUN_STATES


LEGACY_RUN_STATUS_MAP: Dict[str, str] = {
	"cancelled": "canceled",
	"complete": "succeeded",
	"completed": "succeeded",
	"done": "succeeded",
	"error": "failed",
	"errored": "failed",
	"success": "succeeded",
}

LEGACY_NODE_STATUS_MAP: Dict[str, str] = {
	"cancelled": "canceled",
	"done": "succeeded",
	"error": "failed",
	"errored": "failed",
	"success": "succeeded",
}

LEGACY_EVENT_TYPE_MAP: Dict[str, str] = {
	"run_cancelled": "run_canceled",
	"node_cancelled": "node_canceled",
}

_KNOWN_RUN_TERMINALS = {"succeeded", "failed", "canceled", "deleted", "paused"}


@dataclass(frozen=True)
class MigrationOutcome:
	changed: bool
	value: str
	reason: str


def _normalize_token(raw: Any) -> str:
	return str(raw or "").strip().lower()


def canonicalize_run_status(raw: Any) -> MigrationOutcome:
	value = _normalize_token(raw)
	if not value:
		return MigrationOutcome(False, value, "missing")
	mapped = LEGACY_RUN_STATUS_MAP.get(value, value)
	if mapped not in RUN_STATES:
		return MigrationOutcome(False, value, "unknown_run_status")
	return MigrationOutcome(mapped != value, mapped, "ok")


def canonicalize_node_status(raw: Any) -> MigrationOutcome:
	value = _normalize_token(raw)
	if not value:
		return MigrationOutcome(False, value, "missing")
	mapped = LEGACY_NODE_STATUS_MAP.get(value, value)
	if mapped not in NODE_STATES and mapped not in {"succeeded", "failed", "canceled"}:
		return MigrationOutcome(False, value, "unknown_node_status")
	return MigrationOutcome(mapped != value, mapped, "ok")


def canonicalize_event_payload(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], bool, List[str]]:
	if not isinstance(payload, dict):
		return payload, False, ["invalid_payload_type"]
	out = dict(payload)
	notes: List[str] = []
	changed = False

	etype = _normalize_token(out.get("type"))
	if etype:
		mapped_type = LEGACY_EVENT_TYPE_MAP.get(etype, etype)
		if mapped_type != etype:
			out["type"] = mapped_type
			notes.append(f"type:{etype}->{mapped_type}")
			changed = True
		etype = mapped_type

	if etype == "run_finished":
		status_outcome = canonicalize_run_status(out.get("status"))
		if status_outcome.reason == "ok":
			if status_outcome.changed:
				out["status"] = status_outcome.value
				notes.append(f"run_finished.status->{status_outcome.value}")
				changed = True
		elif status_outcome.reason != "missing":
			notes.append(status_outcome.reason)
	elif etype == "node_finished":
		status_outcome = canonicalize_node_status(out.get("status"))
		if status_outcome.reason == "ok" and status_outcome.changed:
			out["status"] = status_outcome.value
			notes.append(f"node_finished.status->{status_outcome.value}")
			changed = True
	elif etype in {"run_canceled", "node_canceled"} and "status" in out:
		status_outcome = canonicalize_run_status(out.get("status"))
		if status_outcome.reason == "ok" and status_outcome.changed:
			out["status"] = status_outcome.value
			notes.append(f"{etype}.status->{status_outcome.value}")
			changed = True

	return out, changed, notes


def validate_migrated_event_payload(payload: Dict[str, Any]) -> Tuple[bool, str]:
	if not isinstance(payload, dict):
		return False, "invalid_payload_type"
	etype = _normalize_token(payload.get("type"))
	if not etype:
		return False, "missing_type"
	if etype == "run_finished":
		status_outcome = canonicalize_run_status(payload.get("status"))
		if status_outcome.reason != "ok":
			return False, "invalid_run_finished_status"
	if etype == "node_finished":
		status_outcome = canonicalize_node_status(payload.get("status"))
		if status_outcome.reason != "ok":
			return False, "invalid_node_finished_status"
	return True, "ok"


def summarize_migration_report(report: Dict[str, Any]) -> Dict[str, Any]:
	runs = report.get("runs") if isinstance(report.get("runs"), dict) else {}
	events = report.get("events") if isinstance(report.get("events"), dict) else {}
	snapshots = report.get("snapshots") if isinstance(report.get("snapshots"), dict) else {}
	return {
		"runsScanned": int(runs.get("scanned", 0)),
		"runsFixed": int(runs.get("fixed", 0)),
		"runsSkipped": int(runs.get("skipped", 0)),
		"eventsScanned": int(events.get("scanned", 0)),
		"eventsFixed": int(events.get("fixed", 0)),
		"eventsSkipped": int(events.get("skipped", 0)),
		"snapshotsScanned": int(snapshots.get("scanned", 0)),
		"snapshotsInvalid": int(snapshots.get("invalid", 0)),
	}


def json_preview(value: Any, limit: int = 240) -> str:
	try:
		raw = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
	except Exception:
		raw = str(value)
	if len(raw) <= limit:
		return raw
	return raw[: max(0, int(limit) - 3)] + "..."


def is_terminal_run_status(status: str) -> bool:
	return _normalize_token(status) in _KNOWN_RUN_TERMINALS
