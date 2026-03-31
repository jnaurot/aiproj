from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, FrozenSet


RUN_STATES: FrozenSet[str] = frozenset(
	{
		"pending",
		"running",
		"cancel_requested",
		"pausing",
		"paused",
		"resuming",
		"succeeded",
		"failed",
		"canceled",
		"deleted",
	}
)


NODE_STATES: FrozenSet[str] = frozenset(
	{
		"idle",
		"running",
		"active",
		"blocked",
		"paused",
		"succeeded_up_to_date",
		"failed",
		"canceled",
		"stale",
	}
)


RUN_TRANSITIONS: Dict[str, FrozenSet[str]] = {
	"pending": frozenset({"running", "paused", "cancel_requested", "pausing", "failed", "canceled", "deleted"}),
	"running": frozenset({"cancel_requested", "pausing", "succeeded", "failed", "canceled", "deleted"}),
	"cancel_requested": frozenset({"canceled", "failed", "pausing", "deleted"}),
	"pausing": frozenset({"paused", "canceled", "failed", "deleted"}),
	"paused": frozenset({"resuming", "canceled", "failed", "deleted"}),
	"resuming": frozenset({"running", "paused", "canceled", "failed", "deleted"}),
	"succeeded": frozenset({"succeeded"}),
	"failed": frozenset({"failed"}),
	"canceled": frozenset({"canceled"}),
	"deleted": frozenset({"deleted"}),
}


NODE_TRANSITIONS: Dict[str, FrozenSet[str]] = {
	"idle": frozenset({"idle", "running", "blocked", "paused", "active", "stale"}),
	"running": frozenset(
		{
			"running",
			"succeeded_up_to_date",
			"failed",
			"canceled",
			"blocked",
			"paused",
			"active",
			"stale",
		}
	),
	"active": frozenset({"active", "running", "blocked", "paused", "succeeded_up_to_date", "failed", "canceled", "stale"}),
	"blocked": frozenset({"blocked", "running", "active", "paused", "failed", "canceled", "stale"}),
	"paused": frozenset({"paused", "active", "running", "failed", "canceled", "stale"}),
	"succeeded_up_to_date": frozenset({"succeeded_up_to_date", "running", "blocked", "paused", "stale"}),
	"failed": frozenset({"failed", "running", "blocked", "paused", "stale"}),
	"canceled": frozenset({"canceled", "running", "blocked", "paused", "stale"}),
	"stale": frozenset({"stale", "running", "blocked", "paused", "active", "failed", "canceled", "succeeded_up_to_date"}),
}


@dataclass(frozen=True)
class TransitionDecision:
	ok: bool
	source: str
	target: str
	reason: str


def _normalize(value: str, *, fallback: str = "") -> str:
	return str(value or "").strip().lower() or fallback


def can_transition_run(source: str, target: str) -> TransitionDecision:
	src = _normalize(source)
	tgt = _normalize(target)
	if not src or not tgt:
		return TransitionDecision(False, src, tgt, "missing_state")
	if src not in RUN_STATES:
		return TransitionDecision(False, src, tgt, "unknown_source_state")
	if tgt not in RUN_STATES:
		return TransitionDecision(False, src, tgt, "unknown_target_state")
	if tgt in RUN_TRANSITIONS.get(src, frozenset()):
		return TransitionDecision(True, src, tgt, "ok")
	return TransitionDecision(False, src, tgt, "illegal_transition")


def can_transition_node(source: str, target: str) -> TransitionDecision:
	src = _normalize(source, fallback="idle")
	tgt = _normalize(target)
	if not tgt:
		return TransitionDecision(False, src, tgt, "missing_state")
	if src not in NODE_STATES:
		return TransitionDecision(False, src, tgt, "unknown_source_state")
	if tgt not in NODE_STATES:
		return TransitionDecision(False, src, tgt, "unknown_target_state")
	if tgt in NODE_TRANSITIONS.get(src, frozenset()):
		return TransitionDecision(True, src, tgt, "ok")
	return TransitionDecision(False, src, tgt, "illegal_transition")
