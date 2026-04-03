from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

CONTROL_SIGNAL_V1 = 1

CONTROL_SIGNAL_TYPES = frozenset(
	{
		"UPSTREAM_OPENED",
		"ITEM_ENQUEUED",
		"INPUT_DRAINED",
		"UPSTREAM_CLOSED",
		"INPUT_READY",
		"INPUT_BLOCKED",
		"NODE_ACTIVE",
		"NODE_QUIESCENT",
		"NODE_TERMINAL",
		# Existing runtime control signals (kept for compatibility).
		"READY",
		"BUSY",
		"DRAIN",
		"PAUSE",
		"BLOCKED",
		"RESUME",
		"LLM_ACQUIRED",
		"LLM_RELEASED",
	}
)

EDGE_CONTROL_SIGNAL_TYPES = frozenset(
	{
		"UPSTREAM_OPENED",
		"ITEM_ENQUEUED",
		"INPUT_DRAINED",
		"UPSTREAM_CLOSED",
		"INPUT_READY",
		"INPUT_BLOCKED",
	}
)


def normalize_control_signal_type(raw: Any) -> Optional[str]:
	value = str(raw or "").strip()
	if not value:
		return None
	if value in CONTROL_SIGNAL_TYPES:
		return value
	upper = value.upper().replace("-", "_").replace(" ", "_")
	if upper in CONTROL_SIGNAL_TYPES:
		return upper
	return None


def enrich_control_signal_event(evt: Dict[str, Any]) -> Optional[Dict[str, Any]]:
	if not isinstance(evt, dict):
		return None
	if str(evt.get("type") or "").strip() != "control_signal":
		return dict(evt)
	signal_type = normalize_control_signal_type(evt.get("signal"))
	if not signal_type:
		return None
	out = dict(evt)
	out["signal"] = signal_type.lower()
	out["event_version"] = int(CONTROL_SIGNAL_V1)
	out["payload_type"] = "control_signal.v1"
	out["control_signal"] = {
		"version": int(CONTROL_SIGNAL_V1),
		"signalType": signal_type,
		"runId": str(out.get("runId") or ""),
		"graphId": str(out.get("graphId") or ""),
		"nodeId": str(out.get("nodeId") or ""),
	}
	return out


def empty_edge_control_state(edge_id: str) -> Dict[str, Any]:
	return {
		"edgeId": str(edge_id or "").strip(),
		"open": False,
		"closed": False,
		"depth": 0,
		"blocked": False,
		"lastSeq": 0,
		"updatedAt": "",
	}


def reduce_edge_control_state(
	previous: Optional[Dict[str, Any]],
	*,
	edge_id: str,
	signal_type: str,
	seq: int,
	at: str,
) -> Dict[str, Any]:
	next_state = dict(previous or empty_edge_control_state(edge_id))
	next_state["edgeId"] = str(edge_id or "").strip()
	prev_seq = int(next_state.get("lastSeq") or 0)
	current_seq = max(0, int(seq or 0))
	if current_seq and current_seq < prev_seq:
		return next_state
	signal = normalize_control_signal_type(signal_type)
	if not signal or signal not in EDGE_CONTROL_SIGNAL_TYPES:
		next_state["lastSeq"] = max(prev_seq, current_seq)
		next_state["updatedAt"] = str(at or "")
		return next_state
	if signal == "UPSTREAM_OPENED":
		next_state["open"] = True
		next_state["closed"] = False
	elif signal == "ITEM_ENQUEUED":
		next_state["open"] = True
		next_state["depth"] = max(0, int(next_state.get("depth") or 0) + 1)
	elif signal == "INPUT_DRAINED":
		next_state["depth"] = 0
	elif signal == "UPSTREAM_CLOSED":
		next_state["open"] = False
		next_state["closed"] = True
	elif signal == "INPUT_BLOCKED":
		next_state["blocked"] = True
	elif signal == "INPUT_READY":
		next_state["blocked"] = False
	next_state["lastSeq"] = max(prev_seq, current_seq)
	next_state["updatedAt"] = str(at or "")
	return next_state


def can_node_terminalize(
	*,
	required_work_edge_ids: Iterable[str],
	edge_control_state: Dict[str, Dict[str, Any]],
	inflight_count: int,
	has_active_lease: bool,
) -> bool:
	if int(inflight_count or 0) > 0:
		return False
	if bool(has_active_lease):
		return False
	for edge_id in required_work_edge_ids:
		key = str(edge_id or "").strip()
		if not key:
			continue
		state = edge_control_state.get(key) or {}
		if not bool(state.get("closed")):
			return False
		if int(state.get("depth") or 0) > 0:
			return False
	return True
