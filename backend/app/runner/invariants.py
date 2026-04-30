from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List


@dataclass(frozen=True)
class InvariantViolation:
	code: str
	message: str
	severity: str
	node_ids: tuple[str, ...] = ()


_ACTIVE_NODE_STATES = {"running", "active"}
_TERMINAL_RUN_STATES = {"succeeded", "failed", "canceled", "deleted"}


def evaluate_runtime_invariants(
	*,
	run_status: str,
	node_status: Dict[str, str] | None,
	run_telemetry: Dict[str, Any] | None,
) -> List[InvariantViolation]:
	status = str(run_status or "").strip().lower()
	nodes = node_status or {}
	telemetry = run_telemetry or {}
	violations: List[InvariantViolation] = []
	active_nodes = tuple(sorted(str(nid) for nid, st in nodes.items() if str(st or "").strip().lower() in _ACTIVE_NODE_STATES))

	if status == "paused" and active_nodes:
		violations.append(
			InvariantViolation(
				code="RUN_PAUSED_HAS_ACTIVE_NODES",
				message="Run is paused while one or more nodes are still active.",
				severity="error",
				node_ids=active_nodes,
			)
		)

	if status in _TERMINAL_RUN_STATES and active_nodes:
		violations.append(
			InvariantViolation(
				code="RUN_TERMINAL_HAS_ACTIVE_NODES",
				# Severity is "warn" not "error": zombie tasks from non-cancellable
				# async operations (e.g. Ollama HTTP calls) may still be inflight for
				# a brief window after a fatal run is terminated.  This is a transient
				# state, not a data-corruption invariant, so it must not be raised as
				# an error or trigger strict-mode failures.
				message="Run is terminal while one or more nodes are still active.",
				severity="warn",
				node_ids=active_nodes,
			)
		)

	lease = telemetry.get("llmLease") if isinstance(telemetry.get("llmLease"), dict) else {}
	lease_state = str(lease.get("state") or "").strip().lower()
	if status == "paused" and lease_state == "acquired":
		violations.append(
			InvariantViolation(
				code="RUN_PAUSED_HAS_ACTIVE_LEASE",
				message="Run is paused while an execution-critical lease is still acquired.",
				severity="error",
				node_ids=(),
			)
		)

	active_edges = telemetry.get("activeEdges")
	if isinstance(active_edges, Iterable):
		non_work_active: list[str] = []
		for entry in active_edges:
			if not isinstance(entry, dict):
				continue
			if bool(entry.get("active")) is not True:
				continue
			plane = str(entry.get("plane") or "").strip().lower()
			if plane and plane != "work":
				edge_id = str(entry.get("edgeId") or "")
				if edge_id:
					non_work_active.append(edge_id)
		if non_work_active:
			violations.append(
				InvariantViolation(
					code="NON_WORK_EDGE_MARKED_ACTIVE",
					message="Only work-plane edges may be marked active/running.",
					severity="error",
					node_ids=tuple(sorted(non_work_active)),
				)
			)

	control_plane = telemetry.get("controlPlane") if isinstance(telemetry.get("controlPlane"), dict) else {}
	if bool(control_plane.get("monotonicViolation")):
		violations.append(
			InvariantViolation(
				code="CONTROL_SIGNAL_SEQ_NON_MONOTONIC",
				message="Control signal sequence must be strictly monotonic within a run.",
				severity="error",
				node_ids=(),
			)
		)

	duplicate_terminal = tuple(
		sorted(str(node_id) for node_id in (control_plane.get("duplicateNodeTerminalIds") or []) if str(node_id).strip())
	)
	if duplicate_terminal:
		violations.append(
			InvariantViolation(
				code="NODE_TERMINAL_DUPLICATE",
				message="NODE_TERMINAL must be emitted once per node per run.",
				severity="error",
				node_ids=duplicate_terminal,
			)
		)

	terminal_with_inflight = tuple(
		sorted(
			str(node_id)
			for node_id in (control_plane.get("terminalWithInflightNodeIds") or [])
			if str(node_id).strip()
		)
	)
	if terminal_with_inflight:
		violations.append(
			InvariantViolation(
				code="NODE_TERMINAL_WITH_INFLIGHT",
				message="Node terminal implies inflight=0.",
				severity="error",
				node_ids=terminal_with_inflight,
			)
		)

	terminal_with_lease = tuple(
		sorted(
			str(node_id)
			for node_id in (control_plane.get("terminalWithActiveLeaseNodeIds") or [])
			if str(node_id).strip()
		)
	)
	if terminal_with_lease:
		violations.append(
			InvariantViolation(
				code="NODE_TERMINAL_WITH_ACTIVE_LEASE",
				message="Node terminal implies no active execution lease.",
				severity="error",
				node_ids=terminal_with_lease,
			)
		)

	completed_not_settled = tuple(
		sorted(
			str(node_id)
			for node_id in (control_plane.get("completedTerminalInputNotSettledNodeIds") or [])
			if str(node_id).strip()
		)
	)
	if completed_not_settled:
		violations.append(
			InvariantViolation(
				code="COMPLETED_TERMINAL_INPUT_NOT_SETTLED",
				message="Completed terminal requires required work inputs closed+drained.",
				severity="error",
				node_ids=completed_not_settled,
			)
		)

	return violations
