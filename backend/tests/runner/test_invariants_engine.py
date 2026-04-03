from app.runner.invariants import evaluate_runtime_invariants


def test_paused_run_with_active_nodes_violates() -> None:
	violations = evaluate_runtime_invariants(
		run_status="paused",
		node_status={"n1": "running", "n2": "succeeded_up_to_date"},
		run_telemetry={},
	)
	codes = {v.code for v in violations}
	assert "RUN_PAUSED_HAS_ACTIVE_NODES" in codes


def test_non_work_active_edge_violates() -> None:
	violations = evaluate_runtime_invariants(
		run_status="running",
		node_status={},
		run_telemetry={
			"activeEdges": [
				{"edgeId": "e_work", "plane": "work", "active": True},
				{"edgeId": "e_param", "plane": "param", "active": True},
			]
		},
	)
	codes = {v.code for v in violations}
	assert "NON_WORK_EDGE_MARKED_ACTIVE" in codes


def test_happy_path_has_no_violations() -> None:
	violations = evaluate_runtime_invariants(
		run_status="running",
		node_status={"n1": "running"},
		run_telemetry={"activeEdges": [{"edgeId": "e_work", "plane": "work", "active": True}]},
	)
	assert violations == []


def test_control_plane_monotonic_violation_detected() -> None:
	violations = evaluate_runtime_invariants(
		run_status="running",
		node_status={},
		run_telemetry={"controlPlane": {"monotonicViolation": True}},
	)
	codes = {v.code for v in violations}
	assert "CONTROL_SIGNAL_SEQ_NON_MONOTONIC" in codes


def test_control_plane_terminal_invariants_detected() -> None:
	violations = evaluate_runtime_invariants(
		run_status="running",
		node_status={},
		run_telemetry={
			"controlPlane": {
				"duplicateNodeTerminalIds": ["n1"],
				"terminalWithInflightNodeIds": ["n2"],
				"terminalWithActiveLeaseNodeIds": ["n3"],
				"completedTerminalInputNotSettledNodeIds": ["n4"],
			}
		},
	)
	codes = {v.code for v in violations}
	assert "NODE_TERMINAL_DUPLICATE" in codes
	assert "NODE_TERMINAL_WITH_INFLIGHT" in codes
	assert "NODE_TERMINAL_WITH_ACTIVE_LEASE" in codes
	assert "COMPLETED_TERMINAL_INPUT_NOT_SETTLED" in codes
