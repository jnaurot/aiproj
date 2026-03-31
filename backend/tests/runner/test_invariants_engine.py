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
