from app.runner.artifacts import RunBindings


def test_run_bindings_support_handle_specific_artifacts() -> None:
	bindings = RunBindings(run_id="run-1", graph_id="graph-1")
	bindings.bind(node_id="n_transform", handle="out", artifact_id="aid-pass", status="computed")
	bindings.bind(node_id="n_transform", handle="out_reject", artifact_id="aid-reject", status="computed")

	assert bindings.get_current_artifact("n_transform", "out") == "aid-pass"
	assert bindings.get_current_artifact("n_transform", "out_reject") == "aid-reject"
	assert bindings.get_current_artifact("n_transform", "missing") is None


def test_run_bindings_default_handle_is_out() -> None:
	bindings = RunBindings(run_id="run-2", graph_id="graph-2")
	bindings.bind(node_id="n_source", artifact_id="aid-source", status="computed")

	assert bindings.get_current_artifact("n_source") == "aid-source"
	assert bindings.get_current_artifact("n_source", "out") == "aid-source"


def test_run_bindings_out_lookup_falls_back_when_single_non_out_binding() -> None:
	bindings = RunBindings(run_id="run-3", graph_id="graph-3")
	bindings.bind(node_id="n_filter", handle="out_reject", artifact_id="aid-reject-only", status="computed")

	assert bindings.get_current_artifact("n_filter", "out_reject") == "aid-reject-only"
	assert bindings.get_current_artifact("n_filter", "out") == "aid-reject-only"
