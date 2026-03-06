from app.runner.node_state import build_exec_key


def test_component_context_changes_exec_key():
	base = dict(
		graph_id="graph_component_exec",
		node_id="cmp:n_component:inner",
		node_kind="tool",
		node_state_hash="state_hash_a",
		upstream_artifact_ids=["artifact_a"],
		input_refs=[("in", "artifact_a")],
		execution_version="2",
		node_impl_version="TOOL@1",
	)

	key_v1 = build_exec_key(
		**base,
		determinism_env={
			"component_instance": {
				"component_id": "cmp_echo",
				"component_revision_id": "rev_1",
				"instance_node_id": "component_node",
			}
		},
	)
	key_v2 = build_exec_key(
		**base,
		determinism_env={
			"component_instance": {
				"component_id": "cmp_echo",
				"component_revision_id": "rev_2",
				"instance_node_id": "component_node",
			}
		},
	)

	assert key_v1 != key_v2
