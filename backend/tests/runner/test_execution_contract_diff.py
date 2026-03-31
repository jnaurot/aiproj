from __future__ import annotations

from app.runner.execution_contract import compare_execution_contracts


def _contract(*, graph_id: str, execution_version: str, env_hash: str, node_state: str, node_env: str, exec_key: str, artifact_id: str):
	return {
		"contractVersion": 1,
		"graphId": graph_id,
		"basis": {
			"schemaVersion": 1,
			"graphId": graph_id,
			"executionVersion": execution_version,
			"environmentHash": env_hash,
			"nodes": {
				"n1": {
					"nodeId": "n1",
					"nodeStateHash": node_state,
					"determinismEnvHash": node_env,
					"binding": {"currentExecKey": exec_key, "currentArtifactId": artifact_id},
					"upstreamBindings": {},
					"executionVersion": execution_version,
				}
			},
		},
	}


def test_diff_categorization_accuracy():
	expected = _contract(
		graph_id="g1",
		execution_version="v1",
		env_hash="env-1",
		node_state="state-1",
		node_env="node-env-1",
		exec_key="exec-1",
		artifact_id="art-1",
	)
	current = _contract(
		graph_id="g2",
		execution_version="v2",
		env_hash="env-2",
		node_state="state-2",
		node_env="node-env-2",
		exec_key="exec-2",
		artifact_id="art-2",
	)
	diff = compare_execution_contracts(expected_contract=expected, current_contract=current)
	assert diff["ok"] is False
	categories = set(diff.get("categories") or [])
	assert "graph" in categories
	assert "env" in categories
	assert "node_params" in categories
	assert "artifact_lineage" in categories
	assert "engine_version" in categories

