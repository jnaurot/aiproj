from __future__ import annotations

from app.runner.validator import GraphValidator


def _graph_with_param_contract(*, source_keys: list[str], required_keys: list[str]) -> dict:
	return {
		"nodes": [
			{
				"id": "src",
				"data": {
					"kind": "tool",
					"label": "src",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
			{
				"id": "dst",
				"data": {
					"kind": "tool",
					"label": "dst",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
		],
		"edges": [
			{
				"id": "e_param",
				"source": "src",
				"sourceHandle": "out",
				"target": "dst",
				"targetHandle": "param_config",
				"data": {
					"mode": "param",
					"contract": {
						"payload": {
							"source": {"type": "json", "keys": source_keys},
							"target": {"type": "json", "requiredKeys": required_keys},
						}
					},
				},
			}
		],
	}


def test_param_contract_shape_validation_passes_when_required_keys_present() -> None:
	result = GraphValidator().validate_pre_execution(
		_graph_with_param_contract(source_keys=["location", "salary_min"], required_keys=["location"])
	)
	assert result.valid is True


def test_param_contract_shape_validation_fails_when_required_keys_missing() -> None:
	result = GraphValidator().validate_pre_execution(
		_graph_with_param_contract(source_keys=["location"], required_keys=["location", "salary_min"])
	)
	assert result.valid is False
	assert any(error.code == "PARAM_CONTRACT_MISMATCH" for error in result.errors)
