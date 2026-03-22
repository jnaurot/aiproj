from __future__ import annotations

from app.runner.validator import GraphValidator


def _node(node_id: str, kind: str) -> dict:
	return {"id": node_id, "data": {"kind": kind, "label": node_id, "params": {}}}


def test_param_edge_validates_required_keys_not_payload_type() -> None:
	graph = {
		"nodes": [_node("src", "source"), _node("dst", "model")],
		"edges": [
			{
				"id": "e_param_ok",
				"source": "src",
				"sourceHandle": "out",
				"target": "dst",
				"targetHandle": "param_filters",
				"data": {
					"mode": "param",
					"contract": {
						"payload": {
							"source": {"type": "text", "keys": ["location", "salary_min"]},
							"target": {"type": "json", "requiredKeys": ["location"]},
						}
					},
				},
			}
		],
	}
	result = GraphValidator().validate_pre_execution(graph)
	assert not any(err.code == "PARAM_CONTRACT_MISMATCH" for err in result.errors)
	assert not any(err.code == "TYPE_MISMATCH" for err in result.errors)


def test_param_edge_reports_missing_required_keys() -> None:
	graph = {
		"nodes": [_node("src", "source"), _node("dst", "model")],
		"edges": [
			{
				"id": "e_param_missing",
				"source": "src",
				"sourceHandle": "out",
				"target": "dst",
				"targetHandle": "param_filters",
				"data": {
					"mode": "param",
					"contract": {
						"payload": {
							"source": {"type": "json", "keys": ["location"]},
							"target": {"type": "json", "requiredKeys": ["location", "salary_min"]},
						}
					},
				},
			}
		],
	}
	result = GraphValidator().validate_pre_execution(graph)
	assert any(err.code == "PARAM_CONTRACT_MISMATCH" for err in result.errors)
	assert not any(err.code == "TYPE_MISMATCH" for err in result.errors)
