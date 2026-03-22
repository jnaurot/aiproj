from __future__ import annotations

from app.runner.validator import GraphValidator


def _node(node_id: str, kind: str) -> dict:
	return {"id": node_id, "data": {"kind": kind, "label": node_id, "params": {}}}


def test_work_payload_mismatch_message_is_mode_specific() -> None:
	graph = {
		"nodes": [_node("src", "source"), _node("dst", "tool")],
		"edges": [
			{
				"id": "e_work",
				"source": "src",
				"sourceHandle": "out",
				"target": "dst",
				"targetHandle": "in",
				"data": {"mode": "work", "contract": {"payload": {"source": {"type": "text"}, "target": {"type": "json"}}}},
			}
		],
	}
	result = GraphValidator().validate_pre_execution(graph)
	message = " | ".join(error.message for error in result.errors)
	assert "Work payload mismatch" in message


def test_param_shape_mismatch_message_is_mode_specific() -> None:
	graph = {
		"nodes": [_node("src", "source"), _node("dst", "model")],
		"edges": [
			{
				"id": "e_param",
				"source": "src",
				"sourceHandle": "out",
				"target": "dst",
				"targetHandle": "param_filters",
				"data": {
					"mode": "param",
					"contract": {
						"payload": {
							"source": {"keys": ["location"]},
							"target": {"requiredKeys": ["location", "salary_min"]},
						}
					},
				},
			}
		],
	}
	result = GraphValidator().validate_pre_execution(graph)
	message = " | ".join(error.message for error in result.errors)
	assert "Param shape mismatch" in message


def test_control_contract_mismatch_message_is_mode_specific() -> None:
	graph = {
		"nodes": [_node("src", "source"), _node("dst", "model")],
		"edges": [
			{
				"id": "e_control",
				"source": "src",
				"sourceHandle": "out",
				"target": "dst",
				"targetHandle": "in",
				"data": {"mode": "control"},
			}
		],
	}
	result = GraphValidator().validate_pre_execution(graph)
	message = " | ".join(error.message for error in result.errors)
	assert "Control contract mismatch" in message
