from __future__ import annotations

from app.runner.validator import GraphValidator


def _node(node_id: str, kind: str) -> dict:
	if kind == "transform":
		return {
			"id": node_id,
			"data": {
				"kind": "transform",
				"label": node_id,
				"params": {"op": "filter", "filter": {"expr": ""}},
			},
		}
	return {"id": node_id, "data": {"kind": kind, "label": node_id, "params": {}}}


def test_work_mode_enforces_payload_type_rules() -> None:
	graph = {
		"nodes": [
			{
				**_node("src", "transform"),
				"data": {
					**_node("src", "transform")["data"],
					"schema": {"expectedSchema": {"source": "declared", "typedSchema": {"type": "json", "fields": []}}},
				},
			},
			{
				**_node("dst", "transform"),
				"data": {
					**_node("dst", "transform")["data"],
					"schema": {
						"expectedInputSchemas": {
							"in": {"source": "declared", "typedSchema": {"type": "text", "fields": []}}
						}
					},
				},
			},
		],
		"edges": [
			{
				"id": "e_work",
				"source": "src",
				"sourceHandle": "out",
				"target": "dst",
				"targetHandle": "in",
				"data": {
					"mode": "work",
					"contract": {"payload": {"source": {"type": "json"}, "target": {"type": "text"}}},
				},
			}
		],
	}

	result = GraphValidator().validate_pre_execution(graph)
	assert any(error.code == "TYPE_MISMATCH" for error in result.errors)


def test_param_mode_enforces_shape_keys_instead_of_payload_type() -> None:
	graph = {
		"nodes": [_node("src", "transform"), _node("dst", "transform")],
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
							"source": {"type": "text", "keys": ["location"]},
							"target": {"type": "json", "requiredKeys": ["location", "salary"]},
						}
					},
				},
			}
		],
	}

	result = GraphValidator().validate_pre_execution(graph)
	assert any(error.code == "PARAM_CONTRACT_MISMATCH" for error in result.errors)
	assert not any(error.code == "TYPE_MISMATCH" for error in result.errors)


def test_control_mode_checks_affinity_and_skips_payload_type() -> None:
	graph = {
		"nodes": [_node("src", "transform"), _node("dst", "transform")],
		"edges": [
			{
				"id": "e_control",
				"source": "src",
				"sourceHandle": "control_out",
				"target": "dst",
				"targetHandle": "control_in",
				"data": {
					"mode": "control",
					"contract": {"payload": {"source": {"type": "json"}, "target": {"type": "text"}}},
				},
			}
		],
	}

	result = GraphValidator().validate_pre_execution(graph)
	assert result.errors == []
