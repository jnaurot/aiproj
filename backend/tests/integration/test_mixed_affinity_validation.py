from __future__ import annotations

from app.runner.validator import GraphValidator


def test_pre_execution_validation_applies_affinity_specific_rules() -> None:
	graph = {
		"nodes": [
			{
				"id": "src_json",
				"data": {
					"kind": "source",
					"params": {"file_format": "json"},
					"schema": {"expectedSchema": {"typedSchema": {"type": "json", "fields": []}}},
				},
			},
			{
				"id": "dst",
				"data": {
					"kind": "transform",
					"params": {"op": "select", "select": {"mode": "include", "columns": []}},
					"schema": {
						"expectedInputSchemas": {
							"in": {"typedSchema": {"type": "text", "fields": []}},
							"param_config": {"typedSchema": {"type": "text", "fields": []}},
							"control_in": {"typedSchema": {"type": "text", "fields": []}},
						}
					},
				},
			},
		],
		"edges": [
			{"id": "e_work", "source": "src_json", "target": "dst", "targetHandle": "in", "data": {"mode": "work"}},
			{
				"id": "e_param",
				"source": "src_json",
				"target": "dst",
				"targetHandle": "param_config",
				"data": {"mode": "param"},
			},
			{
				"id": "e_control",
				"source": "src_json",
				"target": "dst",
				"targetHandle": "control_in",
				"data": {"mode": "control"},
			},
		],
	}
	result = GraphValidator().validate_pre_execution(graph)
	error_by_edge = {e.edge_id: e for e in result.errors if e.edge_id}
	assert "e_work" in error_by_edge
	assert error_by_edge["e_work"].code in {
		"TYPE_MISMATCH",
		"CONTRACT_EDGE_TYPE_MISMATCH",
		"CONTRACT_EDGE_PAYLOAD_TYPE_MISMATCH",
	}
	assert "e_param" not in error_by_edge
	assert "e_control" in error_by_edge
	assert error_by_edge["e_control"].code == "EDGE_MODE_INCOMPATIBLE"
