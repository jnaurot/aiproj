from __future__ import annotations

from app.runner.validator import GraphValidator


def test_mixed_plane_enforcement_reports_only_mode_specific_failures() -> None:
	graph = {
		"nodes": [
			{
				"id": "src",
				"data": {
					"kind": "transform",
					"label": "src",
					"params": {"op": "filter", "filter": {"expr": ""}},
					"schema": {
						"expectedSchema": {
							"source": "declared",
							"typedSchema": {"type": "json", "fields": []},
						}
					},
				},
			},
			{
				"id": "dst",
				"data": {
					"kind": "transform",
					"label": "dst",
					"params": {"op": "filter", "filter": {"expr": ""}},
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
				"data": {"mode": "work", "contract": {"payload": {"source": {"type": "json"}, "target": {"type": "text"}}}},
			},
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
							"source": {"type": "json", "keys": ["location"]},
							"target": {"type": "text", "requiredKeys": ["location", "salary"]},
						}
					},
				},
			},
			{
				"id": "e_control",
				"source": "src",
				"sourceHandle": "control_out",
				"target": "dst",
				"targetHandle": "control_in",
				"data": {"mode": "control", "contract": {"payload": {"source": {"type": "json"}, "target": {"type": "text"}}}},
			},
		],
	}

	result = GraphValidator().validate_pre_execution(graph)
	codes = [error.code for error in result.errors]
	assert "TYPE_MISMATCH" in codes
	assert "PARAM_CONTRACT_MISMATCH" in codes
	assert "EDGE_MODE_INCOMPATIBLE" not in codes
