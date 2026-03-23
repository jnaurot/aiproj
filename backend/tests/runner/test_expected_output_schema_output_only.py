from __future__ import annotations

from app.runner.validator import GraphValidator


def test_declared_input_type_ignores_expected_output_schema_channel() -> None:
	validator = GraphValidator()
	node = {
		"data": {
			"schema": {
				"expectedSchema": {"source": "declared", "typedSchema": {"type": "json", "fields": []}},
				"expectedInputSchemas": {
					"in": {"source": "declared", "typedSchema": {"type": "text", "fields": []}}
				},
			}
		}
	}

	assert validator._node_schema_declared_input_type(node, "in") == "text"


def test_edge_validation_uses_expected_input_schema_not_expected_output_schema() -> None:
	graph = {
		"nodes": [
			{
				"id": "src",
				"data": {
					"kind": "transform",
					"label": "src",
					"params": {"op": "filter", "filter": {"expr": ""}},
					"schema": {
						"expectedSchema": {"source": "declared", "typedSchema": {"type": "text", "fields": []}}
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
						# output-side declaration is intentionally different
						"expectedSchema": {"source": "declared", "typedSchema": {"type": "json", "fields": []}},
						# input-side declaration governs inbound compatibility
						"expectedInputSchemas": {
							"in": {"source": "declared", "typedSchema": {"type": "text", "fields": []}}
						},
					},
				},
			},
		],
		"edges": [
			{
				"id": "e_text",
				"source": "src",
				"sourceHandle": "out",
				"target": "dst",
				"targetHandle": "in",
				"data": {"mode": "work"},
			}
		],
	}

	result = GraphValidator().validate_pre_execution(graph)
	assert not any(err.code == "TYPE_MISMATCH" for err in result.errors)
