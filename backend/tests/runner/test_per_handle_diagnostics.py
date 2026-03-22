from __future__ import annotations

from app.runner.validator import GraphValidator


def test_validator_emits_per_handle_schema_diagnostics_context():
	validator = GraphValidator()
	graph = {
		"nodes": [
			{
				"id": "n_source",
				"data": {
					"kind": "source",
					"label": "Source",
					"sourceKind": "file",
					"params": {"file_format": "txt"},
					"schema": {
						"expectedSchema": {
							"typedSchema": {"type": "text", "fields": []},
							"source": "declared",
							"state": "fresh",
						}
					},
				},
			},
			{
				"id": "n_transform",
				"data": {
					"kind": "transform",
					"label": "Transform",
					"transformKind": "json_to_table",
					"params": {"op": "json_to_table"},
					"schema": {
						"expectedInputSchemas": {
							"in": {
								"typedSchema": {"type": "json", "fields": []},
								"source": "declared",
								"state": "fresh",
							}
						}
					},
				},
			},
		],
		"edges": [
			{
				"id": "e_handle_diag",
				"source": "n_source",
				"sourceHandle": "out",
				"target": "n_transform",
				"targetHandle": "in",
				"data": {"mode": "work"},
			}
		],
	}
	result = validator.validate_pre_execution(graph)
	assert result.valid is False
	err = next((item for item in result.errors if item.edge_id == "e_handle_diag"), None)
	assert err is not None
	assert err.code == "TYPE_MISMATCH"
	assert isinstance(err.details, dict)
	assert err.details.get("targetHandle") == "in"
	assert err.details.get("sourceHandle") == "out"
	assert err.details.get("sourceNodeId") == "n_source"
	assert err.details.get("targetNodeId") == "n_transform"
	assert err.details.get("sourceLabel") == "Source"
	assert err.details.get("targetLabel") == "Transform"
	assert err.details.get("mode") == "work"

