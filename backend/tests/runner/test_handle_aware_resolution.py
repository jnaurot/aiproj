from __future__ import annotations

from app.runner.validator import GraphValidator


def test_declared_input_type_resolves_by_target_handle() -> None:
	validator = GraphValidator()
	node = {
		"data": {
			"schema": {
				"expectedInputSchemas": {
					"in": {"typedSchema": {"type": "text", "fields": []}},
					"param_config": {"typedSchema": {"type": "json", "fields": []}},
				}
			}
		}
	}

	assert validator._node_schema_declared_input_type(node, "in") == "text"
	assert validator._node_schema_declared_input_type(node, "param_config") == "json"
	assert validator._node_schema_declared_input_type(node, "unknown_handle") == "text"
