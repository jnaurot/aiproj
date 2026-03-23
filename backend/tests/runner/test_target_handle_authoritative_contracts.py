from __future__ import annotations

from app.runner.validator import GraphValidator


def test_target_handle_contract_is_authoritative_for_input_type() -> None:
	validator = GraphValidator()
	node = {
		"data": {
			"schema": {
				"expectedInputSchemas": {
					"in": {"typedSchema": {"type": "text", "fields": []}},
					"param_profile": {"typedSchema": {"type": "json", "fields": []}},
					"control_gate": {"typedSchema": {"type": "none", "fields": []}},
				}
			}
		}
	}

	assert validator._node_schema_declared_input_type(node, "in") == "text"
	assert validator._node_schema_declared_input_type(node, "param_profile") == "json"
	assert validator._node_schema_declared_input_type(node, "control_gate") == "none"


def test_target_handle_falls_back_to_in_handle_when_missing() -> None:
	validator = GraphValidator()
	node = {
		"data": {
			"schema": {
				"expectedInputSchemas": {
					"in": {"typedSchema": {"type": "json", "fields": []}},
				}
			}
		}
	}

	assert validator._node_schema_declared_input_type(node, "param_missing") == "json"
