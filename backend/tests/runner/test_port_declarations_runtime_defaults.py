from __future__ import annotations

from app.runner.validator import GraphValidator


def test_port_declarations_override_affinity_resolution() -> None:
	validator = GraphValidator()
	node = {
		"id": "n_target",
		"data": {
			"kind": "tool",
			"portDeclarations": {
				"in": {
					"in": {"plane": "work", "required": False, "cardinality": "many"},
					"param_filters": {"plane": "param", "required": False, "cardinality": "many"},
					"control_in": {"plane": "control", "required": False, "cardinality": "many"},
				}
			},
		},
	}
	assert validator._port_affinity(node, direction="in", handle="in") == "work"
	assert validator._port_affinity(node, direction="in", handle="param_filters") == "param"
	assert validator._port_affinity(node, direction="in", handle="control_in") == "control"


def test_port_declaration_required_and_cardinality_constraints() -> None:
	validator = GraphValidator()
	graph = {
		"nodes": [
			{
				"id": "src_a",
				"data": {"kind": "source", "params": {}},
			},
			{
				"id": "src_b",
				"data": {"kind": "source", "params": {}},
			},
			{
				"id": "target",
				"data": {
					"kind": "tool",
					"params": {},
					"portDeclarations": {
						"in": {
							"in": {"plane": "work", "required": True, "cardinality": "one"},
							"param_config": {"plane": "param", "required": False, "cardinality": "many"},
						},
						"out": {"out": {"plane": "work", "required": False, "cardinality": "many"}},
					},
				},
			},
		],
		"edges": [
			{"id": "e1", "source": "src_a", "target": "target", "targetHandle": "in", "data": {"mode": "work"}},
			{"id": "e2", "source": "src_b", "target": "target", "targetHandle": "in", "data": {"mode": "work"}},
		],
	}
	errors = validator._validate_port_declaration_constraints(graph)
	assert any(err.code == "PORT_CARDINALITY_EXCEEDED" and err.node_id == "target" for err in errors)

