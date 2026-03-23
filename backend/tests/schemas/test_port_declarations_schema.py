from __future__ import annotations

from app.graph_migrations import canonicalize_graph_payload
from app.runner.capabilities import normalize_node_port_declarations


def test_normalize_port_declarations_has_required_shape() -> None:
	decls = normalize_node_port_declarations(
		"model",
		{
			"in": {
				"in": {"plane": "work", "required": True, "cardinality": "one", "behavior": "single_item"},
				"param_context": {"plane": "param", "behavior": "once"},
			},
			"out": {"out": {"plane": "work", "cardinality": "many"}},
		},
	)
	assert set(decls.keys()) == {"in", "out"}
	assert decls["in"]["in"]["plane"] == "work"
	assert decls["in"]["in"]["required"] is True
	assert decls["in"]["in"]["cardinality"] == "one"
	assert decls["in"]["in"]["behavior"] == "single_item"
	assert decls["in"]["param_context"]["plane"] == "param"
	assert decls["out"]["out"]["plane"] == "work"


def test_graph_migration_canonicalizes_port_declarations_and_port_contracts() -> None:
	graph, notes = canonicalize_graph_payload(
		{
			"nodes": [
				{
					"id": "n_model",
					"data": {
						"kind": "model",
						"params": {},
						"portDeclarations": {
							"in": {"in": {"plane": "work", "required": True, "cardinality": "one"}},
							"out": {"out": {"plane": "work", "cardinality": "many"}},
						},
					},
				}
			],
			"edges": [],
		}
	)
	node = graph["nodes"][0]
	data = node.get("data") or {}
	port_decls = data.get("portDeclarations") or {}
	port_contracts = data.get("portContracts") or {}
	assert port_decls["in"]["in"]["required"] is True
	assert port_decls["in"]["in"]["cardinality"] == "one"
	assert port_contracts["in"]["in"]["affinity"] == "work"
	assert any(str(note.get("code") or "") == "NODE_PORT_DECLARATIONS_CANONICALIZED" for note in notes)

