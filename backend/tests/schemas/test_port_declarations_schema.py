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


def test_normalize_port_declarations_respects_explicit_empty_direction_maps() -> None:
	decls = normalize_node_port_declarations(
		"transform",
		{
			"in": {},
			"out": {},
		},
	)
	assert decls["in"] == {}
	assert decls["out"] == {}


def test_graph_migration_preserves_explicit_empty_port_declarations() -> None:
	graph, _notes = canonicalize_graph_payload(
		{
			"nodes": [
				{
					"id": "n_transform",
					"data": {
						"kind": "transform",
						"params": {},
						"portDeclarations": {"in": {}, "out": {}},
					},
				}
			],
			"edges": [],
		}
	)
	node = graph["nodes"][0]
	data = node.get("data") or {}
	port_decls = data.get("portDeclarations") or {}
	assert port_decls.get("in") == {}
	assert port_decls.get("out") == {}


def test_normalize_port_declarations_migrates_legacy_config_plane_to_param() -> None:
	decls = normalize_node_port_declarations(
		"transform",
		{
			"in": {
				"config_in": {"plane": "config", "required": False, "cardinality": "many"},
			},
			"out": {},
		},
	)
	assert decls["in"]["config_in"]["plane"] == "param"
	assert decls["in"]["config_in"]["affinity"] == "param"


def test_graph_migration_migrates_legacy_config_plane_and_edge_mode() -> None:
	graph, notes = canonicalize_graph_payload(
		{
			"nodes": [
				{
					"id": "n_src",
					"data": {
						"kind": "transform",
						"params": {},
						"portDeclarations": {
							"in": {"config_in": {"plane": "config", "required": False, "cardinality": "many"}},
							"out": {"out": {"plane": "work", "cardinality": "many"}},
						},
					},
				},
				{"id": "n_dst", "data": {"kind": "transform", "params": {}}},
			],
			"edges": [
				{"id": "e_cfg", "source": "n_src", "target": "n_dst", "data": {"mode": "config"}},
			],
		}
	)
	node = graph["nodes"][0]
	data = node.get("data") or {}
	assert (((data.get("portDeclarations") or {}).get("in") or {}).get("config_in") or {}).get("plane") == "param"
	edge_mode = (((graph.get("edges") or [])[0] or {}).get("data") or {}).get("mode")
	assert edge_mode == "param"
	note_codes = {str(note.get("code") or "") for note in notes}
	assert "NODE_PORT_PLANE_CONFIG_MIGRATED" in note_codes
	assert "EDGE_MODE_CONFIG_MIGRATED" in note_codes

