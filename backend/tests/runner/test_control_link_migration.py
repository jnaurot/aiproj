from __future__ import annotations

from app.graph_migrations import canonicalize_graph_payload


def _base_graph(edge_data: dict) -> dict:
	return {
		"version": 1,
		"nodes": [
			{"id": "src", "data": {"kind": "source", "label": "src", "params": {}}},
			{"id": "dst", "data": {"kind": "model", "label": "dst", "params": {}}},
		],
		"edges": [
			{
				"id": "e1",
				"source": "src",
				"sourceHandle": "out",
				"target": "dst",
				"targetHandle": "in",
				"data": edge_data,
			}
		],
	}


def test_control_link_kind_defaults_to_data_link() -> None:
	graph, _notes = canonicalize_graph_payload(_base_graph({"mode": "work"}))
	edge_data = graph["edges"][0]["data"]
	assert edge_data["linkKind"] == "data_link"
	assert edge_data["mode"] == "work"


def test_control_link_kind_forces_control_mode() -> None:
	graph, notes = canonicalize_graph_payload(_base_graph({"linkKind": "control_link", "mode": "work"}))
	edge_data = graph["edges"][0]["data"]
	assert edge_data["linkKind"] == "control_link"
	assert edge_data["mode"] == "control"
	assert any(note.get("code") == "EDGE_LINK_KIND_MODE_NORMALIZED" for note in notes)
