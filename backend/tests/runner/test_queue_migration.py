from __future__ import annotations

from app.graph_migrations import canonicalize_graph_payload


def test_migration_defaults_edge_mode_and_queue_settings() -> None:
	graph = {
		"nodes": [
			{"id": "a", "data": {"kind": "source", "params": {"rel_path": ".", "filename": "x.txt", "file_format": "txt"}}},
			{"id": "b", "data": {"kind": "model", "params": {"model": "m", "user_prompt": "u"}}},
		],
		"edges": [{"id": "e1", "source": "a", "target": "b", "data": {"exec": "idle"}}],
	}
	canonical, _notes = canonicalize_graph_payload(graph)
	edge = (canonical.get("edges") or [])[0]
	assert edge["data"]["mode"] == "work"
	assert edge["data"]["fatal"] is False
	assert edge["data"]["queue"]["overflow"] == "block"
	assert int(edge["data"]["queue"]["max"]) >= 1
