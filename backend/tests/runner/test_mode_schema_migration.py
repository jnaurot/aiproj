from __future__ import annotations

from app.graph_migrations import canonicalize_graph_payload


def _graph_with_edge_handles(source_handle: str, target_handle: str, mode: str | None = None) -> dict:
	edge_data: dict = {}
	if mode is not None:
		edge_data["mode"] = mode
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
				"sourceHandle": source_handle,
				"target": "dst",
				"targetHandle": target_handle,
				"data": edge_data,
			}
		],
	}


def test_migration_infers_param_mode_from_handles() -> None:
	graph, notes = canonicalize_graph_payload(_graph_with_edge_handles("out", "param_filters"))
	assert graph["edges"][0]["data"]["mode"] == "param"
	assert any(note.get("code") == "EDGE_MODE_INFERRED" for note in notes)


def test_migration_infers_control_mode_from_handles() -> None:
	graph, notes = canonicalize_graph_payload(_graph_with_edge_handles("control_out", "control_in"))
	assert graph["edges"][0]["data"]["mode"] == "control"
	assert any(note.get("code") == "EDGE_MODE_INFERRED" for note in notes)


def test_migration_invalid_mode_falls_back_to_handle_inference() -> None:
	graph, notes = canonicalize_graph_payload(
		_graph_with_edge_handles("control_out", "control_in", mode="legacy_weird")
	)
	assert graph["edges"][0]["data"]["mode"] == "control"
	assert any(note.get("code") == "EDGE_MODE_DEFAULTED" for note in notes)
