from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_legacy_config_plane_graph_import_migrates_to_param_with_warning() -> None:
	graph_id = "graph_legacy_config_plane_e2e"
	graph = {
		"version": 1,
		"nodes": [
			{
				"id": "n_src",
				"type": "source",
				"position": {"x": 0, "y": 0},
				"data": {"kind": "source", "label": "Source", "params": {"sourceKind": "api"}},
			},
			{
				"id": "n_dst",
				"type": "transform",
				"position": {"x": 280, "y": 0},
				"data": {
					"kind": "transform",
					"label": "Transform",
					"params": {"op": "derive", "derive": {"mode": "rules", "rules": []}},
					"portDeclarations": {
						"in": {
							"config_in": {"plane": "config", "required": False, "cardinality": "many"}
						}
					},
				},
			},
		],
		"edges": [
			{"id": "e_cfg", "source": "n_src", "target": "n_dst", "data": {"mode": "config"}},
		],
	}

	with TestClient(app) as client:
		created = client.post(
			"/graphs",
			json={"graphId": graph_id, "message": "legacy config-plane import", "graph": graph},
		)
		assert created.status_code == 200, created.text
		notes = created.json().get("migrationNotes") or []
		codes = {str(note.get("code") or "") for note in notes if isinstance(note, dict)}
		assert "NODE_PORT_PLANE_CONFIG_MIGRATED" in codes
		assert "EDGE_MODE_CONFIG_MIGRATED" in codes

		latest = client.get(f"/graphs/{graph_id}/latest")
		assert latest.status_code == 200, latest.text
		roundtrip = latest.json().get("graph") or {}
		nodes = roundtrip.get("nodes") or []
		dst = next((n for n in nodes if str(n.get("id") or "") == "n_dst"), None)
		assert dst is not None
		config_in_plane = (
			((((dst.get("data") or {}).get("portDeclarations") or {}).get("in") or {}).get("config_in") or {})
		).get("plane")
		assert config_in_plane == "param"
		edge = ((roundtrip.get("edges") or [])[0] or {})
		assert str(((edge.get("data") or {}).get("mode") or "")) == "param"
