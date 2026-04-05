from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from app.main import app


def _contains_legacy_config_plane(value: Any) -> bool:
	if isinstance(value, dict):
		for key, child in value.items():
			key_norm = str(key or "").strip().lower()
			if key_norm in {"plane", "affinity", "mode"} and str(child or "").strip().lower() == "config":
				return True
			if _contains_legacy_config_plane(child):
				return True
		return False
	if isinstance(value, list):
		return any(_contains_legacy_config_plane(item) for item in value)
	return False


def test_graph_export_never_emits_legacy_config_plane() -> None:
	graph_id = "graph_export_canonical_planes"
	legacy_graph = {
		"version": 1,
		"nodes": [
			{
				"id": "n_src",
				"type": "transform",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "transform",
					"params": {},
					"portDeclarations": {
						"in": {"config_in": {"plane": "config", "required": False, "cardinality": "many"}},
						"out": {"out": {"plane": "work", "cardinality": "many"}},
					},
				},
			},
			{
				"id": "n_dst",
				"type": "model",
				"position": {"x": 240, "y": 0},
				"data": {"kind": "model", "params": {"model": "stub"}},
			},
		],
		"edges": [
			{"id": "e_cfg", "source": "n_src", "target": "n_dst", "data": {"mode": "config"}},
		],
	}

	with TestClient(app) as client:
		create = client.post("/graphs", json={"graphId": graph_id, "graph": legacy_graph})
		assert create.status_code == 200, create.text
		exported = client.get(f"/graphs/{graph_id}/export")
		assert exported.status_code == 200, exported.text
		body = exported.json()
		assert _contains_legacy_config_plane(body) is False


def test_graph_import_canonicalizes_legacy_config_plane_before_persist() -> None:
	legacy_package = {
		"manifest": {"packageType": "aipgraph", "packageVersion": 1},
		"graph": {
			"version": 1,
			"nodes": [
				{
					"id": "n_src",
					"type": "transform",
					"position": {"x": 0, "y": 0},
					"data": {
						"kind": "transform",
						"params": {},
						"portDeclarations": {
							"in": {"config_in": {"plane": "config", "required": False, "cardinality": "many"}},
							"out": {"out": {"plane": "work", "cardinality": "many"}},
						},
					},
				},
				{
					"id": "n_dst",
					"type": "model",
					"position": {"x": 240, "y": 0},
					"data": {"kind": "model", "params": {"model": "stub"}},
				},
			],
			"edges": [
				{"id": "e_cfg", "source": "n_src", "target": "n_dst", "data": {"mode": "config"}},
			],
		},
	}

	with TestClient(app) as client:
		imported = client.post("/graphs/import", json={"package": legacy_package})
		assert imported.status_code == 200, imported.text
		body = imported.json()
		graph = body.get("graph") or {}
		assert _contains_legacy_config_plane(graph) is False
		report = body.get("migrationReport") or {}
		notes = report.get("notes") or []
		assert any(str((note or {}).get("code") or "") == "EDGE_MODE_CONFIG_MIGRATED" for note in notes)
		assert any(str((note or {}).get("code") or "") == "NODE_PORT_PLANE_CONFIG_MIGRATED" for note in notes)

