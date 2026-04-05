from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_graph_migration_report_dry_run_returns_plane_migration_metadata() -> None:
	graph = {
		"version": 1,
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

	with TestClient(app) as client:
		res = client.post("/graphs/migration-report", json={"graph": graph})
		assert res.status_code == 200, res.text
		body = res.json()
		assert body.get("format") == "canonical_graph_v1"
		assert bool(body.get("migrated")) is True
		summary = body.get("summary") or {}
		assert int(summary.get("planeMigrations") or 0) >= 2
		notes = body.get("notes") or []
		node_note = next(
			(note for note in notes if str((note or {}).get("code") or "") == "NODE_PORT_PLANE_CONFIG_MIGRATED"),
			{},
		)
		assert str(node_note.get("entityId") or "") == "n_src"
		assert str(node_note.get("oldPlane") or "") == "config"
		assert str(node_note.get("newPlane") or "") == "param"
		assert bool(node_note.get("autoFixApplied")) is True
		assert "graph" not in body


def test_graph_migration_report_can_include_canonical_graph() -> None:
	graph = {
		"version": 1,
		"nodes": [
			{
				"id": "n_src",
				"data": {"kind": "source", "params": {"sourceKind": "api"}},
			},
			{
				"id": "n_dst",
				"data": {"kind": "model", "params": {"model": "stub"}},
			},
		],
		"edges": [
			{"id": "e_1", "source": "n_src", "target": "n_dst", "sourceHandle": "param_cfg", "targetHandle": "in"},
		],
	}

	with TestClient(app) as client:
		res = client.post("/graphs/migration-report", json={"graph": graph, "includeCanonicalGraph": True})
		assert res.status_code == 200, res.text
		body = res.json()
		canonical_graph = body.get("graph") or {}
		edges = canonical_graph.get("edges") or []
		assert len(edges) == 1
		mode = (((edges[0] or {}).get("data") or {}).get("mode"))
		assert str(mode) in {"work", "param", "control"}

