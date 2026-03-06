from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def _graph_payload(label: str):
	return {
		"version": 1,
		"nodes": [
			{
				"id": "n1",
				"type": "source",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "source",
					"label": label,
					"sourceKind": "file",
					"status": "idle",
					"ports": {"in": None, "out": "table"},
					"params": {"source_type": "text", "text": "x", "output_mode": "rows"},
				},
			}
		],
		"edges": [],
	}


def test_graph_read_path_always_enabled(monkeypatch):
	graph_id = "graph_phase3_read_switch"

	# seed revision directly in store
	with TestClient(app) as client:
		client.app.state.graph_revisions.create_revision(
			graph_id=graph_id,
			graph=_graph_payload("seeded"),
			message="seed",
		)

		monkeypatch.setenv("GRAPH_STORE_V2_READ", "0")
		on = client.get(f"/graphs/{graph_id}/latest")
		assert on.status_code == 200, on.text
		body = on.json()
		assert body["graphId"] == graph_id
		assert body["graph"]["nodes"][0]["data"]["label"] == "seeded"


def test_graph_feature_flags_runtime_update():
	with TestClient(app) as client:
		before = client.get("/graphs/feature-flags")
		assert before.status_code == 200, before.text
		before_body = before.json()
		assert before_body["flags"]["GRAPH_STORE_V2_READ"] is True
		assert before_body["flags"]["GRAPH_STORE_V2_WRITE"] is True
		assert before_body["flags"]["GRAPH_EXPORT_V2"] is True

		res = client.put(
			"/graphs/feature-flags",
			json={
				"GRAPH_STORE_V2_READ": True,
				"GRAPH_STORE_V2_WRITE": True,
				"GRAPH_EXPORT_V2": False,
			},
		)
		assert res.status_code == 410, res.text


def test_graph_export_import_package_v2():
	graph_id = "graph_phase5_pkg"
	with TestClient(app) as client:
		created = client.post(
			"/graphs",
			json={
				"graphId": graph_id,
				"message": "seed",
				"graph": _graph_payload("phase5"),
			},
		)
		assert created.status_code == 200, created.text

		exported = client.get(f"/graphs/{graph_id}/export")
		assert exported.status_code == 200, exported.text
		pkg = exported.json()["package"]
		assert pkg["manifest"]["packageType"] == "aipgraph"
		assert int(pkg["manifest"]["packageVersion"]) == 2

		imported = client.post(
			"/graphs/import",
			json={
				"package": pkg,
				"targetGraphId": f"{graph_id}_imported",
				"message": "imported",
			},
		)
		assert imported.status_code == 200, imported.text
		body = imported.json()
		assert body["graphId"] == f"{graph_id}_imported"
		assert body["migrationReport"]["format"] == "aipgraph_v2"

		legacy = client.post(
			"/graphs/import",
			json={
				"package": {
					"version": 1,
					"nodes": [],
					"edges": [],
				},
				"targetGraphId": f"{graph_id}_legacy",
			},
		)
		assert legacy.status_code == 400
