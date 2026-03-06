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


def test_graph_read_switch_flag(monkeypatch):
	graph_id = "graph_phase3_read_switch"

	# seed revision directly in store
	with TestClient(app) as client:
		client.app.state.graph_revisions.create_revision(
			graph_id=graph_id,
			graph=_graph_payload("seeded"),
			message="seed",
		)

		monkeypatch.setenv("GRAPH_STORE_V2_READ", "0")
		off = client.get(f"/graphs/{graph_id}/latest")
		assert off.status_code == 503

		monkeypatch.setenv("GRAPH_STORE_V2_READ", "1")
		on = client.get(f"/graphs/{graph_id}/latest")
		assert on.status_code == 200, on.text
		body = on.json()
		assert body["graphId"] == graph_id
		assert body["graph"]["nodes"][0]["data"]["label"] == "seeded"


def test_graph_feature_flags_runtime_update():
	with TestClient(app) as client:
		before = client.get("/graphs/feature-flags")
		assert before.status_code == 200, before.text

		res = client.put(
			"/graphs/feature-flags",
			json={
				"GRAPH_STORE_V2_READ": True,
				"GRAPH_STORE_V2_WRITE": True,
				"GRAPH_EXPORT_V2": False,
			},
		)
		assert res.status_code == 200, res.text
		body = res.json()
		assert body["flags"]["GRAPH_STORE_V2_READ"] is True
		assert body["flags"]["GRAPH_STORE_V2_WRITE"] is True
		assert body["flags"]["GRAPH_EXPORT_V2"] is False
