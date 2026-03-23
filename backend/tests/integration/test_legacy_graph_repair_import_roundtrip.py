from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_import_roundtrip_repairs_malformed_handle_schemas_and_edge_snapshot() -> None:
	graph_id = "graph_legacy_repair_roundtrip"
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
				"type": "model",
				"position": {"x": 320, "y": 0},
				"data": {
					"kind": "model",
					"label": "Model",
					"params": {"model": "stub"},
					"schema": {
						"expectedInputSchemas": {
							"": {"typedSchema": {"type": "json"}},
							"in": {"typedSchema": {"type": "text", "fields": []}},
							"param_filters": {"typedSchema": "bad"},
						}
					},
				},
			},
		],
		"edges": [
			{
				"id": "e_legacy",
				"source": "n_src",
				"target": "n_dst",
				"data": {
					"contract": {
						"payload": {"source": {"type": "json"}, "target": {"type": "text"}},
						"snapshot": {"decision": "??"},
					}
				},
			}
		],
	}

	with TestClient(app) as client:
		created = client.post(
			"/graphs",
			json={"graphId": graph_id, "message": "legacy repair seed", "graph": graph},
		)
		assert created.status_code == 200, created.text

		latest = client.get(f"/graphs/{graph_id}/latest")
		assert latest.status_code == 200, latest.text
		body = latest.json()
		roundtrip = (body.get("graph") or {})
		nodes = roundtrip.get("nodes") or []
		dst = next((node for node in nodes if str(node.get("id") or "") == "n_dst"), None)
		assert dst is not None
		expected = ((((dst.get("data") or {}).get("schema") or {}).get("expectedInputSchemas") or {}))
		assert "" not in expected
		assert ((expected.get("param_filters") or {}).get("typedSchema") or {}).get("type") == "text"
		edge = ((roundtrip.get("edges") or [])[0] or {})
		snapshot = ((((edge.get("data") or {}).get("contract") or {}).get("snapshot") or {}))
		assert str(snapshot.get("sourceSchemaFingerprint") or "")
		assert str(snapshot.get("targetSchemaFingerprint") or "")
