from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_graph_revision_roundtrip_preserves_migrated_processing_policy_shape() -> None:
	graph_id = "graph_port_schema_policy_compat"
	graph = {
		"version": 1,
		"nodes": [
			{
				"id": "n1",
				"type": "transform",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "transform",
					"label": "Transform",
					"status": "idle",
					"params": {"op": "filter", "filter": {"expr": ""}},
					"processingPolicy": {
						"consumeMode": "read_once",
						"batchSize": 2,
						"maxInflight": 3,
						"input_handles": {
							"in": {"consumeMode": "continuous", "batchSize": 4, "maxInflight": 5}
						},
					},
				},
			}
		],
		"edges": [],
	}

	with TestClient(app) as client:
		created = client.post(
			"/graphs",
			json={"graphId": graph_id, "message": "port-schema policy seed", "graph": graph},
		)
		assert created.status_code == 200, created.text

		latest = client.get(f"/graphs/{graph_id}/latest")
		assert latest.status_code == 200, latest.text
		body = latest.json()
		node = ((body.get("graph") or {}).get("nodes") or [])[0]
		policy = ((node.get("data") or {}).get("processingPolicy") or {})
		assert policy.get("consume_mode") == "once"
		assert policy.get("batch_size") == 2
		assert policy.get("max_inflight") == 3
		handle_policy = (policy.get("input_handles") or {}).get("in") or {}
		assert handle_policy.get("consume_mode") == "single_item"
		assert handle_policy.get("batch_size") == 4
		assert handle_policy.get("max_inflight") == 5
