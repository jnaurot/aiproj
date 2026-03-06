from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def _component_payload(label: str):
    return {
        "graph": {
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
        },
        "api": {
            "inputs": [
                {
                    "name": "in_data",
                    "portType": "table",
                    "required": True,
                    "typedSchema": {"type": "table", "fields": []},
                }
            ],
            "outputs": [
                {
                    "name": "out_data",
                    "portType": "json",
                    "required": True,
                    "typedSchema": {"type": "json", "fields": []},
                }
            ],
        },
        "configSchema": {},
    }


def test_component_routes_create_list_get():
    component_id = "cmp_route_test"
    with TestClient(app) as client:
        created = client.post(
            "/components",
            json={
                "componentId": component_id,
                "message": "seed",
                **_component_payload("v1"),
            },
        )
        assert created.status_code == 200, created.text
        body = created.json()
        assert body["componentId"] == component_id
        revision_id = body["revisionId"]

        listed_components = client.get("/components")
        assert listed_components.status_code == 200, listed_components.text
        components = listed_components.json()["components"]
        assert any(c["componentId"] == component_id for c in components)

        listed_revisions = client.get(f"/components/{component_id}/revisions")
        assert listed_revisions.status_code == 200, listed_revisions.text
        revisions = listed_revisions.json()["revisions"]
        assert len(revisions) >= 1
        assert revisions[0]["componentId"] == component_id

        fetched = client.get(f"/components/{component_id}/revisions/{revision_id}")
        assert fetched.status_code == 200, fetched.text
        detail = fetched.json()
        assert detail["componentId"] == component_id
        assert detail["revisionId"] == revision_id
        assert detail["definition"]["graph"]["nodes"][0]["data"]["label"] == "v1"


def test_component_routes_reject_invalid_payload():
    with TestClient(app) as client:
        res = client.post(
            "/components",
            json={
                "componentId": "cmp_invalid",
                "graph": {"version": 1, "nodes": []},
                "api": {"inputs": [], "outputs": []},
            },
        )
        assert res.status_code == 422, res.text

