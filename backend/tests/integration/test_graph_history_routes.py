from __future__ import annotations

from uuid import uuid4

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


def test_graph_history_routes_create_list_latest_get():
	graph_id = f"graph_history_{uuid4().hex[:8]}"
	with TestClient(app) as client:
		created_v1 = client.post(
			"/graphs",
			json={
				"graphId": graph_id,
				"message": "v1",
				"graph": _graph_payload("v1"),
			},
		)
		assert created_v1.status_code == 200, created_v1.text
		v1_id = created_v1.json()["revisionId"]

		created_v2 = client.post(
			"/graphs",
			json={
				"graphId": graph_id,
				"message": "v2",
				"graph": _graph_payload("v2"),
			},
		)
		assert created_v2.status_code == 200, created_v2.text
		v2_id = created_v2.json()["revisionId"]

		created_v3 = client.post(
			"/graphs",
			json={
				"graphId": graph_id,
				"message": "v3",
				"graph": _graph_payload("v3"),
			},
		)
		assert created_v3.status_code == 200, created_v3.text
		v3_id = created_v3.json()["revisionId"]

		latest = client.get(f"/graphs/{graph_id}/latest")
		assert latest.status_code == 200, latest.text
		latest_body = latest.json()
		assert latest_body["revisionId"] == v3_id
		assert latest_body["graph"]["nodes"][0]["data"]["label"] == "v3"

		listed = client.get(f"/graphs/{graph_id}/revisions", params={"limit": 10, "offset": 0})
		assert listed.status_code == 200, listed.text
		revisions = listed.json()["revisions"]
		assert [r["revisionId"] for r in revisions[:3]] == [v3_id, v2_id, v1_id]
		assert revisions[0]["parentRevisionId"] == v2_id
		assert revisions[1]["parentRevisionId"] == v1_id

		paged = client.get(f"/graphs/{graph_id}/revisions", params={"limit": 1, "offset": 1})
		assert paged.status_code == 200, paged.text
		assert len(paged.json()["revisions"]) == 1
		assert paged.json()["revisions"][0]["revisionId"] == v2_id

		detail_v2 = client.get(f"/graphs/{graph_id}/revisions/{v2_id}")
		assert detail_v2.status_code == 200, detail_v2.text
		assert detail_v2.json()["graph"]["nodes"][0]["data"]["label"] == "v2"


def test_graph_history_latest_missing_returns_404():
	with TestClient(app) as client:
		missing = client.get(f"/graphs/graph_missing_{uuid4().hex[:8]}/latest")
		assert missing.status_code == 404


def test_graph_names_unique_and_list_graphs():
	graph_name = f"Graph_{uuid4().hex[:8]}"
	with TestClient(app) as client:
		first = client.post(
			"/graphs",
			json={
				"graphName": graph_name,
				"revisionKind": "save_graph_as",
				"message": "create",
				"graph": _graph_payload("name-a"),
			},
		)
		assert first.status_code == 200, first.text
		first_graph_id = first.json()["graphId"]

		dup = client.post(
			"/graphs",
			json={
				"graphName": graph_name,
				"revisionKind": "save_graph_as",
				"message": "dup",
				"graph": _graph_payload("name-b"),
			},
		)
		assert dup.status_code == 400, dup.text
		assert "graph name already exists" in dup.text

		listed = client.get("/graphs", params={"limit": 100, "offset": 0})
		assert listed.status_code == 200, listed.text
		graphs = listed.json().get("graphs", [])
		match = [g for g in graphs if g.get("graphId") == first_graph_id]
		assert len(match) == 1
		assert match[0].get("graphName") == graph_name


def test_version_names_unique_within_graph():
	graph_id = f"graph_versions_{uuid4().hex[:8]}"
	with TestClient(app) as client:
		v1 = client.post(
			"/graphs",
			json={
				"graphId": graph_id,
				"versionName": "v-alpha",
				"revisionKind": "save_version",
				"graph": _graph_payload("a"),
			},
		)
		assert v1.status_code == 200, v1.text

		v2 = client.post(
			"/graphs",
			json={
				"graphId": graph_id,
				"versionName": "v-alpha",
				"revisionKind": "save_version",
				"graph": _graph_payload("b"),
			},
		)
		assert v2.status_code == 400, v2.text
		assert "version name already exists in graph" in v2.text


def test_delete_latest_revision_and_delete_graph():
	graph_id = f"graph_delete_{uuid4().hex[:8]}"
	with TestClient(app) as client:
		first = client.post(
			"/graphs",
			json={
				"graphId": graph_id,
				"message": "first",
				"graph": _graph_payload("first"),
			},
		)
		assert first.status_code == 200, first.text
		first_id = first.json()["revisionId"]

		second = client.post(
			"/graphs",
			json={
				"graphId": graph_id,
				"message": "second",
				"graph": _graph_payload("second"),
			},
		)
		assert second.status_code == 200, second.text
		second_id = second.json()["revisionId"]

		delete_latest = client.delete(f"/graphs/{graph_id}/revisions/{second_id}")
		assert delete_latest.status_code == 200, delete_latest.text
		assert delete_latest.json().get("deleted") is True
		assert delete_latest.json().get("graphDeleted") is False

		latest = client.get(f"/graphs/{graph_id}/latest")
		assert latest.status_code == 200, latest.text
		assert latest.json()["revisionId"] == first_id

		delete_graph = client.delete(f"/graphs/{graph_id}")
		assert delete_graph.status_code == 200, delete_graph.text
		assert delete_graph.json().get("deleted") is True

		missing = client.get(f"/graphs/{graph_id}/latest")
		assert missing.status_code == 404


def _legacy_component_graph_for_import() -> dict:
	return {
		"version": 1,
		"nodes": [
			{
				"id": "cmp1",
				"type": "component",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "component",
					"ports": {"in": None, "out": "json"},
					"params": {
						"componentRef": {
							"componentId": "cmp_a",
							"revisionId": "crev_1",
							"apiVersion": "v1",
						},
						"api": {
							"inputs": [],
							"outputs": [
								{"name": "summary", "portType": "text", "required": True, "typedSchema": {"type": "text", "fields": []}}
							],
						},
						"bindings": {
							"inputs": {},
							"config": {},
							"outputs": {"out_data": {"nodeId": "n_inner", "artifact": "current"}},
						},
						"config": {},
					},
				},
			},
			{
				"id": "llm1",
				"type": "llm",
				"position": {"x": 300, "y": 0},
				"data": {"kind": "llm", "ports": {"in": "text", "out": "text"}, "params": {"model": "x"}},
			},
		],
		"edges": [
			{
				"id": "e1",
				"source": "cmp1",
				"sourceHandle": "out",
				"target": "llm1",
				"targetHandle": "in",
				"data": {
					"contract": {
						"out": "json",
						"in": "text",
						"payload": {"source": {"type": "json"}, "target": {"type": "string"}},
					}
				},
			}
		],
	}


def test_import_accepts_legacy_v1_package_and_canonicalizes_graph():
	with TestClient(app) as client:
		legacy_graph = _legacy_component_graph_for_import()
		resp = client.post(
			"/graphs/import",
			json={
				"package": {
					"manifest": {
						"packageType": "aipgraph",
						"packageVersion": 1,
						"schemaVersion": 1,
					},
					"graph": legacy_graph,
				},
				"message": "legacy-v1-import",
			},
		)
		assert resp.status_code == 200, resp.text
		body = resp.json()
		report = body.get("migrationReport") or {}
		assert report.get("format") == "aipgraph_v1_legacy"
		assert bool(report.get("migrated")) is True
		stored = body.get("graph") or {}
		cmp_node = next(n for n in stored.get("nodes", []) if n.get("id") == "cmp1")
		bindings_outputs = (((cmp_node.get("data") or {}).get("params") or {}).get("bindings") or {}).get("outputs") or {}
		assert "summary" in bindings_outputs
		assert "out_data" not in bindings_outputs
		edge = next(e for e in stored.get("edges", []) if e.get("id") == "e1")
		assert str(edge.get("sourceHandle") or "") == "summary"


def test_import_accepts_raw_legacy_graph_without_manifest():
	with TestClient(app) as client:
		legacy_graph = _legacy_component_graph_for_import()
		resp = client.post(
			"/graphs/import",
			json={
				"package": legacy_graph,
				"message": "raw-legacy-import",
			},
		)
		assert resp.status_code == 200, resp.text
		body = resp.json()
		report = body.get("migrationReport") or {}
		assert report.get("format") == "raw_graph_legacy"
		assert bool(report.get("migrated")) is True
		stored = body.get("graph") or {}
		edge = next(e for e in stored.get("edges", []) if e.get("id") == "e1")
		assert str(edge.get("sourceHandle") or "") == "summary"
