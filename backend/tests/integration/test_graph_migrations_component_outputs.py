from __future__ import annotations

from uuid import uuid4

from fastapi.testclient import TestClient

from app.graph_migrations import canonicalize_graph_payload
from app.main import app


def _legacy_component_graph() -> dict:
	return {
		"version": 1,
		"nodes": [
			{
				"id": "cmp1",
				"type": "component",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "component",
					"label": "Component",
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
								{
									"name": "summary",
									"portType": "text",
									"required": True,
									"typedSchema": {"type": "text", "fields": []},
								}
							],
						},
						"bindings": {
							"inputs": {},
							"config": {},
							"outputs": {
								"out_data": {"nodeId": "n_inner", "artifact": "current"},
							},
						},
						"config": {},
					},
				},
			},
			{
				"id": "llm1",
				"type": "llm",
				"position": {"x": 400, "y": 0},
				"data": {
					"kind": "llm",
					"label": "LLM",
					"ports": {"in": "text", "out": "text"},
					"params": {
						"baseUrl": "http://localhost:11434",
						"model": "x",
						"user_prompt": "hi",
						"system_prompt": "",
					},
				},
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
						"payload": {
							"source": {"type": "json"},
							"target": {"type": "string"},
						},
					}
				},
			}
		],
	}


def test_canonicalize_graph_payload_fixes_legacy_component_handles_bindings_and_contracts():
	graph, notes = canonicalize_graph_payload(_legacy_component_graph())
	assert isinstance(notes, list)
	cmp_node = next(n for n in graph["nodes"] if n["id"] == "cmp1")
	bindings_outputs = (((cmp_node.get("data") or {}).get("params") or {}).get("bindings") or {}).get("outputs") or {}
	assert "summary" in bindings_outputs
	assert "out_data" not in bindings_outputs
	edge = graph["edges"][0]
	assert str(edge.get("sourceHandle")) == "summary"
	contract = ((edge.get("data") or {}).get("contract") or {})
	assert str(contract.get("out")) == "text"
	assert str((((contract.get("payload") or {}).get("source") or {}).get("type") or "")) == "string"


def test_graph_create_revision_applies_component_migration_normalization():
	graph_id = f"graph_migrate_cmp_{uuid4().hex[:8]}"
	with TestClient(app) as client:
		created = client.post(
			"/graphs",
			json={
				"graphId": graph_id,
				"message": "legacy-component-migration",
				"graph": _legacy_component_graph(),
			},
		)
		assert created.status_code == 200, created.text
		body = created.json()
		assert isinstance(body.get("migrationNotes"), list)
		latest = client.get(f"/graphs/{graph_id}/latest")
		assert latest.status_code == 200, latest.text
		stored = latest.json()["graph"]
		cmp_node = next(n for n in stored["nodes"] if n["id"] == "cmp1")
		bindings_outputs = (((cmp_node.get("data") or {}).get("params") or {}).get("bindings") or {}).get("outputs") or {}
		assert "summary" in bindings_outputs
		assert "out_data" not in bindings_outputs
		edge = stored["edges"][0]
		assert str(edge.get("sourceHandle")) == "summary"
		contract = ((edge.get("data") or {}).get("contract") or {})
		assert str(contract.get("out")) == "text"
		assert str((((contract.get("payload") or {}).get("source") or {}).get("type") or "")) == "string"

