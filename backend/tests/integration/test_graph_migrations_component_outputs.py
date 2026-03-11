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


def _invalid_multi_output_handle_graph() -> dict:
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
								{"name": "summary", "portType": "text", "required": True, "typedSchema": {"type": "text", "fields": []}},
								{"name": "source", "portType": "text", "required": True, "typedSchema": {"type": "text", "fields": []}},
							],
						},
						"bindings": {
							"inputs": {},
							"config": {},
							"outputs": {
								"summary": {"nodeId": "n_sum", "artifact": "current"},
								"source": {"nodeId": "n_src", "artifact": "current"},
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
						"output": {"mode": "text", "strict": True},
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
						"out": "text",
						"in": "text",
						"payload": {
							"source": {"type": "string"},
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
	assert not isinstance(((edge.get("data") or {}).get("componentOutputBinding")), dict)


def test_canonicalize_graph_payload_aligns_component_output_typed_schema_in_graph_node_params():
	graph = {
		"version": 1,
		"nodes": [
			{
				"id": "cmp1",
				"type": "component",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "component",
					"params": {
						"api": {
							"inputs": [],
							"outputs": [
								{
									"name": "summary",
									"portType": "text",
									"required": True,
									"typedSchema": {"type": "json", "fields": [{"name": "x", "type": "text"}]},
								}
							],
						},
						"bindings": {"inputs": {}, "config": {}, "outputs": {"summary": {"nodeId": "n1", "artifact": "current"}}},
						"config": {},
					},
				},
			}
		],
		"edges": [],
	}
	next_graph, notes = canonicalize_graph_payload(graph)
	assert isinstance(notes, list)
	params = ((next_graph["nodes"][0].get("data") or {}).get("params") or {})
	out = (((params.get("api") or {}).get("outputs") or [{}])[0]) if isinstance((params.get("api") or {}).get("outputs"), list) else {}
	assert str((out.get("typedSchema") or {}).get("type") or "") == "text"
	assert ((out.get("typedSchema") or {}).get("fields") or []) == []


def test_canonicalize_graph_payload_infers_named_handle_from_unique_contract_port_type():
	graph = {
		"version": 1,
		"nodes": [
			{
				"id": "cmp1",
				"type": "component",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "component",
					"params": {
						"api": {
							"inputs": [],
							"outputs": [
								{"name": "summary", "portType": "text", "required": True, "typedSchema": {"type": "text", "fields": []}},
								{"name": "payload", "portType": "json", "required": True, "typedSchema": {"type": "json", "fields": []}},
							],
						},
						"bindings": {
							"inputs": {},
							"config": {},
							"outputs": {
								"summary": {"nodeId": "n_sum", "artifact": "current"},
								"payload": {"nodeId": "n_payload", "artifact": "current"},
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
				"data": {"kind": "llm", "ports": {"in": "text", "out": "text"}},
			},
		],
		"edges": [
			{
				"id": "e1",
				"source": "cmp1",
				"sourceHandle": "out",
				"target": "llm1",
				"targetHandle": "in",
				"data": {"contract": {"out": "text", "in": "text", "payload": {"source": {"type": "json"}, "target": {"type": "string"}}}},
			}
		],
	}
	next_graph, _ = canonicalize_graph_payload(graph)
	edge = next_graph["edges"][0]
	assert str(edge.get("sourceHandle") or "") == "summary"


def test_canonicalize_graph_payload_defaults_builtin_profile_storage():
	graph = {
		"version": 1,
		"nodes": [
			{
				"id": "tool1",
				"type": "tool",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "tool",
					"ports": {"in": "json", "out": "json"},
					"params": {
						"provider": "builtin",
						"name": "Builtin tool",
						"builtin": {"toolId": "noop"},
					},
				},
			}
		],
		"edges": [],
	}
	next_graph, notes = canonicalize_graph_payload(graph)
	assert isinstance(notes, list)
	tool_params = ((next_graph["nodes"][0].get("data") or {}).get("params") or {})
	builtin = tool_params.get("builtin") or {}
	assert str(builtin.get("profileId") or "") == "core"
	assert builtin.get("customPackages") == []


def test_canonicalize_graph_payload_removes_legacy_ports_from_nodes():
	graph = {
		"version": 1,
		"nodes": [
			{
				"id": "src1",
				"type": "source",
				"position": {"x": 0, "y": 0},
				"data": {"kind": "source", "sourceKind": "file", "params": {"output": {"mode": "text"}}},
			},
			{
				"id": "tx1",
				"type": "transform",
				"position": {"x": 300, "y": 0},
				"data": {"kind": "transform", "params": {"op": "text_to_table"}},
			},
			{
				"id": "llm1",
				"type": "llm",
				"position": {"x": 600, "y": 0},
				"data": {"kind": "llm", "params": {"output": {"mode": "json"}}},
			},
		],
		"edges": [],
	}
	next_graph, notes = canonicalize_graph_payload(graph)
	assert isinstance(notes, list)
	nodes = {str(n.get("id") or ""): n for n in next_graph.get("nodes", [])}
	assert "ports" not in ((nodes["src1"].get("data") or {}))
	assert "ports" not in ((nodes["tx1"].get("data") or {}))
	assert "ports" not in ((nodes["llm1"].get("data") or {}))
	assert not any(str(note.get("code") or "") == "NODE_PORTS_DERIVED" for note in notes)


def test_canonicalize_graph_payload_updates_edge_contracts_after_port_derivation():
	graph = {
		"version": 1,
		"nodes": [
			{
				"id": "src1",
				"type": "source",
				"position": {"x": 0, "y": 0},
				"data": {"kind": "source", "sourceKind": "file", "params": {"output": {"mode": "text"}}},
			},
			{
				"id": "tx1",
				"type": "transform",
				"position": {"x": 300, "y": 0},
				"data": {"kind": "transform", "params": {"op": "text_to_table"}},
			},
		],
		"edges": [
			{
				"id": "e1",
				"source": "src1",
				"sourceHandle": "out",
				"target": "tx1",
				"targetHandle": "in",
			}
		],
	}
	next_graph, _ = canonicalize_graph_payload(graph)
	edge = next_graph["edges"][0]
	contract = ((edge.get("data") or {}).get("contract") or {})
	assert str(contract.get("out") or "") == "text"
	assert str(contract.get("in") or "") == "text"
	payload = contract.get("payload") if isinstance(contract.get("payload"), dict) else {}
	assert str(((payload.get("source") or {}).get("type") or "")) == "string"
	assert str(((payload.get("target") or {}).get("type") or "")) == "string"


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
		assert not isinstance(((edge.get("data") or {}).get("componentOutputBinding")), dict)


def test_graph_create_revision_rejects_ambiguous_multi_output_component_source_handle():
	graph_id = f"graph_invalid_cmp_{uuid4().hex[:8]}"
	with TestClient(app) as client:
		created = client.post(
			"/graphs",
			json={
				"graphId": graph_id,
				"message": "invalid-component-handle",
				"graph": _invalid_multi_output_handle_graph(),
			},
		)
		assert created.status_code == 422, created.text
		detail = created.json().get("detail", {})
		assert str(detail.get("code") or "") == "COMPONENT_OUTPUT_HANDLE_UNRESOLVED"


def test_canonicalize_graph_payload_canonicalizes_node_schema_contract_channels():
	graph = {
		"version": 1,
		"nodes": [
			{
				"id": "src1",
				"type": "source",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "source",
					"sourceKind": "file",
					"params": {"output": {"mode": "json"}},
					"schema": {
						"inferredSchema": {
							"source": "sample",
							"state": "fresh",
							"typedSchema": {
								"type": "json",
								"fields": [{"name": "id", "type": "json", "nullable": False}],
							},
							"extraKey": "drop",
						},
						"expectedSchema": {"source": "declared", "typedSchema": {"type": "json", "fields": []}},
						"unknownChannel": {},
					},
				},
			}
		],
		"edges": [],
	}
	next_graph, notes = canonicalize_graph_payload(graph)
	assert isinstance(notes, list)
	assert any(str(note.get("code") or "") == "NODE_SCHEMA_CONTRACT_CANONICALIZED" for note in notes)
	schema = (((next_graph["nodes"][0].get("data") or {}).get("schema") or {}))
	assert "unknownChannel" not in schema
	assert "extraKey" not in ((schema.get("inferredSchema") or {}))
	assert str((((schema.get("inferredSchema") or {}).get("source")) or "")) == "sample"
	assert str((((schema.get("expectedSchema") or {}).get("source")) or "")) == "declared"


def test_canonicalize_graph_payload_drops_invalid_node_schema_contract():
	graph = {
		"version": 1,
		"nodes": [
			{
				"id": "src1",
				"type": "source",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "source",
					"sourceKind": "file",
					"params": {"output": {"mode": "json"}},
					"schema": ["invalid"],
				},
			}
		],
		"edges": [],
	}
	next_graph, notes = canonicalize_graph_payload(graph)
	assert isinstance(notes, list)
	assert any(str(note.get("code") or "") == "NODE_SCHEMA_CONTRACT_DROPPED" for note in notes)
	assert "schema" not in ((next_graph["nodes"][0].get("data") or {}))
