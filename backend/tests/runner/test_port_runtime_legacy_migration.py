from __future__ import annotations

from app.graph_migrations import canonicalize_graph_payload
from app.runner.validator import GraphValidator


def test_legacy_port_runtime_migration_notes_include_deprecation_metadata() -> None:
	graph = {
		"version": 1,
		"nodes": [
			{
				"id": "n1",
				"type": "tool",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"schema": {
						"expectedInputSchema": {
							"typedSchema": {"type": "json", "fields": []},
							"source": "declared",
							"state": "fresh",
						}
					},
					"portContracts": {"in": {"in": {"affinity": "work"}}, "out": {"out": {"affinity": "work"}}},
				},
			}
		],
		"edges": [],
	}
	_, notes = canonicalize_graph_payload(graph)
	note_codes = {str(note.get("code") or ""): note for note in notes}
	assert "NODE_SCHEMA_EXPECTED_INPUTS_MIGRATED" in note_codes
	assert "NODE_PORT_CONTRACTS_DEPRECATED" in note_codes
	schema_note = note_codes["NODE_SCHEMA_EXPECTED_INPUTS_MIGRATED"]
	assert schema_note.get("severity") == "warning"
	assert (schema_note.get("deprecation") or {}).get("removeAfter") == "2026-06-30"
	port_note = note_codes["NODE_PORT_CONTRACTS_DEPRECATED"]
	assert port_note.get("severity") == "warning"
	assert (port_note.get("deprecation") or {}).get("replacement") == "data.portDeclarations"


def test_validator_emits_port_runtime_deprecation_warnings() -> None:
	validator = GraphValidator()
	graph = {
		"nodes": [
			{
				"id": "src",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"portContracts": {"out": {"out": {"affinity": "work"}}},
				},
			},
			{
				"id": "dst",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"schema": {
						"expectedInputSchema": {
							"typedSchema": {"type": "json", "fields": []},
							"source": "declared",
							"state": "fresh",
						}
					},
					"portContracts": {"in": {"in": {"affinity": "work"}}},
				},
			},
		],
		"edges": [
			{
				"id": "e1",
				"source": "src",
				"target": "dst",
				"data": {
					"mode": "work",
					"queue": {"policy": "round_robin"},
					"contract": {
						"payload": {"source": {"type": "json"}, "target": {"type": "json"}},
						"snapshot": {
							"sourceSchemaFingerprint": "{\"type\":\"json\"}",
							"targetSchemaFingerprint": "{\"type\":\"json\"}",
							"compatible": True,
							"decision": "native",
						},
					},
				},
			}
		],
	}
	result = validator.validate_pre_execution(graph)
	codes = {warning.code for warning in result.warnings}
	assert "LEGACY_EXPECTED_INPUT_SCHEMA_DEPRECATED" in codes
	assert "LEGACY_PORT_CONTRACTS_DEPRECATED" in codes
	assert "EDGE_QUEUE_POLICY_PREVIEW" in codes
