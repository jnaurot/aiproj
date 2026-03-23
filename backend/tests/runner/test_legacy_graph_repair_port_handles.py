from __future__ import annotations

from app.graph_migrations import canonicalize_graph_payload


def test_legacy_repair_normalizes_malformed_expected_input_schemas_and_snapshots() -> None:
	graph, notes = canonicalize_graph_payload(
		{
			"nodes": [
				{
					"id": "n_model",
					"data": {
						"kind": "model",
						"schema": {
							"expectedInputSchemas": {
								"": {"typedSchema": {"type": "json"}},
								"in": {"typedSchema": {"type": "text", "fields": []}},
								"param_filters": {"typedSchema": "invalid"},
							}
						},
					},
				}
			],
			"edges": [
				{
					"id": "e1",
					"source": "n_model",
					"target": "n_model",
					"data": {
						"contract": {
							"payload": {"source": {"type": "text"}, "target": {"type": "json"}},
							"snapshot": {"decision": "??"},
						}
					},
				}
			],
		}
	)
	node = graph["nodes"][0]
	expected_inputs = (((node.get("data") or {}).get("schema") or {}).get("expectedInputSchemas") or {})
	assert "" not in expected_inputs
	assert expected_inputs.get("in", {}).get("typedSchema", {}).get("type") == "text"
	assert expected_inputs.get("param_filters", {}).get("typedSchema", {}).get("type") == "text"
	edge = graph["edges"][0]
	snapshot = ((((edge.get("data") or {}).get("contract") or {}).get("snapshot") or {}))
	assert str(snapshot.get("sourceSchemaFingerprint") or "")
	assert str(snapshot.get("targetSchemaFingerprint") or "")
	codes = {str(note.get("code") or "") for note in notes}
	assert "NODE_EXPECTED_INPUT_SCHEMAS_REPAIRED" in codes
	assert "EDGE_CONTRACT_SNAPSHOT_REPAIRED" in codes
