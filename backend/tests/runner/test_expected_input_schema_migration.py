from __future__ import annotations

from app.graph_migrations import canonicalize_graph_payload


def test_migrates_legacy_expected_input_schema_to_split_map():
	graph = {
		"version": 1,
		"nodes": [
			{
				"id": "n_model",
				"type": "model",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "model",
					"label": "Model",
					"params": {},
					"schema": {
						"expectedInputSchema": {
							"typedSchema": {"type": "json", "fields": []},
							"source": "declared",
							"state": "fresh",
						}
					},
				},
			}
		],
		"edges": [],
	}
	canonical, notes = canonicalize_graph_payload(graph)
	node = canonical["nodes"][0]
	schema = node["data"]["schema"]
	assert schema["expectedInputSchemas"]["in"]["typedSchema"]["type"] == "json"
	assert "expectedInputSchema" not in schema
	assert any(note.get("code") == "NODE_SCHEMA_EXPECTED_INPUTS_MIGRATED" for note in notes)
