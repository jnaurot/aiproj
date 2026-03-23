from __future__ import annotations

from app.graph_migrations import canonicalize_graph_payload
from app.schema_contracts import canonicalize_schema_envelope


def test_schema_envelope_rejects_legacy_expected_input_schema_channel() -> None:
	canonical, changed = canonicalize_schema_envelope(
		{
			"expectedInputSchema": {
				"typedSchema": {"type": "json", "fields": []},
				"source": "declared",
				"state": "fresh",
			},
			"expectedInputSchemas": {
				"in": {
					"typedSchema": {"type": "json", "fields": []},
					"source": "declared",
					"state": "fresh",
				}
			},
		}
	)
	assert changed is True
	assert isinstance(canonical, dict)
	assert "expectedInputSchema" not in canonical
	assert ((canonical.get("expectedInputSchemas") or {}).get("in") or {}).get("typedSchema", {}).get("type") == "json"


def test_graph_migration_converts_and_removes_legacy_expected_input_schema() -> None:
	graph = {
		"version": 1,
		"nodes": [
			{
				"id": "n_sink",
				"type": "model",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "model",
					"label": "Sink",
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
	canonical, _notes = canonicalize_graph_payload(graph)
	schema = (((canonical.get("nodes") or [])[0].get("data") or {}).get("schema") or {})
	assert "expectedInputSchema" not in schema
	assert ((schema.get("expectedInputSchemas") or {}).get("in") or {}).get("typedSchema", {}).get("type") == "json"
