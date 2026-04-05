from __future__ import annotations

from app.component_contracts import canonicalize_component_definition


def test_component_authoring_roundtrip_preserves_exposure_profiles() -> None:
	definition = {
		"schemaVersion": 1,
		"graph": {"nodes": [], "edges": []},
		"api": {
			"inputs": [],
			"outputs": [{"name": "out_text", "required": True, "typedSchema": {"type": "text", "fields": []}}],
		},
		"bindings": {"outputs": {"out_text": {"outputRef": "llm:writer", "artifact": "current"}}},
		"exposureRegistry": [
			{
				"handle_id": "h_data_out_text",
				"alias": "out_text",
				"internal_source_path": "out:out_text",
				"kind": "data_output",
				"native_type": {"type": "text"},
				"exposed": True,
				"published": True,
				"debug_visible": True,
			},
			{
				"handle_id": "h_debug_only",
				"alias": "debug_payload",
				"internal_source_path": "out:debug_payload",
				"kind": "data_output",
				"native_type": {"type": "json"},
				"exposed": True,
				"published": False,
				"debug_visible": True,
			},
		],
	}

	normalized, _ = canonicalize_component_definition(definition, schema_version=1)
	registry = normalized.get("exposureRegistry") if isinstance(normalized.get("exposureRegistry"), list) else []
	published = normalized.get("published_profile") if isinstance(normalized.get("published_profile"), list) else []
	debug = normalized.get("debug_profile") if isinstance(normalized.get("debug_profile"), list) else []

	assert len(registry) == 2
	assert {str(item.get("handle_id")) for item in published} == {"h_data_out_text"}
	assert {str(item.get("handle_id")) for item in debug} == {"h_data_out_text", "h_debug_only"}

