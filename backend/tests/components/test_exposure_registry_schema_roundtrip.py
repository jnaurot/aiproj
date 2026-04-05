from app.component_contracts import canonicalize_component_definition


def test_exposure_registry_roundtrip_preserves_handle_identity():
	definition = {
		"graph": {"nodes": [], "edges": []},
		"api": {
			"inputs": [{"name": "in_data", "typedSchema": {"type": "json", "fields": []}}],
			"outputs": [{"name": "out_data", "typedSchema": {"type": "text", "fields": []}}],
		},
		"exposureRegistry": [
			{
				"handle_id": "data_out::out_data",
				"alias": "out_data",
				"internal_source_path": "out:out_data",
				"kind": "data_output",
				"native_contract": {"type": "text", "fields": []},
				"exposed": True,
				"published": True,
				"debug_visible": False,
			}
		],
	}
	normalized, _notes = canonicalize_component_definition(definition, 1)
	registry = normalized.get("exposureRegistry") if isinstance(normalized.get("exposureRegistry"), list) else []
	assert len(registry) == 1
	assert registry[0]["handle_id"] == "data_out::out_data"
	assert registry[0]["published"] is True
	assert "published_profile" in normalized
	assert "debug_profile" in normalized

