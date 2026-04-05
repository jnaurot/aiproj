from app.component_contracts import canonicalize_component_definition


def test_missing_exposure_registry_derives_defaults_from_api():
	definition = {
		"graph": {"nodes": [], "edges": []},
		"api": {
			"inputs": [{"name": "in_data", "typedSchema": {"type": "table", "fields": []}}],
			"outputs": [{"name": "out_data", "typedSchema": {"type": "json", "fields": []}}],
		},
	}
	normalized, _notes = canonicalize_component_definition(definition, 1)
	registry = normalized.get("exposureRegistry") if isinstance(normalized.get("exposureRegistry"), list) else []
	handle_ids = {str(item.get("handle_id")) for item in registry if isinstance(item, dict)}
	assert "work_in::in_data" in handle_ids
	assert "data_out::out_data" in handle_ids

