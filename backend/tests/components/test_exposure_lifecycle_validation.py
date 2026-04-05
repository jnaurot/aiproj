from app.component_contracts import validate_component_definition


def test_validation_rejects_published_without_exposed():
	definition = {
		"graph": {"nodes": [], "edges": []},
		"api": {"inputs": [], "outputs": []},
		"exposureRegistry": [
			{
				"handle_id": "h1",
				"alias": "h1",
				"internal_source_path": "out:h1",
				"kind": "data_output",
				"native_contract": {"type": "json", "fields": []},
				"exposed": False,
				"published": True,
				"debug_visible": False,
			}
		],
	}
	diags = validate_component_definition(definition)
	codes = {d.code for d in diags}
	assert "INVALID_EXPOSURE_LIFECYCLE" in codes

