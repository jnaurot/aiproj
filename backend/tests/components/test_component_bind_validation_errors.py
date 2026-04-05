from app.component_contracts import validate_component_definition


def test_invalid_exposure_kind_reports_validation_error():
	definition = {
		"graph": {"nodes": [], "edges": []},
		"api": {"inputs": [], "outputs": []},
		"exposureRegistry": [
			{
				"handle_id": "bad_kind",
				"alias": "bad_kind",
				"internal_source_path": "out:bad_kind",
				"kind": "control_output",
				"native_contract": {"type": "json", "fields": []},
				"exposed": True,
				"published": True,
				"debug_visible": False,
			}
		],
	}
	diags = validate_component_definition(definition)
	assert any(d.code == "INVALID_EXPOSURE_KIND" for d in diags)

