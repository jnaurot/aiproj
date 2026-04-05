from app.component_contracts import materialize_exposure_profiles


def test_materialize_profiles_separates_published_and_debug():
	records = [
		{
			"handle_id": "h_pub",
			"alias": "h_pub",
			"internal_source_path": "out:h_pub",
			"kind": "data_output",
			"native_contract": {"type": "json", "fields": []},
			"exposed": True,
			"published": True,
			"debug_visible": False,
		},
		{
			"handle_id": "h_dbg",
			"alias": "h_dbg",
			"internal_source_path": "out:h_dbg",
			"kind": "data_output",
			"native_contract": {"type": "json", "fields": []},
			"exposed": True,
			"published": False,
			"debug_visible": True,
		},
	]
	profiles = materialize_exposure_profiles(records)
	published_ids = {str(item.get("handle_id")) for item in profiles.get("published_profile", [])}
	debug_ids = {str(item.get("handle_id")) for item in profiles.get("debug_profile", [])}
	assert published_ids == {"h_pub"}
	assert debug_ids == {"h_pub", "h_dbg"}

