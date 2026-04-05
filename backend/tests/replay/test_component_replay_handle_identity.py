from app.component_contracts import component_contract_diff


def test_replay_identity_handles_match_when_published_profiles_match():
	before = [
		{
			"handle_id": "data_out::out_data",
			"kind": "data_output",
			"native_contract": {"type": "json", "fields": []},
		}
	]
	after = [
		{
			"handle_id": "data_out::out_data",
			"kind": "data_output",
			"native_contract": {"type": "json", "fields": []},
		}
	]
	diff = component_contract_diff(before, after)
	assert diff["breaking"] is False

