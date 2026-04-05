from app.component_contracts import component_contract_diff


def test_replay_identity_flags_breaking_when_published_handle_removed():
	before = [
		{
			"handle_id": "data_out::out_data",
			"kind": "data_output",
			"native_contract": {"type": "json", "fields": []},
		}
	]
	after = []
	diff = component_contract_diff(before, after)
	assert diff["breaking"] is True
	assert diff["removed"] == ["data_out::out_data"]

