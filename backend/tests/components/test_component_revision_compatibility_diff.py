from app.component_contracts import component_contract_diff


def test_component_contract_diff_detects_breaking_changes():
	before = [
		{
			"handle_id": "h1",
			"kind": "data_output",
			"native_contract": {"type": "json", "fields": []},
		},
		{
			"handle_id": "h2",
			"kind": "data_output",
			"native_contract": {"type": "text", "fields": []},
		},
	]
	after = [
		{
			"handle_id": "h2",
			"kind": "data_output",
			"native_contract": {"type": "json", "fields": []},
		}
	]
	diff = component_contract_diff(before, after)
	assert diff["breaking"] is True
	assert diff["removed"] == ["h1"]
	assert len(diff["retyped"]) == 1

