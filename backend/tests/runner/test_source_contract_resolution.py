from app.runner.contracts import JSON_ANY_V1, TABLE_V1, TEXT_V1, default_contract_for_node


def _source_node(params: dict, source_kind: str = "file") -> dict:
	return {
		"id": "n_source",
		"data": {
			"kind": "source",
			"sourceKind": source_kind,
			"params": params,
		},
	}


def test_source_contract_uses_explicit_output_mode_over_file_format():
	node = _source_node({"file_format": "csv", "output": {"mode": "json"}})
	assert default_contract_for_node(node) == JSON_ANY_V1


def test_source_contract_uses_expected_schema_before_output_mode():
	node = _source_node(
		{
			"file_format": "csv",
			"output": {"mode": "json"},
		}
	)
	node["data"]["schema"] = {
		"expectedSchema": {
			"typedSchema": {"type": "text", "fields": []},
		}
	}
	assert default_contract_for_node(node) == TEXT_V1


def test_source_contract_falls_back_when_no_explicit_mode():
	node = _source_node({"file_format": "csv"})
	assert default_contract_for_node(node) == TABLE_V1
