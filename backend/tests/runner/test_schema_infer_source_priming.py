from app.runner.schema_infer import infer_typed_schema_from_sample_profile


def test_infer_typed_schema_from_sample_profile_table():
	schema = infer_typed_schema_from_sample_profile(
		[
			{"id": 1, "name": "a", "score": 1.2, "flag": True, "meta": {"k": "v"}, "nullable": None},
			{"id": 2, "name": "b", "score": 2.4, "flag": False, "meta": {"k": "x"}, "nullable": "x"},
		],
		"table",
	)
	assert schema.get("type") == "table"
	fields = {f.get("name"): f for f in schema.get("fields") or []}
	assert fields["id"]["type"] in {"int", "float"}
	assert fields["name"]["type"] == "string"
	assert fields["flag"]["type"] == "bool"
	assert fields["meta"]["type"] == "json"
	assert fields["nullable"]["nullable"] is True


def test_infer_typed_schema_from_sample_profile_json_deterministic():
	sample = {"ok": True, "items": [{"id": 1}, {"id": 2}]}
	s1 = infer_typed_schema_from_sample_profile(sample, "json")
	s2 = infer_typed_schema_from_sample_profile(sample, "json")
	assert s1 == s2
	assert s1.get("type") == "json"
	assert isinstance(s1.get("schema"), dict)
