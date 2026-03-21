from app.executors.llm import _resolve_llm_output_mode, normalize_llm_params


def _node_with_expected_type(expected_type: str) -> dict:
	return {
		"id": "n_model",
		"data": {
			"kind": "model",
			"schema": {
				"expectedSchema": {
					"typedSchema": {"type": expected_type, "fields": []},
				}
			},
		},
	}


def test_normalize_llm_params_maps_nested_output_mode():
	params = normalize_llm_params({"output": {"mode": "text"}})
	assert params["output_mode"] == "text"


def test_normalize_llm_params_prefers_nested_output_mode_over_legacy_output_mode():
	params = normalize_llm_params({"output_mode": "json", "output": {"mode": "text"}})
	assert params["output_mode"] == "text"


def test_resolve_llm_output_mode_prefers_explicit_params_over_declared_schema():
	node = _node_with_expected_type("json")
	norm = normalize_llm_params({"output": {"mode": "text"}})
	assert _resolve_llm_output_mode(node, norm) == "text"


def test_resolve_llm_output_mode_falls_back_to_declared_schema_when_missing_explicit():
	node = _node_with_expected_type("json")
	norm = normalize_llm_params({})
	assert _resolve_llm_output_mode(node, norm) == "json"
