from app.runner.contracts import EMBEDDINGS_ANY_V1, JSON_ANY_V1, TEXT_V1, default_contract_for_node


def _model_node(params: dict | None = None, schema: dict | None = None) -> dict:
	node = {
		"id": "n_model",
		"data": {
			"kind": "model",
			"llmKind": "ollama",
			"modelKind": "llm",
			"taskKind": "generate",
			"params": {
				"baseUrl": "http://localhost:11434",
				"model": "demo-model",
				"user_prompt": "hello",
				**(params or {}),
			},
		},
	}
	if schema:
		node["data"]["schema"] = schema
	return node


def test_model_contract_uses_declared_schema_before_output_mode():
	node = _model_node(
		params={"output": {"mode": "embeddings", "embedding": {"dims": 8}}},
		schema={"expectedSchema": {"typedSchema": {"type": "text", "fields": []}}},
	)
	assert default_contract_for_node(node) == TEXT_V1


def test_model_contract_uses_output_mode_when_declared_schema_missing():
	node = _model_node(params={"output": {"mode": "json", "jsonSchema": {"type": "object"}}})
	assert default_contract_for_node(node) == JSON_ANY_V1


def test_model_contract_falls_back_to_text_default():
	node = _model_node()
	assert default_contract_for_node(node) == TEXT_V1


def test_model_contract_resolution_is_deterministic_across_vectors():
	vectors = [
		(
			"declared_overrides_params",
			_model_node(
				params={"output": {"mode": "text"}},
				schema={"expectedSchema": {"typedSchema": {"type": "json", "fields": []}}},
			),
			JSON_ANY_V1,
		),
		(
			"params_used_when_no_declared",
			_model_node(params={"output": {"mode": "embeddings", "embedding": {"dims": 8}}}),
			EMBEDDINGS_ANY_V1,
		),
		("default_text", _model_node(), TEXT_V1),
	]
	resolved = [(case_id, default_contract_for_node(node)) for case_id, node, _ in vectors]
	assert resolved == [(case_id, expected) for case_id, _, expected in vectors]