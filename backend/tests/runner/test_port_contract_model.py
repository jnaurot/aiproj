from __future__ import annotations

from app.component_contracts import canonicalize_input_contracts


def test_canonicalize_input_contracts_normalizes_handles_and_defaults() -> None:
	raw = {
		"workInputs": {
			"defaultSchema": {"typedSchema": {"type": "json", "fields": []}},
			"handles": {
				"in": {"typedSchema": {"type": "json", "fields": []}},
				"": {"typedSchema": {"type": "text", "fields": []}},
				None: {"typedSchema": {"type": "text", "fields": []}},
			},
		},
		"paramInputs": {"handles": {"param_config": {"shape": {"requiredKeys": ["a"]}}}},
	}
	normalized = canonicalize_input_contracts(raw)

	assert set(normalized.keys()) == {"workInputs", "paramInputs", "controlInputs"}
	assert normalized["workInputs"]["defaultSchema"]["typedSchema"]["type"] == "json"
	assert "in" in normalized["workInputs"]["handles"]
	assert "" not in normalized["workInputs"]["handles"]
	assert "None" not in normalized["workInputs"]["handles"]
	assert "param_config" in normalized["paramInputs"]["handles"]
	assert normalized["controlInputs"]["handles"] == {}


def test_canonicalize_input_contracts_empty_for_non_object() -> None:
	normalized = canonicalize_input_contracts(None)
	assert normalized == {
		"workInputs": {"handles": {}},
		"paramInputs": {"handles": {}},
		"controlInputs": {"handles": {}},
	}
