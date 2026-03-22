from __future__ import annotations

from app.component_contracts import canonicalize_component_definition


def test_component_api_legacy_inputs_map_to_work_inputs() -> None:
	definition = {
		"api": {
			"inputs": [{"name": "in", "typedSchema": {"type": "text"}}],
			"outputs": [{"name": "out", "typedSchema": {"type": "json"}}],
		},
		"graph": {"nodes": [], "edges": []},
	}

	canonical, _notes = canonicalize_component_definition(definition)
	api = canonical["api"]

	assert api["inputs"][0]["name"] == "in"
	assert api["workInputs"][0]["name"] == "in"
	assert api["paramInputs"] == []
	assert api["controlInputs"] == []


def test_component_api_affinity_inputs_preserved() -> None:
	definition = {
		"api": {
			"workInputs": [{"name": "in", "typedSchema": {"type": "json"}}],
			"paramInputs": [{"name": "param_filters", "typedSchema": {"type": "json"}}],
			"controlInputs": [{"name": "control_in", "typedSchema": {"type": "text"}}],
			"outputs": [{"name": "out", "typedSchema": {"type": "json"}}],
		},
		"graph": {"nodes": [], "edges": []},
	}

	canonical, _notes = canonicalize_component_definition(definition)
	api = canonical["api"]

	assert [entry["name"] for entry in api["workInputs"]] == ["in"]
	assert [entry["name"] for entry in api["paramInputs"]] == ["param_filters"]
	assert [entry["name"] for entry in api["controlInputs"]] == ["control_in"]
	assert [entry["name"] for entry in api["inputs"]] == ["in"]
