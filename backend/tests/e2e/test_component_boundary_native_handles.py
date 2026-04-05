from __future__ import annotations

import pytest

from app.runner.run import resolve_input_refs


@pytest.mark.asyncio
async def test_component_multi_output_native_handles_and_partial_changes() -> None:
	nodes = {
		"component_1": {
			"id": "component_1",
			"data": {
				"kind": "component",
				"params": {
					"api": {
						"inputs": [],
						"outputs": [
							{"name": "out_text", "typedSchema": {"type": "text", "fields": []}},
							{"name": "out_json", "typedSchema": {"type": "json", "fields": []}},
						],
					},
					"bindings": {
						"outputs": {
							"out_text": {"artifact": "current", "outputRef": "n_text.out"},
							"out_json": {"artifact": "current", "outputRef": "n_json.out"},
						}
					},
				},
			},
		},
		"down_text": {"id": "down_text", "data": {"kind": "llm", "params": {"model": "x", "user_prompt": "x"}}},
		"down_json": {"id": "down_json", "data": {"kind": "transform", "params": {"op": "json_filter"}}},
	}
	edges = {
		"e_int_text": {"id": "e_int_text", "source": "cmp:component_1:n_text", "target": "component_1", "targetHandle": "out_text"},
		"e_int_json": {"id": "e_int_json", "source": "cmp:component_1:n_json", "target": "component_1", "targetHandle": "out_json"},
		"e_out_text": {"id": "e_out_text", "source": "component_1", "sourceHandle": "out_text", "target": "down_text", "targetHandle": "in"},
		"e_out_json": {"id": "e_out_json", "source": "component_1", "sourceHandle": "out_json", "target": "down_json", "targetHandle": "in"},
	}

	artifacts_v1 = {"cmp:component_1:n_text": "a_text_v1", "cmp:component_1:n_json": "a_json_v1"}
	text_refs_v1 = await resolve_input_refs(edges, "down_text", lambda nid: artifacts_v1.get(str(nid)), lambda nid: nodes.get(str(nid)), None)
	json_refs_v1 = await resolve_input_refs(edges, "down_json", lambda nid: artifacts_v1.get(str(nid)), lambda nid: nodes.get(str(nid)), None)
	assert text_refs_v1 == [("in", "a_text_v1")]
	assert json_refs_v1 == [("in", "a_json_v1")]

	artifacts_v2 = {"cmp:component_1:n_text": "a_text_v2", "cmp:component_1:n_json": "a_json_v1"}
	text_refs_v2 = await resolve_input_refs(edges, "down_text", lambda nid: artifacts_v2.get(str(nid)), lambda nid: nodes.get(str(nid)), None)
	json_refs_v2 = await resolve_input_refs(edges, "down_json", lambda nid: artifacts_v2.get(str(nid)), lambda nid: nodes.get(str(nid)), None)
	assert text_refs_v2 == [("in", "a_text_v2")]
	assert json_refs_v2 == [("in", "a_json_v1")]

