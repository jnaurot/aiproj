import pytest

from app.runner.run import resolve_input_refs


@pytest.mark.asyncio
async def test_component_partial_output_change_does_not_collapse_other_output_identity():
	nodes = {
		"component_1": {
			"id": "component_1",
			"data": {
				"kind": "component",
				"params": {
					"api": {
						"inputs": [],
						"outputs": [
							{"name": "out_a", "typedSchema": {"type": "text", "fields": []}},
							{"name": "out_b", "typedSchema": {"type": "text", "fields": []}},
						],
					},
					"bindings": {
						"outputs": {
							"out_a": {"artifact": "current", "outputRef": "n1.out"},
							"out_b": {"artifact": "current", "outputRef": "n2.out"},
						}
					},
				},
			},
		},
		"down_a": {"id": "down_a", "data": {"kind": "llm", "params": {"model": "x", "user_prompt": "x"}}},
		"down_b": {"id": "down_b", "data": {"kind": "llm", "params": {"model": "x", "user_prompt": "x"}}},
	}
	edges = {
		"e_int_a": {"id": "e_int_a", "source": "cmp:component_1:n1", "target": "component_1", "targetHandle": "out_a"},
		"e_int_b": {"id": "e_int_b", "source": "cmp:component_1:n2", "target": "component_1", "targetHandle": "out_b"},
		"e_out_a": {"id": "e_out_a", "source": "component_1", "sourceHandle": "out_a", "target": "down_a", "targetHandle": "in"},
		"e_out_b": {"id": "e_out_b", "source": "component_1", "sourceHandle": "out_b", "target": "down_b", "targetHandle": "in"},
	}

	artifacts_v1 = {"cmp:component_1:n1": "a_v1", "cmp:component_1:n2": "b_v1"}
	refs_a_v1 = await resolve_input_refs(edges, "down_a", lambda nid: artifacts_v1.get(str(nid)), lambda nid: nodes.get(str(nid)), None)
	refs_b_v1 = await resolve_input_refs(edges, "down_b", lambda nid: artifacts_v1.get(str(nid)), lambda nid: nodes.get(str(nid)), None)
	assert refs_a_v1 == [("in", "a_v1")]
	assert refs_b_v1 == [("in", "b_v1")]

	artifacts_v2 = {"cmp:component_1:n1": "a_v2", "cmp:component_1:n2": "b_v1"}
	refs_a_v2 = await resolve_input_refs(edges, "down_a", lambda nid: artifacts_v2.get(str(nid)), lambda nid: nodes.get(str(nid)), None)
	refs_b_v2 = await resolve_input_refs(edges, "down_b", lambda nid: artifacts_v2.get(str(nid)), lambda nid: nodes.get(str(nid)), None)
	assert refs_a_v2 == [("in", "a_v2")]
	assert refs_b_v2 == [("in", "b_v1")]
