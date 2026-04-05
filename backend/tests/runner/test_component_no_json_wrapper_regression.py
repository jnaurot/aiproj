import pytest

from app.runner.run import resolve_input_refs


@pytest.mark.asyncio
async def test_component_downstream_prefers_bound_native_output_over_component_wrapper_artifact():
	nodes = {
		"component_1": {
			"id": "component_1",
			"data": {
				"kind": "component",
				"params": {
					"api": {"inputs": [], "outputs": [{"name": "out_data", "typedSchema": {"type": "text", "fields": []}}]},
					"bindings": {"outputs": {"out_data": {"artifact": "current", "outputRef": "internal.out"}}},
				},
			},
		},
		"down": {"id": "down", "data": {"kind": "llm", "params": {"model": "x", "user_prompt": "x"}}},
	}
	edges = {
		"e_internal": {
			"id": "e_internal",
			"source": "cmp:component_1:internal",
			"target": "component_1",
			"targetHandle": "out_data",
		},
		"e_parent": {
			"id": "e_parent",
			"source": "component_1",
			"sourceHandle": "out_data",
			"target": "down",
			"targetHandle": "in",
		},
	}
	artifacts = {
		"cmp:component_1:internal": "native_text_artifact",
	}
	refs = await resolve_input_refs(
		edges=edges,
		node_id="down",
		get_current_artifact=lambda nid: artifacts.get(str(nid), None),
		get_node_by_id=lambda nid: nodes.get(str(nid), None),
		artifact_store=None,
	)
	assert refs == [("in", "native_text_artifact")]
