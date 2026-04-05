import pytest

from app.runner.run import resolve_input_refs


def _component_node() -> dict:
	return {
		"id": "component_1",
		"data": {
			"kind": "component",
			"params": {
				"api": {
					"inputs": [],
					"outputs": [
						{"name": "out_data", "typedSchema": {"type": "text", "fields": []}},
						{"name": "out_json", "typedSchema": {"type": "json", "fields": []}},
					],
				},
				"bindings": {
					"outputs": {
						"out_data": {"artifact": "current", "outputRef": "llm_summary.out"},
						"out_json": {"artifact": "current", "outputRef": "llm_json.out"},
					}
				},
			},
		},
	}


def _llm_node(node_id: str) -> dict:
	return {
		"id": node_id,
		"data": {
			"kind": "llm",
			"schema": {"expectedSchema": {"typedSchema": {"type": "text", "fields": []}}},
			"params": {"model": "x", "user_prompt": "test", "output_mode": "text"},
		},
	}


@pytest.mark.asyncio
async def test_component_routes_downstream_to_native_internal_artifact_per_handle():
	nodes = {
		"component_1": _component_node(),
		"llm_a": _llm_node("llm_a"),
		"llm_b": _llm_node("llm_b"),
	}
	edges = {
		"e_internal_summary": {
			"id": "e_internal_summary",
			"source": "cmp:component_1:n_llm_summary",
			"target": "component_1",
			"targetHandle": "out_data",
		},
		"e_internal_json": {
			"id": "e_internal_json",
			"source": "cmp:component_1:n_llm_json",
			"target": "component_1",
			"targetHandle": "out_json",
		},
		"e_parent_out_data": {
			"id": "e_parent_out_data",
			"source": "component_1",
			"sourceHandle": "out_data",
			"target": "llm_a",
			"targetHandle": "in",
		},
		"e_parent_out_json": {
			"id": "e_parent_out_json",
			"source": "component_1",
			"sourceHandle": "out_json",
			"target": "llm_b",
			"targetHandle": "in",
		},
	}
	artifacts = {
		"cmp:component_1:n_llm_summary": "artifact_summary_native",
		"cmp:component_1:n_llm_json": "artifact_json_native",
	}

	refs_a = await resolve_input_refs(
		edges=edges,
		node_id="llm_a",
		get_current_artifact=lambda nid: artifacts.get(str(nid), None),
		get_node_by_id=lambda nid: nodes.get(str(nid), None),
		artifact_store=None,
	)
	refs_b = await resolve_input_refs(
		edges=edges,
		node_id="llm_b",
		get_current_artifact=lambda nid: artifacts.get(str(nid), None),
		get_node_by_id=lambda nid: nodes.get(str(nid), None),
		artifact_store=None,
	)

	assert refs_a == [("in", "artifact_summary_native")]
	assert refs_b == [("in", "artifact_json_native")]
