import pytest

from app.runner.run import resolve_input_refs


@pytest.mark.asyncio
async def test_component_input_routing_preserves_data_param_control_handles():
	nodes = {
		"up_data": {"id": "up_data", "data": {"kind": "source"}},
		"up_param": {"id": "up_param", "data": {"kind": "source"}},
		"up_ctl": {"id": "up_ctl", "data": {"kind": "source"}},
		"component_1": {
			"id": "component_1",
			"data": {
				"kind": "component",
				"params": {
					"api": {
						"inputs": [{"name": "in_data", "typedSchema": {"type": "json", "fields": []}}],
						"outputs": [{"name": "out_data", "typedSchema": {"type": "json", "fields": []}}],
					}
				},
			},
		},
	}
	edges = {
		"e_data": {"id": "e_data", "source": "up_data", "target": "component_1", "targetHandle": "in_data"},
		"e_param": {"id": "e_param", "source": "up_param", "target": "component_1", "targetHandle": "param_cfg"},
		"e_ctl": {"id": "e_ctl", "source": "up_ctl", "target": "component_1", "targetHandle": "control_in"},
	}
	artifacts = {"up_data": "a_data", "up_param": "a_param", "up_ctl": "a_ctl"}
	refs = await resolve_input_refs(
		edges=edges,
		node_id="component_1",
		get_current_artifact=lambda nid: artifacts.get(str(nid), None),
		get_node_by_id=lambda nid: nodes.get(str(nid), None),
		artifact_store=None,
	)
	assert ("in_data", "a_data") in refs
	assert ("param_cfg", "a_param") in refs
	assert ("control_in", "a_ctl") in refs

