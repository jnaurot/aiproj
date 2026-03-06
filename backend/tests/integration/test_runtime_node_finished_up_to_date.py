import pytest

from app.runtime import RuntimeManager


@pytest.mark.asyncio
async def test_node_finished_succeeded_marks_binding_up_to_date_without_artifact():
	rt = RuntimeManager()
	handle = rt.create_run("run_binding_component_parent")
	handle.graph_id = "graph_test"

	rt._apply_event_to_state(
		handle,
		{
			"type": "node_finished",
			"runId": handle.run_id,
			"at": "2026-03-06T00:00:00Z",
			"nodeId": "component_parent",
			"status": "succeeded",
		},
	)

	binding = handle.node_bindings.get("component_parent") or {}
	assert binding.get("status") == "succeeded_up_to_date"
	assert binding.get("isUpToDate") is True
