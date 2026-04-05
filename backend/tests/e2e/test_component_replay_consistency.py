from __future__ import annotations

from typing import Any, Dict

import pytest

from app.runtime import RuntimeManager
from app.runner.run import _build_frontier_identity_basis


def _graph() -> Dict[str, Any]:
	return {
		"nodes": [
			{
				"id": "component_1",
				"data": {
					"kind": "component",
					"params": {
						"componentRef": {"componentId": "CompA", "revisionId": "r1", "apiVersion": "v1"},
						"api": {"inputs": [], "outputs": [{"name": "out_text", "typedSchema": {"type": "text", "fields": []}}]},
					},
				},
			},
			{"id": "n2", "data": {"kind": "model", "params": {"model": "qwen3.5:4b", "temperature": 0}}},
		],
		"edges": [{"id": "e1", "source": "component_1", "sourceHandle": "out_text", "target": "n2", "targetHandle": "in"}],
	}


@pytest.mark.asyncio
async def test_component_replay_consistency_nonbreaking_contract() -> None:
	rt = RuntimeManager()
	source_run_id = "run-component-replay-consistency"
	graph = _graph()
	bindings = {
		"component_1": {"currentExecKey": "exec-comp", "currentArtifactId": "a-comp"},
		"n2": {"currentExecKey": "exec-n2", "currentArtifactId": "a-n2"},
	}
	basis = _build_frontier_identity_basis(
		graph=graph,
		graph_id="graph-component-replay",
		node_ids=["n2"],
		node_bindings=bindings,
		execution_version="v1",
	)
	contract = {"contractVersion": 1, "graphId": "graph-component-replay", "basis": basis}

	handle = rt.create_run(source_run_id)
	handle.status = "succeeded"
	handle.graph_id = "graph-component-replay"
	handle.graph = graph
	handle.node_bindings = dict(bindings)
	handle.execution_contract = dict(contract)

	captured: Dict[str, Any] = {}

	async def _fake_start_run(run_id, graph_payload, run_from, run_mode=None, graph_id=None, resume_snapshot=None):
		captured["run_id"] = run_id
		captured["graph_id"] = graph_id

	rt.start_run = _fake_start_run  # type: ignore[method-assign]
	result = await rt.request_replay(source_run_id=source_run_id)
	assert result["replayed"] is True
	assert str(captured.get("graph_id") or "") == "graph-component-replay"

