from __future__ import annotations

from app.runner.validator import GraphValidator


def _node(node_id: str, kind: str) -> dict:
	return {"id": node_id, "data": {"kind": kind, "label": node_id, "params": {}}}


def test_control_edge_bypasses_payload_mismatch_validation() -> None:
	graph = {
		"nodes": [_node("src", "source"), _node("dst", "model")],
		"edges": [
			{
				"id": "e_control_payload",
				"source": "src",
				"sourceHandle": "control_out",
				"target": "dst",
				"targetHandle": "control_in",
				"data": {
					"mode": "control",
					"contract": {"payload": {"source": {"type": "text"}, "target": {"type": "json"}}},
				},
			}
		],
	}
	result = GraphValidator().validate_pre_execution(graph)
	assert not any(error.code == "TYPE_MISMATCH" for error in result.errors)
	assert not any(error.code == "PAYLOAD_SCHEMA_MISMATCH" for error in result.errors)


def test_control_edge_requires_control_affinity_handles() -> None:
	graph = {
		"nodes": [_node("src", "source"), _node("dst", "model")],
		"edges": [
			{
				"id": "e_control_bad_handles",
				"source": "src",
				"sourceHandle": "out",
				"target": "dst",
				"targetHandle": "in",
				"data": {
					"mode": "control",
					"contract": {"payload": {"source": {"type": "text"}, "target": {"type": "json"}}},
				},
			}
		],
	}
	result = GraphValidator().validate_pre_execution(graph)
	assert any(error.code == "EDGE_MODE_INCOMPATIBLE" for error in result.errors)
