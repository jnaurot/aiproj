from __future__ import annotations

from app.runner.validator import GraphValidator


def _node(node_id: str, kind: str) -> dict:
	return {"id": node_id, "data": {"kind": kind, "label": node_id, "params": {}}}


def test_edge_mode_work_to_param_rejected() -> None:
	graph = {
		"nodes": [_node("src", "source"), _node("dst", "model")],
		"edges": [
			{
				"id": "e1",
				"source": "src",
				"sourceHandle": "out",
				"target": "dst",
				"targetHandle": "param_filters",
				"data": {"mode": "work", "contract": {"payload": {"source": {"type": "text"}, "target": {"type": "text"}}}},
			}
		],
	}
	result = GraphValidator().validate_pre_execution(graph)
	assert any(e.code == "EDGE_MODE_INCOMPATIBLE" for e in result.errors)


def test_edge_mode_param_to_param_allowed() -> None:
	graph = {
		"nodes": [_node("src", "source"), _node("dst", "model")],
		"edges": [
			{
				"id": "e1",
				"source": "src",
				"sourceHandle": "out",
				"target": "dst",
				"targetHandle": "param_filters",
				"data": {"mode": "param", "contract": {"payload": {"source": {"type": "text"}, "target": {"type": "text"}}}},
			}
		],
	}
	result = GraphValidator().validate_pre_execution(graph)
	assert not any(e.code == "EDGE_MODE_INCOMPATIBLE" for e in result.errors)
