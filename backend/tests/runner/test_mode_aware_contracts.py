from __future__ import annotations

from app.runner.validator import GraphValidator


def _node(node_id: str, kind: str) -> dict:
	return {"id": node_id, "data": {"kind": kind, "label": node_id, "params": {}}}


def _edge(mode: str, *, source_handle: str = "out", target_handle: str = "in") -> dict:
	return {
		"id": f"e_{mode}",
		"source": "src",
		"sourceHandle": source_handle,
		"target": "dst",
		"targetHandle": target_handle,
		"data": {
			"mode": mode,
			"contract": {
				"payload": {
					"source": {"type": "text"},
					"target": {"type": "json"},
				}
			},
		},
	}


def test_work_mode_enforces_payload_type_compatibility() -> None:
	graph = {"nodes": [_node("src", "source"), _node("dst", "tool")], "edges": [_edge("work")]}
	result = GraphValidator().validate_pre_execution(graph)
	assert any(err.code == "TYPE_MISMATCH" for err in result.errors)


def test_param_mode_skips_payload_type_mismatch() -> None:
	graph = {
		"nodes": [_node("src", "source"), _node("dst", "model")],
		"edges": [_edge("param", target_handle="param_filters")],
	}
	result = GraphValidator().validate_pre_execution(graph)
	assert not any(err.code == "TYPE_MISMATCH" for err in result.errors)
	assert not any(err.code == "PAYLOAD_SCHEMA_MISMATCH" for err in result.errors)


def test_control_mode_skips_payload_type_mismatch() -> None:
	graph = {
		"nodes": [_node("src", "source"), _node("dst", "model")],
		"edges": [_edge("control", source_handle="control_out", target_handle="control_in")],
	}
	result = GraphValidator().validate_pre_execution(graph)
	assert not any(err.code == "TYPE_MISMATCH" for err in result.errors)
	assert not any(err.code == "PAYLOAD_SCHEMA_MISMATCH" for err in result.errors)
