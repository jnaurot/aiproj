from __future__ import annotations

from app.runner.validator import GraphValidator


def _tool_node(node_id: str) -> dict:
	return {
		"id": node_id,
		"data": {
			"kind": "tool",
			"label": node_id,
			"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
		},
	}


def test_control_link_cycle_is_rejected() -> None:
	graph = {
		"nodes": [_tool_node("a"), _tool_node("b")],
		"edges": [
			{
				"id": "e_ab",
				"source": "a",
				"sourceHandle": "control_out",
				"target": "b",
				"targetHandle": "control_in",
				"data": {"mode": "control", "linkKind": "control_link"},
			},
			{
				"id": "e_ba",
				"source": "b",
				"sourceHandle": "control_out",
				"target": "a",
				"targetHandle": "control_in",
				"data": {"mode": "control", "linkKind": "control_link"},
			},
		],
	}
	result = GraphValidator().validate_pre_execution(graph)
	codes = {str(err.code) for err in result.errors}
	assert "CONTROL_LINK_CYCLE" in codes


def test_control_link_conflict_on_same_handle_is_rejected() -> None:
	graph = {
		"nodes": [_tool_node("ctl"), _tool_node("legacy"), _tool_node("sink")],
		"edges": [
			{
				"id": "e_control_link",
				"source": "ctl",
				"sourceHandle": "control_out",
				"target": "sink",
				"targetHandle": "control_gate",
				"data": {"mode": "control", "linkKind": "control_link"},
			},
			{
				"id": "e_legacy_control",
				"source": "legacy",
				"sourceHandle": "control_out",
				"target": "sink",
				"targetHandle": "control_gate",
				"data": {"mode": "control", "linkKind": "data_link"},
			},
		],
	}
	result = GraphValidator().validate_pre_execution(graph)
	codes = {str(err.code) for err in result.errors}
	assert "CONTROL_LINK_CONFLICT" in codes


def test_control_link_without_work_inbound_is_flagged_deadlock_risk() -> None:
	graph = {
		"nodes": [_tool_node("ctl"), _tool_node("sink")],
		"edges": [
			{
				"id": "e_control_only",
				"source": "ctl",
				"sourceHandle": "control_out",
				"target": "sink",
				"targetHandle": "control_gate",
				"data": {"mode": "control", "linkKind": "control_link"},
			}
		],
	}
	result = GraphValidator().validate_pre_execution(graph)
	codes = {str(err.code) for err in result.errors}
	assert "CONTROL_LINK_DEADLOCK_RISK" in codes

