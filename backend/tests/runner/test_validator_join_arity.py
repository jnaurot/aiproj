from __future__ import annotations

from app.runner.validator import GraphValidator


def _node(node_id: str, kind: str, params: dict) -> dict:
	return {
		"id": node_id,
		"data": {
			"kind": kind,
			"label": node_id,
			"params": params,
		},
	}


def test_join_arity_counts_only_work_edges() -> None:
	graph = {
		"nodes": [
			_node("src_a", "source", {}),
			_node("src_b", "source", {}),
			_node("join_1", "transform", {"op": "join", "join": {"clauses": [{"leftNodeId": "src_a", "leftCol": "id", "rightNodeId": "src_b", "rightCol": "id", "how": "inner"}]}}),
		],
		"edges": [
			{"id": "e_work", "source": "src_a", "target": "join_1", "data": {"mode": "work"}},
			{"id": "e_param", "source": "src_b", "target": "join_1", "data": {"mode": "param"}},
		],
	}
	result = GraphValidator()._validate_transform_join_arity(graph)
	assert len(result) == 1
	assert result[0].code == "TRANSFORM_JOIN_INPUT_ARITY"


def test_join_arity_allows_two_or_more_work_edges() -> None:
	graph = {
		"nodes": [
			_node("src_a", "source", {}),
			_node("src_b", "source", {}),
			_node("src_c", "source", {}),
			_node("join_1", "transform", {"op": "join", "join": {"clauses": [{"leftNodeId": "src_a", "leftCol": "id", "rightNodeId": "src_b", "rightCol": "id", "how": "inner"}]}}),
		],
		"edges": [
			{"id": "e1", "source": "src_a", "target": "join_1", "data": {"mode": "work"}},
			{"id": "e2", "source": "src_b", "target": "join_1", "data": {"mode": "work"}},
			{"id": "e3", "source": "src_c", "target": "join_1", "data": {"mode": "work"}},
		],
	}
	result = GraphValidator()._validate_transform_join_arity(graph)
	assert result == []

