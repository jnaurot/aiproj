from __future__ import annotations

from app.runner.run import _resolve_retry_policy


def test_retry_policy_inheritance_graph_node_op() -> None:
	graph = {"retry": {"max_attempts": 2, "backoff_seconds": 0.5}}
	node = {
		"id": "n1",
		"data": {
			"kind": "transform",
			"params": {
				"retry": {"max_attempts": 3},
				"filter": {"expr": "x > 1", "retry": {"max_attempts": 4, "jitter_seconds": 0.2}},
			},
		},
	}
	effective = _resolve_retry_policy(graph=graph, node=node, op_name="filter")
	assert effective["max_attempts"] == 4
	assert effective["backoff_seconds"] == 0.5
	assert effective["jitter_seconds"] == 0.2


def test_retry_policy_defaults_when_invalid() -> None:
	graph = {"retry": {"max_attempts": "x", "backoff_seconds": -10}}
	node = {"id": "n2", "data": {"kind": "tool", "params": {"retry": {"jitter_seconds": "bad"}}}}
	effective = _resolve_retry_policy(graph=graph, node=node, op_name=None)
	assert effective["max_attempts"] == 1
	assert effective["backoff_seconds"] == 0.0
	assert effective["jitter_seconds"] == 0.0
