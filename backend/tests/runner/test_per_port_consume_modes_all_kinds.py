from __future__ import annotations

from app.runner.run import _node_processing_policy


def _node(kind: str) -> dict:
	return {
		"data": {
			"kind": kind,
			"processingPolicy": {
				"consume_mode": "once",
				"batch_size": 1,
				"max_inflight": 1,
				"input_handles": {
					"in": {"consume_mode": "batch", "batch_size": 3, "max_inflight": 2}
				},
			},
		}
	}


def test_per_port_policy_applies_across_all_node_kinds() -> None:
	for kind in ("source", "transform", "model", "llm", "tool"):
		node = _node(kind)
		policy = _node_processing_policy(node, input_handle="in")
		assert policy["consume_mode"] == "batch"
		assert policy["batch_size"] == 3
		assert policy["max_inflight"] == 2
