from __future__ import annotations

from app.runner.run import _node_processing_policy


def test_per_port_batch_and_single_item_overrides() -> None:
	node = {
		"data": {
			"kind": "transform",
			"processingPolicy": {
				"consume_mode": "once",
				"batch_size": 1,
				"max_inflight": 1,
				"input_handles": {
					"in": {"consume_mode": "batch", "batch_size": 4, "max_inflight": 2},
					"param_profile": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
				},
			},
		}
	}

	in_policy = _node_processing_policy(node, input_handle="in")
	assert in_policy["consume_mode"] == "batch"
	assert in_policy["batch_size"] == 4
	assert in_policy["max_inflight"] == 2

	param_policy = _node_processing_policy(node, input_handle="param_profile")
	assert param_policy["consume_mode"] == "single_item"
	assert param_policy["batch_size"] == 1
	assert param_policy["max_inflight"] == 1
