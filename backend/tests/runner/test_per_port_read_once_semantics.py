from __future__ import annotations

from app.runner.run import _node_processing_policy


def test_read_once_and_continuous_aliases_normalize_to_runtime_modes() -> None:
	node = {
		"data": {
			"kind": "model",
			"processingPolicy": {
				"consume_mode": "read_once",
				"batch_size": 1,
				"max_inflight": 1,
				"input_handles": {
					"in": {"consume_mode": "continuous", "batch_size": 2, "max_inflight": 1}
				},
			},
		}
	}

	default_policy = _node_processing_policy(node)
	assert default_policy["consume_mode"] == "once"

	handle_policy = _node_processing_policy(node, input_handle="in")
	assert handle_policy["consume_mode"] == "single_item"
	assert handle_policy["batch_size"] == 2


def test_read_once_string_false_does_not_force_once() -> None:
	node = {
		"data": {
			"kind": "transform",
			"processingPolicy": {
				"consume_mode": "single_item",
				"read_once": "false",
				"batch_size": 1,
				"max_inflight": 1,
			},
		}
	}

	policy = _node_processing_policy(node)
	assert policy["read_once"] is False
	assert policy["consume_mode"] == "single_item"


def test_handle_read_once_string_false_does_not_force_once() -> None:
	node = {
		"data": {
			"kind": "transform",
			"processingPolicy": {
				"consume_mode": "single_item",
				"input_handles": {
					"in": {
						"consume_mode": "single_item",
						"readOnce": "false",
						"batch_size": 1,
						"max_inflight": 1,
					}
				},
			},
		}
	}

	policy = _node_processing_policy(node, input_handle="in")
	assert policy["read_once"] is False
	assert policy["consume_mode"] == "single_item"
