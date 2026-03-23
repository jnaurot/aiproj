from __future__ import annotations

from app.graph_migrations import canonicalize_graph_payload


def test_legacy_processing_policy_aliases_are_migrated_preserving_behavior() -> None:
	graph = {
		"version": 1,
		"nodes": [
			{
				"id": "n1",
				"type": "transform",
				"position": {"x": 0, "y": 0},
				"data": {
					"kind": "transform",
					"params": {"op": "filter", "filter": {"expr": ""}},
					"processingPolicy": {
						"consumeMode": "read_once",
						"batchSize": 2,
						"maxInflight": 3,
						"input_handles": {
							"in": {"consumeMode": "continuous", "batchSize": 4, "maxInflight": 5}
						},
					},
				},
			}
		],
		"edges": [],
	}

	canonical, _notes = canonicalize_graph_payload(graph)
	node = (canonical.get("nodes") or [])[0]
	policy = ((node.get("data") or {}).get("processingPolicy") or {})
	assert policy.get("consume_mode") == "once"
	assert policy.get("batch_size") == 2
	assert policy.get("max_inflight") == 3
	handle_policy = (policy.get("input_handles") or {}).get("in") or {}
	assert handle_policy.get("consume_mode") == "single_item"
	assert handle_policy.get("batch_size") == 4
	assert handle_policy.get("max_inflight") == 5


def test_missing_processing_policy_defaults_match_legacy_behavior() -> None:
	graph = {
		"version": 1,
		"nodes": [
			{
				"id": "n1",
				"type": "tool",
				"position": {"x": 0, "y": 0},
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}},
			}
		],
		"edges": [],
	}

	canonical, _notes = canonicalize_graph_payload(graph)
	node = (canonical.get("nodes") or [])[0]
	policy = ((node.get("data") or {}).get("processingPolicy") or {})
	assert policy.get("consume_mode") == "once"
	assert policy.get("batch_size") == 1
	assert policy.get("max_inflight") == 1
	assert policy.get("input_handles") == {}
