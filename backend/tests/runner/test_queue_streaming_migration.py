from __future__ import annotations

from app.graph_migrations import canonicalize_graph_payload


def test_canonicalize_graph_adds_streaming_and_processing_policy_defaults() -> None:
	graph = {
		"nodes": [
			{"id": "n1", "data": {"kind": "source", "params": {}}},
			{"id": "n2", "data": {"kind": "transform", "params": {}}},
		],
		"edges": [{"id": "e1", "source": "n1", "target": "n2"}],
	}
	canonical, _notes = canonicalize_graph_payload(graph)
	nodes = {str(n.get("id")): n for n in canonical.get("nodes", [])}
	edge = canonical.get("edges", [])[0]
	assert (nodes["n2"].get("data") or {}).get("processingPolicy", {}).get("consume_mode") == "once"
	assert (edge.get("data") or {}).get("work", {}).get("item_mode") == "artifact"
	assert int((edge.get("data") or {}).get("work", {}).get("max_items") or 0) >= 1
