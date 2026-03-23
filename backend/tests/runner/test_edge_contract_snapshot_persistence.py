from __future__ import annotations

from app.graph_migrations import canonicalize_graph_payload


def test_edge_contract_snapshot_is_materialized_during_canonicalization() -> None:
	graph, _notes = canonicalize_graph_payload(
		{
			"nodes": [
				{"id": "src", "data": {"kind": "source", "params": {}}},
				{"id": "dst", "data": {"kind": "model", "params": {}}},
			],
			"edges": [
				{
					"id": "e1",
					"source": "src",
					"target": "dst",
					"data": {
						"mode": "work",
						"contract": {
							"out": "json",
							"in": "text",
							"payload": {"source": {"type": "json"}, "target": {"type": "string"}}
						},
					},
				}
			],
		}
	)
	edge = graph["edges"][0]
	snapshot = (((edge.get("data") or {}).get("contract") or {}).get("snapshot") or {})
	assert str(snapshot.get("sourceSchemaFingerprint") or "")
	assert str(snapshot.get("targetSchemaFingerprint") or "")
	assert snapshot.get("compatible") is True
	assert str(snapshot.get("decision") or "") in {"native", "coerced", "adapter", "incompatible"}

