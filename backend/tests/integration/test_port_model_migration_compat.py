from __future__ import annotations

from app.graph_migrations import canonicalize_graph_payload


def test_migration_v2_materializes_ports_snapshots_and_processing_policy_defaults() -> None:
	graph, notes = canonicalize_graph_payload(
		{
			"nodes": [
				{
					"id": "src",
					"data": {
						"kind": "source",
						"processingPolicy": {"consumeMode": "read_once", "batchSize": 2},
						"schema": {"expectedInputSchema": {"typedSchema": {"type": "text", "fields": []}}},
					},
				},
				{"id": "dst", "data": {"kind": "model", "params": {"model": "stub"}}},
			],
			"edges": [
				{
					"id": "e_mig",
					"source": "src",
					"target": "dst",
					"data": {
						"contract": {
							"payload": {"source": {"type": "text"}, "target": {"type": "text"}}
						}
					},
				}
			],
		}
	)
	src = next(node for node in graph["nodes"] if node.get("id") == "src")
	policy = ((src.get("data") or {}).get("processingPolicy") or {})
	assert policy.get("consume_mode") == "once"
	assert bool(policy.get("read_once")) is True
	assert isinstance((((src.get("data") or {}).get("portDeclarations") or {}).get("in")), dict)
	edge = graph["edges"][0]
	snapshot = ((((edge.get("data") or {}).get("contract") or {}).get("snapshot") or {}))
	assert str(snapshot.get("sourceSchemaFingerprint") or "")
	assert str(snapshot.get("targetSchemaFingerprint") or "")
	assert any(str(note.get("code") or "").startswith("NODE_") for note in notes)
