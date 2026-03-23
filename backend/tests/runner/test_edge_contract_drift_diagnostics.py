from __future__ import annotations

from app.runner.validator import GraphValidator


def test_validator_emits_edge_contract_drift_warning() -> None:
	validator = GraphValidator()
	graph = {
		"nodes": [
			{"id": "src", "data": {"kind": "source", "params": {"output": {"mode": "json"}}}},
			{
				"id": "dst",
				"data": {
					"kind": "model",
					"schema": {"expectedInputSchemas": {"in": {"typedSchema": {"type": "json", "fields": []}}}},
					"params": {"model": "stub"},
				},
			},
		],
		"edges": [
			{
				"id": "e_drift",
				"source": "src",
				"target": "dst",
				"sourceHandle": "out",
				"targetHandle": "in",
				"data": {
					"mode": "work",
					"contract": {
						"payload": {"source": {"type": "json"}, "target": {"type": "json"}},
						"snapshot": {
							"sourceSchemaFingerprint": "{\"type\":\"text\"}",
							"targetSchemaFingerprint": "{\"type\":\"text\"}",
							"compatible": True,
							"decision": "native",
						},
					},
				},
			}
		],
	}
	result = validator.validate_pre_execution(graph)
	drift = [w for w in result.warnings if w.code == "EDGE_CONTRACT_DRIFT"]
	assert drift, "expected EDGE_CONTRACT_DRIFT warning"
	assert drift[0].edge_id == "e_drift"
	assert "currentSourceSchemaFingerprint" in (drift[0].details or {})
	assert "snapshotSourceSchemaFingerprint" in (drift[0].details or {})

