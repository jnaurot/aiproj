from __future__ import annotations

from app.runner.schema_diagnostics import SCHEMA_DIAGNOSTIC_CODES
from app.runner.validator import GraphValidator


def _node(node_id: str, *, kind: str, label: str, ports: dict, params: dict | None = None) -> dict:
	return {
		"id": node_id,
		"data": {
			"kind": kind,
			"label": label,
			"ports": ports,
			"params": params or {},
		},
	}


def test_schema_constraint_solver_emits_adapter_suggestion_for_type_mismatch() -> None:
	graph = {
		"nodes": [
			_node(
				"n_source",
				kind="source",
				label="Source",
				ports={"in": None, "out": "text"},
				params={"sourceKind": "file", "snapshot_id": "a" * 64, "file_format": "txt"},
			),
			_node(
				"n_transform",
				kind="transform",
				label="Transform",
				ports={"in": "table", "out": "table"},
				params={"op": "filter", "filter": {"expr": ""}},
			),
		],
		"edges": [
			{
				"id": "e1",
				"source": "n_source",
				"target": "n_transform",
				"data": {
					"contract": {
						"payload": {
							"source": {"type": "text"},
							"target": {"type": "table"},
						}
					}
				},
			},
		],
	}

	result = GraphValidator().validate_pre_execution(graph)
	type_errors = [e for e in result.errors if e.code == "TYPE_MISMATCH"]
	assert type_errors, "expected TYPE_MISMATCH"
	assert "TYPE_MISMATCH" in SCHEMA_DIAGNOSTIC_CODES
	msg = type_errors[0].message
	assert "provided_schema=" in msg
	assert "required_schema=" in msg
	assert "Auto-adapter suggestion:" in msg
	assert "text_to_table" in msg


def test_schema_constraint_solver_emits_required_provided_payload_mismatch() -> None:
	graph = {
		"nodes": [
			_node(
				"n_source",
				kind="source",
				label="Source",
				ports={"in": None, "out": "table"},
				params={"sourceKind": "file", "snapshot_id": "b" * 64, "file_format": "csv"},
			),
			_node(
				"n_transform",
				kind="transform",
				label="Transform",
				ports={"in": "table", "out": "table"},
				params={"op": "select", "select": {"mode": "include", "columns": ["id"]}},
			),
		],
		"edges": [
			{
				"id": "e1",
				"source": "n_source",
				"target": "n_transform",
				"data": {
					"contract": {
						"payload": {
							"source": {"columns": ["id"]},
							"target": {"required_columns": ["id", "value"]},
						}
					}
				},
			},
		],
	}

	result = GraphValidator().validate_pre_execution(graph)
	payload_errors = [e for e in result.errors if e.code == "PAYLOAD_SCHEMA_MISMATCH"]
	assert payload_errors, "expected PAYLOAD_SCHEMA_MISMATCH"
	assert "PAYLOAD_SCHEMA_MISMATCH" in SCHEMA_DIAGNOSTIC_CODES
	msg = payload_errors[0].message
	assert "provided_schema=" in msg
	assert "required_schema=" in msg
	assert "value" in msg


def test_schema_constraint_solver_requires_typed_schema_coverage() -> None:
	graph = {
		"nodes": [
			_node(
				"n_source",
				kind="source",
				label="Source",
				ports={"in": None, "out": "table"},
				params={"sourceKind": "file", "snapshot_id": "c" * 64, "file_format": "csv"},
			),
			_node(
				"n_transform",
				kind="transform",
				label="Transform",
				ports={"in": "table", "out": "table"},
				params={"op": "select", "select": {"mode": "include", "columns": ["id"]}},
			),
		],
		"edges": [
			{
				"id": "e1",
				"source": "n_source",
				"target": "n_transform",
				"data": {
					"contract": {
						"payload": {
							"source": {"type": "table", "columns": []},
							"target": {"type": "table", "required_columns": ["id"]},
						}
					}
				},
			},
		],
	}

	result = GraphValidator().validate_pre_execution(graph)
	typed_schema_errors = [e for e in result.errors if e.code == "CONTRACT_EDGE_TYPED_SCHEMA_MISSING"]
	assert typed_schema_errors, "expected CONTRACT_EDGE_TYPED_SCHEMA_MISSING"
	details = typed_schema_errors[0].details or {}
	assert details.get("expected", {}).get("typedSchema", {}).get("fields") == "non-empty"
	assert details.get("actual", {}).get("typedSchema", {}).get("fields") == []
