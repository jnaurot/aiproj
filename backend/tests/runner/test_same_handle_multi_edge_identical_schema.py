from __future__ import annotations

from app.runner.validator import GraphValidator


def _base_graph() -> dict:
	return {
		"nodes": [
			{
				"id": "src_a",
				"data": {
					"kind": "source",
					"sourceKind": "file",
					"schema": {
						"expectedSchema": {
							"source": "declared",
							"typedSchema": {"type": "table", "fields": []},
						}
					},
				},
			},
			{
				"id": "src_b",
				"data": {
					"kind": "source",
					"sourceKind": "file",
					"schema": {
						"expectedSchema": {
							"source": "declared",
							"typedSchema": {"type": "table", "fields": []},
						}
					},
				},
			},
			{
				"id": "dst",
				"data": {
					"kind": "transform",
					"transformKind": "filter",
					"params": {"op": "filter", "filter": {"expr": ""}},
					"schema": {
						"expectedInputSchemas": {
							"in": {
								"source": "declared",
								"typedSchema": {"type": "table", "fields": []},
							}
						}
					},
				},
			},
		],
		"edges": [],
	}


def test_multi_edge_same_handle_requires_identical_provided_schema() -> None:
	graph = _base_graph()
	graph["edges"] = [
		{
			"id": "e_a",
			"source": "src_a",
			"target": "dst",
			"sourceHandle": "out",
			"targetHandle": "in",
			"data": {
				"mode": "work",
				"contract": {
					"payload": {
						"source": {"type": "table", "columns": ["a"]},
						"target": {"type": "table"},
					}
				},
			},
		},
		{
			"id": "e_b",
			"source": "src_b",
			"target": "dst",
			"sourceHandle": "out",
			"targetHandle": "in",
			"data": {
				"mode": "work",
				"contract": {
					"payload": {
						"source": {"type": "table", "columns": ["b"]},
						"target": {"type": "table"},
					}
				},
			},
		},
	]

	result = GraphValidator().validate_pre_execution(graph)
	assert result.valid is False
	conflicts = [e for e in result.errors if e.message.startswith("Work payload mismatch: multiple inbound edges")]
	assert conflicts
	assert conflicts[0].details is not None
	assert conflicts[0].details.get("targetNodeId") == "dst"
	assert conflicts[0].details.get("targetHandle") == "in"


def test_multi_edge_same_handle_allows_identical_provided_schema() -> None:
	graph = _base_graph()
	graph["edges"] = [
		{
			"id": "e_a",
			"source": "src_a",
			"target": "dst",
			"sourceHandle": "out",
			"targetHandle": "in",
			"data": {
				"mode": "work",
				"contract": {
					"payload": {
						"source": {"type": "table", "columns": ["a"]},
						"target": {"type": "table"},
					}
				},
			},
		},
		{
			"id": "e_b",
			"source": "src_b",
			"target": "dst",
			"sourceHandle": "out",
			"targetHandle": "in",
			"data": {
				"mode": "work",
				"contract": {
					"payload": {
						"source": {"type": "table", "columns": ["a"]},
						"target": {"type": "table"},
					}
				},
			},
		},
	]

	result = GraphValidator().validate_pre_execution(graph)
	assert [e for e in result.errors if e.message.startswith("Work payload mismatch: multiple inbound edges")] == []
