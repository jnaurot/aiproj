from __future__ import annotations

from app.graph_migrations import canonicalize_graph_payload


def test_filter_ambiguous_dual_mode_defaults_to_sql_with_warning() -> None:
	graph, notes = canonicalize_graph_payload(
		{
			"nodes": [
				{
					"id": "n_filter",
					"data": {
						"kind": "transform",
						"params": {
							"op": "filter",
							"filter": {
								"expr": '"salary" > 40000',
								"rules": {
									"kind": "group",
									"op": "all",
									"conditions": [{"kind": "condition", "column": "job_type", "op": "eq", "value": "full_time"}],
								},
							},
						},
					},
				}
			],
			"edges": [],
		}
	)
	node = (graph.get("nodes") or [])[0]
	assert ((node.get("data") or {}).get("params") or {}).get("filter", {}).get("mode") == "sql"
	assert any(note.get("code") == "TRANSFORM_FILTER_MODE_AMBIGUOUS_RESOLVED" for note in notes)


def test_derive_ambiguous_dual_mode_defaults_to_sql_with_warning() -> None:
	graph, notes = canonicalize_graph_payload(
		{
			"nodes": [
				{
					"id": "n_derive",
					"data": {
						"kind": "transform",
						"params": {
							"op": "derive",
							"derive": {
								"columns": [{"name": "salary_x2", "expr": '"salary" * 2'}],
								"rules": [
									{"name": "salary_plus_bonus", "formula": {"op": "add", "args": [{"column": "salary"}, 5000]}}
								],
							},
						},
					},
				}
			],
			"edges": [],
		}
	)
	node = (graph.get("nodes") or [])[0]
	assert ((node.get("data") or {}).get("params") or {}).get("derive", {}).get("mode") == "sql"
	assert any(note.get("code") == "TRANSFORM_DERIVE_MODE_AMBIGUOUS_RESOLVED" for note in notes)


def test_explicit_filter_mode_is_preserved_without_ambiguous_warning() -> None:
	graph, notes = canonicalize_graph_payload(
		{
			"nodes": [
				{
					"id": "n_filter_mode",
					"data": {
						"kind": "transform",
						"params": {
							"op": "filter",
							"filter": {
								"mode": "rules",
								"expr": '"salary" > 40000',
								"rules": {
									"kind": "group",
									"op": "all",
									"conditions": [{"kind": "condition", "column": "job_type", "op": "eq", "value": "full_time"}],
								},
							},
						},
					},
				}
			],
			"edges": [],
		}
	)
	node = (graph.get("nodes") or [])[0]
	assert ((node.get("data") or {}).get("params") or {}).get("filter", {}).get("mode") == "rules"
	assert not any(note.get("code") == "TRANSFORM_FILTER_MODE_AMBIGUOUS_RESOLVED" for note in notes)

