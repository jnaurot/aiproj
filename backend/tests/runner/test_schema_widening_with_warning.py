from __future__ import annotations

from app.runner.validator import GraphValidator


def _graph(
	*,
	source_type: str,
	target_type: str,
	target_policy: str | None = None,
) -> dict:
	target_params = {"op": "filter", "filter": {"expr": ""}}
	if target_policy:
		target_params["coercion_policy"] = target_policy
	return {
		"nodes": [
			{
				"id": "src",
				"data": {
					"kind": "transform",
					"label": "src",
					"params": {"op": "filter", "filter": {"expr": ""}},
					"schema": {
						"expectedSchema": {
							"source": "declared",
							"typedSchema": {"type": source_type, "fields": []},
						}
					},
				},
			},
			{
				"id": "dst",
				"data": {
					"kind": "transform",
					"label": "dst",
					"params": target_params,
					"schema": {
						"expectedInputSchemas": {
							"in": {"source": "declared", "typedSchema": {"type": target_type, "fields": []}}
						}
					},
				},
			},
		],
		"edges": [
			{
				"id": "e1",
				"source": "src",
				"sourceHandle": "out",
				"target": "dst",
				"targetHandle": "in",
				"data": {"mode": "work", "contract": {"payload": {"source": {"type": source_type}, "target": {"type": target_type}}}},
			}
		],
	}


def test_safe_widening_is_allowed_without_warning() -> None:
	result = GraphValidator().validate_pre_execution(
		_graph(source_type="json", target_type="table", target_policy="safe_widening")
	)
	assert result.errors == []
	assert result.warnings == []


def test_lossy_coercion_is_allowed_with_warning_when_policy_allows() -> None:
	result = GraphValidator().validate_pre_execution(
		_graph(source_type="json", target_type="text", target_policy="allow_lossy")
	)
	assert result.errors == []
	assert any(w.code == "TYPE_COERCION_WARNING" for w in result.warnings)


def test_strict_policy_blocks_safe_widening() -> None:
	result = GraphValidator().validate_pre_execution(
		_graph(source_type="json", target_type="table", target_policy="strict")
	)
	assert any(e.code == "TYPE_MISMATCH" for e in result.errors)
