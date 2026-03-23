from __future__ import annotations

from app.runner.validator import GraphValidator


def test_adapter_suggestion_builder_returns_actionable_guidance() -> None:
	validator = GraphValidator()
	suggestions = validator._adapter_suggestions(  # type: ignore[attr-defined]
		"text",
		"table",
		{"id": "n_dst", "data": {"kind": "transform", "label": "Transform"}},
	)
	assert suggestions, "expected adapter guidance suggestions"
	assert any("text_to_table" in str(hint).lower() for hint in suggestions)
