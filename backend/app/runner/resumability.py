from __future__ import annotations

from typing import Any, Dict, Final, Literal

ResumabilityClass = Literal[
	"non_resumable",
	"safe_boundary_resumable",
	"advanced_interrupt_resumable",
]


NODE_KIND_RESUMABILITY: Final[Dict[str, ResumabilityClass]] = {
	"source": "safe_boundary_resumable",
	"transform": "safe_boundary_resumable",
	"model": "safe_boundary_resumable",
	"llm": "safe_boundary_resumable",
	"component": "safe_boundary_resumable",
	"tool": "non_resumable",
}


def _tool_side_effect_mode(params: Dict[str, Any]) -> str:
	mode = str(params.get("side_effect_mode") or "effectful").strip().lower()
	if mode not in {"pure", "idempotent", "effectful"}:
		return "effectful"
	return mode


def classify_node_resumability(node: Dict[str, Any]) -> ResumabilityClass:
	data = (node.get("data") or {}) if isinstance(node, dict) else {}
	kind = str(data.get("kind") or "").strip().lower()
	declared = NODE_KIND_RESUMABILITY.get(kind)
	if declared is None:
		raise ValueError(f"RESUMABILITY_DECLARATION_MISSING:{kind or '(empty)'}")

	params = (data.get("params") or {}) if isinstance(data.get("params"), dict) else {}
	explicit = str(
		data.get("resumability")
		or params.get("resumability")
		or params.get("resume_class")
		or ""
	).strip().lower()
	if explicit in {"non_resumable", "safe_boundary_resumable", "advanced_interrupt_resumable"}:
		return explicit  # type: ignore[return-value]

	if kind == "tool":
		# Fail closed for tools unless explicitly safe/idempotent.
		side_effect_mode = _tool_side_effect_mode(params)
		if side_effect_mode in {"pure", "idempotent"}:
			return "safe_boundary_resumable"
		return "non_resumable"
	return declared

