from __future__ import annotations

import json
from typing import Any, Dict, Tuple

from app.runner.schemas import LLMParams


def evaluate_model_output_gate(params: LLMParams, output_mode: str, raw_data: str) -> Tuple[bool, str]:
	cfg = params.eval_gate if isinstance(params.eval_gate, dict) else {}
	if not bool(cfg.get("enabled", False)):
		return True, "eval_gate disabled"
	min_output_chars = int(cfg.get("min_output_chars") or 1)
	fail_on_warnings = bool(cfg.get("fail_on_warnings", False))
	required_substring = str(cfg.get("required_substring") or "").strip()
	required_json_keys = cfg.get("required_json_keys")
	required_json_keys = required_json_keys if isinstance(required_json_keys, list) else []

	text = str(raw_data or "")
	if len(text) < min_output_chars:
		return False, f"output length {len(text)} is below min_output_chars={min_output_chars}"
	if required_substring and required_substring not in text:
		return False, f"required_substring='{required_substring}' not found"
	if str(output_mode or "").strip().lower() in {"json", "embeddings"}:
		try:
			obj = json.loads(text) if text else {}
		except Exception:
			return False, "eval_gate requires valid JSON for json/embeddings output"
		if fail_on_warnings and isinstance(obj, dict):
			warnings = obj.get("_warnings")
			if isinstance(warnings, list) and len(warnings) > 0:
				return False, f"warnings present ({len(warnings)}) while fail_on_warnings=true"
		if required_json_keys:
			if not isinstance(obj, dict):
				return False, "required_json_keys requires object payload"
			missing = [str(k) for k in required_json_keys if str(k) not in obj]
			if missing:
				return False, f"missing required_json_keys: {missing}"
	return True, "eval_gate passed"
