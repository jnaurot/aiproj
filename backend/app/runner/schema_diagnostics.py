from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Set


def _contract_path() -> Path:
	return Path(__file__).resolve().parents[3] / "shared" / "schema_diagnostics.v1.json"


def load_schema_diagnostics_contract() -> Dict[str, Any]:
	path = _contract_path()
	with path.open("r", encoding="utf-8") as fh:
		raw = json.load(fh)
	if not isinstance(raw, dict):
		raise ValueError("schema diagnostics contract must be an object")
	return raw


def load_schema_diagnostic_codes() -> Set[str]:
	contract = load_schema_diagnostics_contract()
	codes = contract.get("codes")
	if not isinstance(codes, list):
		raise ValueError("schema diagnostics contract must include a codes list")
	return {str(code) for code in codes if str(code).strip()}


SCHEMA_DIAGNOSTIC_CODES = load_schema_diagnostic_codes()
TYPE_MISMATCH = "TYPE_MISMATCH"
PAYLOAD_SCHEMA_MISMATCH = "PAYLOAD_SCHEMA_MISMATCH"
