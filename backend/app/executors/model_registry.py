from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

from app.runner.schemas import LLMParams


def _safe_profile_key(ref: str) -> str:
	key = re.sub(r"[^A-Za-z0-9_]", "_", str(ref or "").strip())
	return key.upper()


def _env_json_dict(env_key: str) -> Optional[Dict[str, Any]]:
	raw = os.getenv(env_key)
	if not raw:
		return None
	try:
		obj = json.loads(raw)
	except Exception:
		return None
	if isinstance(obj, dict):
		return obj
	return None


def _resolve_secret_ref(value: Optional[str]) -> Optional[str]:
	ref = str(value or "").strip()
	if not ref:
		return None
	env_val = os.getenv(ref)
	if env_val:
		return env_val
	return ref


@dataclass(frozen=True)
class ResolvedModelConnection:
	base_url: str
	api_key: Optional[str]
	profile_id: Optional[str]
	resolved_from: str


def resolve_model_connection(params: LLMParams, provider: str) -> ResolvedModelConnection:
	profile_id = str(params.connection_ref or "").strip() or None
	base_url = str(params.base_url or "").strip()
	api_key = _resolve_secret_ref(params.api_key_ref)
	resolved_from = "params"

	if profile_id:
		registry = _env_json_dict("MODEL_CONNECTION_PROFILES_JSON") or {}
		profile_obj = registry.get(profile_id) if isinstance(registry, dict) else None
		if not isinstance(profile_obj, dict):
			profile_obj = _env_json_dict(f"MODEL_CONN_{_safe_profile_key(profile_id)}")
		if not isinstance(profile_obj, dict):
			raise ValueError(f"MISSING_SECRET: connection_ref '{profile_id}' is not set")

		base_url = str(
			profile_obj.get("base_url")
			or profile_obj.get("baseUrl")
			or base_url
			or ""
		).strip()
		api_key = (
			_resolve_secret_ref(str(profile_obj.get("api_key_ref") or "").strip())
			or str(profile_obj.get("api_key") or "").strip()
			or api_key
		)
		resolved_from = "connection_ref"

	if not base_url:
		raise ValueError("MISSING_SECRET: resolved model connection is missing base_url")
	return ResolvedModelConnection(
		base_url=base_url,
		api_key=api_key if api_key else None,
		profile_id=profile_id,
		resolved_from=resolved_from,
	)
