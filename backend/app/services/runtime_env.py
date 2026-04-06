from __future__ import annotations

import json
import os
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

_ENV_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
_SENSITIVE_HINT_RE = re.compile(r"(TOKEN|SECRET|PASSWORD|API_KEY|KEY|CREDENTIAL|AUTH)", re.IGNORECASE)
_OVERRIDES_PATH = Path("./data/runtime/env_overrides.json")
_SCHEMA_VERSION = 1

_BASE_ENV: Dict[str, str] = dict(os.environ)
_LOCK = threading.RLock()
_LOADED = False
_OVERRIDES: Dict[str, str] = {}


@dataclass(frozen=True)
class EnvVarSpec:
	name: str
	description: str
	default: Optional[str] = None
	restart_required: bool = False
	sensitive: bool = False
	category: str = "runtime"


_SUPPORTED_ENV_SPECS: List[EnvVarSpec] = [
	EnvVarSpec("ARTIFACT_STORE", "Artifact storage backend (disk|memory).", "disk", restart_required=True, category="storage"),
	EnvVarSpec("ARTIFACT_DIR", "Artifact root directory when ARTIFACT_STORE=disk.", "./data/artifacts", restart_required=True, category="storage"),
	EnvVarSpec("ARTIFACT_RETENTION_MODE", "Artifact retention mode (off|by_run).", "by_run", category="storage"),
	EnvVarSpec("ARTIFACT_KEEP_RECENT_RUNS", "How many recent terminal runs to retain artifacts for.", "5", category="storage"),
	EnvVarSpec("ARTIFACT_RETENTION_INCLUDE_FAILED", "Include failed runs in run-scoped artifact retention.", "1", category="storage"),
	EnvVarSpec("ARTIFACT_RETENTION_INCLUDE_CANCELED", "Include canceled runs in run-scoped artifact retention.", "1", category="storage"),
	EnvVarSpec("ENABLE_MAINTENANCE_ENDPOINTS", "Enables maintenance endpoints.", "0", category="api"),
	EnvVarSpec("MODEL_REGISTRY_REQUIRE_ADMIN", "Require admin header for model registry mutations.", "0", category="api"),
	EnvVarSpec("NO_CUDA_GUARD_DISABLED", "Disable CUDA guard checks at startup.", "0", restart_required=True, category="runtime"),
	EnvVarSpec("SOURCE_INCREMENTAL_STATE_FILE", "Path for source incremental state file.", "./data/source_incremental_state.json", category="source"),
	EnvVarSpec("OBJECT_STORE_MOCK_ROOT", "Root path for object_store mock source mode.", "", category="source"),
	EnvVarSpec("MODEL_CONNECTION_PROFILES_JSON", "JSON mapping for model connection profiles.", "{}", sensitive=True, category="model"),
	EnvVarSpec("RUNNER_MAX_NODES", "Max nodes allowed per run.", "2000", category="scheduler"),
	EnvVarSpec("RUNNER_MAX_EDGES", "Max edges allowed per run (0 allows none).", "5000", category="scheduler"),
	EnvVarSpec("RUNNER_MAX_RUNTIME_MS", "Max run wall time in ms (0 disables).", "0", category="scheduler"),
	EnvVarSpec("RUNNER_MAX_CONCURRENCY", "Global max concurrent node execution.", "4", category="scheduler"),
	EnvVarSpec("RUNNER_MAX_SOURCE", "Max concurrent source nodes.", "2", category="scheduler"),
	EnvVarSpec("RUNNER_MAX_TRANSFORM", "Max concurrent transform nodes.", "2", category="scheduler"),
	EnvVarSpec("RUNNER_MAX_MODEL", "Max concurrent model/llm nodes.", "2", category="scheduler"),
	EnvVarSpec("RUNNER_MAX_LLM", "Legacy alias for RUNNER_MAX_MODEL.", "2", category="scheduler"),
	EnvVarSpec("RUNNER_MAX_TOOL", "Max concurrent tool nodes.", "2", category="scheduler"),
	EnvVarSpec(
		"CONTROL_PLANE_V1",
		"Control-plane cutover mode (off|observe|enforce).",
		"enforce",
		category="scheduler",
	),
	EnvVarSpec("RUNNER_ADAPTIVE_MODE", "Adaptive scheduler mode (off|observe|enforce).", "off", category="scheduler"),
	EnvVarSpec("RUNNER_ADAPTIVE_EVAL_INTERVAL_MS", "Adaptive scheduler evaluation interval in ms.", "500", category="scheduler"),
	EnvVarSpec("RUNNER_ADAPTIVE_COOLDOWN_MS", "Cooldown before applying another enforced adaptive cap change.", "1000", category="scheduler"),
	EnvVarSpec("RUNNER_ADAPTIVE_QUEUE_HIGH", "Queue depth threshold to reduce caps.", "32", category="scheduler"),
	EnvVarSpec("RUNNER_ADAPTIVE_QUEUE_LOW", "Queue depth threshold to increase caps.", "4", category="scheduler"),
	EnvVarSpec("RUNNER_ADAPTIVE_LATENCY_HIGH_MS", "Average runtime latency threshold to reduce caps.", "1200", category="scheduler"),
	EnvVarSpec("RUNNER_ADAPTIVE_FAILURE_HIGH", "Failure-rate threshold to reduce caps.", "0.25", category="scheduler"),
	EnvVarSpec("RUNNER_ADAPTIVE_LEASE_WAIT_HIGH_MS", "Model wait threshold to reduce caps.", "800", category="scheduler"),
	EnvVarSpec("RUNNER_ADAPTIVE_STEP_DOWN", "Per-decision decrement for adaptive caps under pressure.", "1", category="scheduler"),
	EnvVarSpec("RUNNER_ADAPTIVE_STEP_UP", "Per-decision increment for adaptive caps during recovery.", "1", category="scheduler"),
	EnvVarSpec("RUNNER_ADAPTIVE_MIN_GLOBAL", "Minimum global concurrency cap under adaptive enforcement.", "1", category="scheduler"),
	EnvVarSpec("RUNNER_ADAPTIVE_MIN_SOURCE", "Minimum source concurrency cap under adaptive enforcement.", "1", category="scheduler"),
	EnvVarSpec("RUNNER_ADAPTIVE_MIN_TRANSFORM", "Minimum transform concurrency cap under adaptive enforcement.", "1", category="scheduler"),
	EnvVarSpec("RUNNER_ADAPTIVE_MIN_MODEL", "Minimum model concurrency cap under adaptive enforcement.", "1", category="scheduler"),
	EnvVarSpec("RUNNER_ADAPTIVE_MIN_TOOL", "Minimum tool concurrency cap under adaptive enforcement.", "1", category="scheduler"),
	EnvVarSpec("RUNNER_MAX_MODEL_PROVIDER", "Default provider-specific model lease cap.", "", category="llm"),
	EnvVarSpec("RUNNER_MODEL_PROVIDER_ACQUIRE_TIMEOUT", "Default provider lease acquire timeout (seconds).", "", category="llm"),
	EnvVarSpec("RUNNER_MAX_MODEL_PROVIDER_*", "Provider-specific lease cap, e.g. RUNNER_MAX_MODEL_PROVIDER_OLLAMA.", "", category="llm"),
	EnvVarSpec(
		"RUNNER_MODEL_PROVIDER_ACQUIRE_TIMEOUT_*",
		"Provider-specific lease acquire timeout, e.g. RUNNER_MODEL_PROVIDER_ACQUIRE_TIMEOUT_OLLAMA.",
		"",
		category="llm",
	),
	EnvVarSpec("RUNNER_NODE_MAX_RETRIES", "Max retries per node execution.", "0", category="scheduler"),
	EnvVarSpec("RUNNER_NODE_RETRY_BACKOFF_MS", "Retry backoff between node retries.", "0", category="scheduler"),
	EnvVarSpec("RUNNER_QUEUE_GLOBAL_MAX", "Global queue max depth override.", "", category="scheduler"),
	EnvVarSpec("RUNNER_QUEUE_PER_EDGE_MAX", "Per-edge queue max depth override.", "", category="scheduler"),
	EnvVarSpec("STRICT_SCHEMA_EDGE_CHECKS", "Enable strict edge schema checks.", "1", category="feature_flags"),
	EnvVarSpec("STRICT_SCHEMA_EDGE_CHECKS_V2", "Enable strict edge schema checks v2.", "1", category="feature_flags"),
	EnvVarSpec("STRICT_COERCION_POLICY", "Enable strict coercion policy.", "1", category="feature_flags"),
	EnvVarSpec("RUNTIME_STRICT_STALE_TRANSITIONS", "Raise runtime error on stale transition guardrails.", "0", category="runtime"),
	EnvVarSpec("RUNTIME_STRICT_INVALIDATION_ASSERTS", "Raise runtime error on invalidation assert regressions.", "0", category="runtime"),
	EnvVarSpec("LLM_TABLE_MAX_ROWS", "Max rows when materializing table artifact for prompts.", "200", category="llm"),
	EnvVarSpec("LLM_TABLE_MAX_COLS", "Max columns when materializing table artifact for prompts.", "50", category="llm"),
	EnvVarSpec("LLM_PROMPT_MAX_CHARS", "Max prompt chars after materialization.", "20000", category="llm"),
	EnvVarSpec("LLM_TABLE_SORT_ROWS", "Sort table rows before prompt materialization.", "1", category="llm"),
	EnvVarSpec("OLLAMA_BASE_URL", "Default Ollama base URL for backend LLM calls.", "http://127.0.0.1:11434", category="llm"),
	EnvVarSpec("NODE_DUPLICATE_ENABLED", "Enable/disable duplicate-on-click behavior.", "1", category="ui"),
	EnvVarSpec(
		"NODE_DOC_FEEDBACK_LLM_BASE_URL",
		"Override Ollama base URL used by node-doc feedback suggester.",
		"",
		category="llm",
	),
	EnvVarSpec(
		"NODE_DOC_FEEDBACK_LLM_TIMEOUT_SECONDS",
		"Timeout in seconds for node-doc feedback LLM suggester requests.",
		"4",
		category="llm",
	),
	EnvVarSpec(
		"NODE_DOC_EXPLAIN_LLM_TIMEOUT_SECONDS",
		"Timeout in seconds for node-doc explain LLM requests.",
		"4",
		category="llm",
	),
	EnvVarSpec(
		"NODE_DOC_TOOLTIP_ENABLED",
		"Enable/disable node documentation tooltip display.",
		"1",
		category="ui",
	),
	EnvVarSpec(
		"NODE_DOC_TOOLTIP_OPEN_DELAY_MS",
		"Delay in ms before node documentation tooltip opens.",
		"500",
		category="ui",
	),
	EnvVarSpec(
		"NODE_DOC_PLANES_EXPANSION_ENABLED",
		"Enable/disable Data/Control/Param plane expansion in node docs.",
		"1",
		category="ui",
	),
	EnvVarSpec(
		"NODE_DOC_PLANES_EXPANSION_DELAY_MS",
		"Delay in ms before Data/Control/Param planes auto-expand in node docs.",
		"1200",
		category="ui",
	),
	EnvVarSpec(
		"NODE_DOC_EXPLAIN_LLM_MODEL",
		"LLM model used for AI-generated node explanations.",
		"glm-4.7-flash:latest",
		category="llm",
	),
	EnvVarSpec(
		"NODE_DOC_EXPLAIN_LLM_TEMPERATURE",
		"Sampling temperature for AI-generated node explanations.",
		"0.2",
		category="llm",
	),
	EnvVarSpec(
		"NODE_DOC_EXPLAIN_LLM_TOP_P",
		"Top-p sampling value for AI-generated node explanations.",
		"1.0",
		category="llm",
	),
	EnvVarSpec(
		"NODE_DOC_EXPLAIN_LLM_MAX_TOKENS",
		"Max tokens for AI-generated node explanations.",
		"512",
		category="llm",
	),
	EnvVarSpec(
		"NODE_DUPLICATE_DELAY_MS",
		"Delay in ms before duplicating a node on click/press (0 = immediate).",
		"500",
		category="ui",
	),
	EnvVarSpec("CACHE_KEY_DEBUG", "Enable cache key debug diagnostics.", "0", category="runtime"),
	EnvVarSpec("MODEL_CONN_*", "Model connection profile env refs, e.g. MODEL_CONN_DEV.", "", sensitive=True, category="model"),
]


def _ensure_loaded() -> None:
	global _LOADED, _OVERRIDES
	if _LOADED:
		return
	with _LOCK:
		if _LOADED:
			return
		try:
			raw = json.loads(_OVERRIDES_PATH.read_text(encoding="utf-8"))
			overrides = raw.get("overrides") if isinstance(raw, dict) else {}
			if isinstance(overrides, dict):
				_OVERRIDES = {str(k): str(v) for k, v in overrides.items() if isinstance(k, str)}
			else:
				_OVERRIDES = {}
		except Exception:
			_OVERRIDES = {}
		for key, value in _OVERRIDES.items():
			os.environ[str(key)] = str(value)
		_LOADED = True


def _persist_locked() -> None:
	_OVERRIDES_PATH.parent.mkdir(parents=True, exist_ok=True)
	payload = {"schemaVersion": _SCHEMA_VERSION, "overrides": dict(sorted(_OVERRIDES.items()))}
	_OVERRIDES_PATH.write_text(json.dumps(payload, ensure_ascii=True, separators=(",", ":")), encoding="utf-8")


def _is_sensitive(name: str) -> bool:
	n = str(name or "").strip().upper()
	if _SENSITIVE_HINT_RE.search(n):
		return True
	for spec in _SUPPORTED_ENV_SPECS:
		if spec.name == n and spec.sensitive:
			return True
		if spec.name.endswith("*") and n.startswith(spec.name[:-1]) and spec.sensitive:
			return True
	return False


def _is_valid_name(name: str) -> bool:
	n = str(name or "").strip().upper()
	return bool(_ENV_NAME_RE.match(n))


def get_supported_specs() -> List[EnvVarSpec]:
	return list(_SUPPORTED_ENV_SPECS)


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
	key = str(name or "").strip()
	if not key:
		return default
	_ensure_loaded()
	with _LOCK:
		if key in _OVERRIDES:
			return _OVERRIDES[key]
	return os.getenv(key, default)


def get_bool_env(name: str, default: bool) -> bool:
	raw = str(get_env(name, "") or "").strip().lower()
	if not raw:
		return bool(default)
	if raw in {"1", "true", "yes", "on"}:
		return True
	if raw in {"0", "false", "no", "off"}:
		return False
	return bool(default)


def set_env_override(name: str, value: str) -> None:
	key = str(name or "").strip().upper()
	if not _is_valid_name(key):
		raise ValueError(f"Invalid env var name '{key}'")
	_ensure_loaded()
	with _LOCK:
		_OVERRIDES[key] = str(value)
		os.environ[key] = str(value)
		_persist_locked()


def clear_env_override(name: str) -> None:
	key = str(name or "").strip().upper()
	if not _is_valid_name(key):
		raise ValueError(f"Invalid env var name '{key}'")
	_ensure_loaded()
	with _LOCK:
		_OVERRIDES.pop(key, None)
		if key in _BASE_ENV:
			os.environ[key] = _BASE_ENV[key]
		else:
			os.environ.pop(key, None)
		_persist_locked()


def list_env_state(*, include_sensitive_values: bool = False) -> List[Dict[str, Any]]:
	_ensure_loaded()
	rows: List[Dict[str, Any]] = []
	seen: set[str] = set()
	for spec in _SUPPORTED_ENV_SPECS:
		if spec.name.endswith("*"):
			prefix = spec.name[:-1]
			matching = sorted({k for k in set(os.environ.keys()) | set(_OVERRIDES.keys()) if str(k).startswith(prefix)})
			if not matching:
				matching = []
			for key in matching:
				seen.add(key)
				val = get_env(key, spec.default)
				sensitive = _is_sensitive(key)
				rows.append(
					{
						"name": key,
						"description": spec.description,
						"category": spec.category,
						"defaultValue": spec.default,
						"value": val if (include_sensitive_values or not sensitive) else None,
						"masked": bool(sensitive and not include_sensitive_values and bool(val)),
						"hasValue": bool(val),
						"source": "override" if key in _OVERRIDES else ("env" if key in os.environ else ("default" if spec.default is not None else "unset")),
						"restartRequired": bool(spec.restart_required),
						"sensitive": bool(sensitive),
						"supported": True,
					}
				)
			continue
		key = spec.name
		seen.add(key)
		val = get_env(key, spec.default)
		sensitive = _is_sensitive(key)
		rows.append(
			{
				"name": key,
				"description": spec.description,
				"category": spec.category,
				"defaultValue": spec.default,
				"value": val if (include_sensitive_values or not sensitive) else None,
				"masked": bool(sensitive and not include_sensitive_values and bool(val)),
				"hasValue": bool(val),
				"source": "override" if key in _OVERRIDES else ("env" if key in os.environ else ("default" if spec.default is not None else "unset")),
				"restartRequired": bool(spec.restart_required),
				"sensitive": bool(sensitive),
				"supported": True,
			}
		)
	for key in sorted(set(os.environ.keys()) | set(_OVERRIDES.keys())):
		name = str(key or "").strip()
		if not name or name in seen:
			continue
		seen.add(name)
		val = get_env(name, None)
		sensitive = _is_sensitive(name)
		rows.append(
			{
				"name": name,
				"description": "Uncataloged environment variable.",
				"category": "uncategorized",
				"defaultValue": None,
				"value": val if (include_sensitive_values or not sensitive) else None,
				"masked": bool(sensitive and not include_sensitive_values and bool(val)),
				"hasValue": bool(val),
				"source": "override" if name in _OVERRIDES else "env",
				"restartRequired": False,
				"sensitive": bool(sensitive),
				"supported": False,
			}
		)
	rows.sort(key=lambda item: (str(item.get("category") or ""), str(item.get("name") or "")))
	return rows
