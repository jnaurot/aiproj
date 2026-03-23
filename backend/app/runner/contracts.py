from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional


DEFAULT_CONTRACT_VERSION = 1

TEXT_V1 = "TEXT_V1"
JSON_ANY_V1 = "JSON_ANY_V1"
TABLE_V1 = "TABLE_V1"
# Back-compat alias for older imports/tests.
TABLE_ANY_V1 = TABLE_V1
BINARY_V1 = "BINARY_V1"
EMBEDDINGS_ANY_V1 = "EMBEDDINGS_ANY_V1"
IMAGE_V1 = "IMAGE_V1"
AUDIO_V1 = "AUDIO_V1"
VIDEO_V1 = "VIDEO_V1"

COERCION_POLICY_STRICT = "strict"
COERCION_POLICY_SAFE_WIDENING = "safe_widening"
COERCION_POLICY_ALLOW_LOSSY = "allow_lossy"

_SAFE_COERCIONS = {
    ("text", "table"),
    ("json", "table"),
    ("table", "json"),
}

_LOSSY_COERCIONS = {
    ("json", "text"),
    ("text", "json"),
}


def _canon_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def normalize_coercion_policy(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    if value == COERCION_POLICY_ALLOW_LOSSY:
        return COERCION_POLICY_ALLOW_LOSSY
    if value in {"strict", "forbid"}:
        return COERCION_POLICY_STRICT
    return COERCION_POLICY_SAFE_WIDENING


def evaluate_schema_coercion(
    provided_type_raw: Any,
    required_type_raw: Any,
    policy_raw: Any = COERCION_POLICY_SAFE_WIDENING,
) -> Dict[str, Any]:
    provided_type = str(provided_type_raw or "").strip().lower()
    required_type = str(required_type_raw or "").strip().lower()
    policy = normalize_coercion_policy(policy_raw)
    if provided_type == "string":
        provided_type = "text"
    if required_type == "string":
        required_type = "text"
    if not provided_type or not required_type:
        return {"mode": "blocked", "allowed": False, "lossy": False, "policy": policy}
    if provided_type == required_type:
        return {"mode": "native", "allowed": True, "lossy": False, "policy": policy}
    pair = (provided_type, required_type)
    if pair in _SAFE_COERCIONS:
        if policy == COERCION_POLICY_STRICT:
            return {"mode": "blocked", "allowed": False, "lossy": False, "policy": policy}
        return {"mode": "safe", "allowed": True, "lossy": False, "policy": policy}
    if pair in _LOSSY_COERCIONS:
        if policy != COERCION_POLICY_ALLOW_LOSSY:
            return {"mode": "blocked", "allowed": False, "lossy": False, "policy": policy}
        return {"mode": "lossy", "allowed": True, "lossy": True, "policy": policy}
    return {"mode": "blocked", "allowed": False, "lossy": False, "policy": policy}


def schema_fingerprint(schema_obj: Any) -> str:
    # TABLE_V1 fingerprints only canonical core (contract/version/columns).
    # stats/provenance are informational and intentionally excluded.
    if isinstance(schema_obj, dict):
        core = canonical_table_core(schema_obj)
        if core is not None:
            return hashlib.sha256(_canon_json(core).encode("utf-8")).hexdigest()
    return hashlib.sha256(_canon_json(schema_obj).encode("utf-8")).hexdigest()


def _normalize_column_type(value: Any) -> str:
    text = str(value).strip().lower() if value is not None else ""
    return text or "unknown"


def canonical_table_columns(columns: Any) -> list[Dict[str, str]]:
    out: list[Dict[str, str]] = []
    if not isinstance(columns, list):
        return out
    for col in columns:
        if isinstance(col, dict):
            name = str(col.get("name") or "").strip()
            if not name:
                continue
            col_type = _normalize_column_type(col.get("type", col.get("dtype")))
            out.append({"name": name, "type": col_type})
        elif col is not None:
            name = str(col).strip()
            if name:
                out.append({"name": name, "type": "unknown"})
    return out


def canonical_table_core(schema_obj: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(schema_obj, dict):
        return None
    contract = str(schema_obj.get("contract") or "").strip().upper()
    version = int(schema_obj.get("version") or DEFAULT_CONTRACT_VERSION)
    if contract == TABLE_V1:
        cols_raw = schema_obj.get("columns")
        if cols_raw is None:
            table = schema_obj.get("table")
            cols_raw = table.get("columns") if isinstance(table, dict) else None
        cols = canonical_table_columns(cols_raw)
        return {"contract": TABLE_V1, "version": version, "columns": cols}
    # Envelope shape {contract, version, table:{columns:[...]}, ...}
    table = schema_obj.get("table")
    if isinstance(table, dict):
        table_contract = str(schema_obj.get("contract") or "").strip().upper()
        if table_contract == TABLE_V1:
            cols = canonical_table_columns(table.get("columns"))
            return {"contract": TABLE_V1, "version": version, "columns": cols}
    return None


def canonical_schema_for_contract(contract: str) -> Dict[str, Any]:
    c = str(contract or "").strip().upper()
    if c == TABLE_V1:
        return {
            "schema_version": DEFAULT_CONTRACT_VERSION,
            "contract": TABLE_V1,
            "version": 1,
            "columns": [],
        }
    return {
        "schema_version": DEFAULT_CONTRACT_VERSION,
        "contract": c or BINARY_V1,
    }


def _contract_from_output_mode(output_mode: Optional[str], fallback: str) -> str:
    m = str(output_mode or "").strip().lower()
    if m == "json":
        return JSON_ANY_V1
    if m == "text":
        return TEXT_V1
    if m == "table":
        return TABLE_ANY_V1
    if m == "binary":
        return BINARY_V1
    if m == "embeddings":
        return EMBEDDINGS_ANY_V1
    return fallback


def _declared_typed_schema_type_from_node(node: Dict[str, Any]) -> Optional[str]:
    data = (node.get("data") or {}) if isinstance(node, dict) else {}
    schema_env = data.get("schema") if isinstance(data.get("schema"), dict) else {}
    if not isinstance(schema_env, dict):
        return None
    obs = schema_env.get("expectedSchema")
    if not isinstance(obs, dict):
        return None
    typed = obs.get("typedSchema")
    if not isinstance(typed, dict):
        return None
    t = str(typed.get("type") or "").strip().lower()
    if t == "string":
        t = "text"
    if t in {"table", "json", "text", "binary", "embeddings", "image", "audio", "video"}:
        return t
    return None


def _contract_from_typed_type(typed: Optional[str]) -> Optional[str]:
    t = str(typed or "").strip().lower()
    if t == "json":
        return JSON_ANY_V1
    if t == "text":
        return TEXT_V1
    if t == "table":
        return TABLE_ANY_V1
    if t == "binary":
        return BINARY_V1
    if t == "embeddings":
        return EMBEDDINGS_ANY_V1
    if t == "image":
        return IMAGE_V1
    if t == "audio":
        return AUDIO_V1
    if t == "video":
        return VIDEO_V1
    return None


def default_contract_for_node(node: Dict[str, Any]) -> str:
    data = (node.get("data") or {}) if isinstance(node, dict) else {}
    kind = str(data.get("kind") or "").strip().lower()
    params = (data.get("params") or {}) if isinstance(data.get("params"), dict) else {}

    typed_contract = _contract_from_typed_type(_declared_typed_schema_type_from_node(node))
    if typed_contract:
        return typed_contract
    output_obj = params.get("output") if isinstance(params.get("output"), dict) else {}
    output_mode = str(output_obj.get("mode") or "").strip().lower()
    mode_contract = _contract_from_output_mode(output_mode, "")
    if mode_contract:
        return mode_contract

    if kind == "source":
        source_kind = str(data.get("sourceKind") or params.get("source_type") or "").strip().lower()
        file_format = str(params.get("file_format") or "").strip().lower()
        if source_kind == "file":
            if file_format in {"jpg", "jpeg", "png", "webp", "gif", "svg", "tif", "tiff"}:
                return IMAGE_V1
            if file_format in {"mp3", "wav", "flac", "ogg", "m4a", "aac"}:
                return AUDIO_V1
            if file_format in {"mp4", "mov", "webm"}:
                return VIDEO_V1
            if file_format in {"csv", "tsv", "parquet", "excel"}:
                return TABLE_V1
            if file_format in {"json"}:
                return JSON_ANY_V1
            if file_format in {"txt", "pdf"}:
                return TEXT_V1
            return BINARY_V1
        if source_kind in {"database", "warehouse"}:
            fallback = TABLE_V1
        elif source_kind == "object_store":
            if file_format in {"json"}:
                fallback = JSON_ANY_V1
            elif file_format in {"txt", "pdf"}:
                fallback = TEXT_V1
            elif file_format in {"csv", "tsv", "parquet", "excel"}:
                fallback = TABLE_V1
            else:
                fallback = BINARY_V1
        elif source_kind == "api":
            fallback = JSON_ANY_V1
        else:
            fallback = TABLE_V1
        return fallback

    if kind == "transform":
        return TABLE_V1

    if kind in {"llm", "model"}:
        return TEXT_V1

    if kind == "tool":
        return JSON_ANY_V1

    return BINARY_V1
