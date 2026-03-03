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


def _canon_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


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


def default_contract_for_node(node: Dict[str, Any]) -> str:
    data = (node.get("data") or {}) if isinstance(node, dict) else {}
    kind = str(data.get("kind") or "").strip().lower()
    params = (data.get("params") or {}) if isinstance(data.get("params"), dict) else {}
    output_obj = (params.get("output") or {}) if isinstance(params.get("output"), dict) else {}
    output_mode = params.get("output_mode") or output_obj.get("mode")

    if kind == "source":
        source_kind = str(data.get("sourceKind") or params.get("source_type") or "").strip().lower()
        file_format = str(params.get("file_format") or "").strip().lower()
        if source_kind == "file" and file_format in {"jpg", "jpeg", "png", "webp", "gif", "svg", "tif", "tiff"}:
            return IMAGE_V1
        if source_kind == "file" and file_format in {"mp3", "wav", "flac", "ogg", "m4a", "aac"}:
            return AUDIO_V1
        if source_kind == "file" and file_format in {"mp4", "mov", "webm"}:
            return VIDEO_V1
        if source_kind in {"file", "database"}:
            fallback = TABLE_V1
        elif source_kind == "api":
            fallback = JSON_ANY_V1
        else:
            fallback = TABLE_V1
        return _contract_from_output_mode(output_mode, fallback)

    if kind == "transform":
        return TABLE_V1

    if kind == "llm":
        return _contract_from_output_mode(output_mode, TEXT_V1)

    if kind == "tool":
        return _contract_from_output_mode(output_mode, JSON_ANY_V1)

    return BINARY_V1
