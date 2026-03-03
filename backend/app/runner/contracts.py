from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional


DEFAULT_CONTRACT_VERSION = 1

TEXT_V1 = "TEXT_V1"
JSON_ANY_V1 = "JSON_ANY_V1"
TABLE_ANY_V1 = "TABLE_ANY_V1"
BINARY_V1 = "BINARY_V1"
EMBEDDINGS_ANY_V1 = "EMBEDDINGS_ANY_V1"
IMAGE_V1 = "IMAGE_V1"
AUDIO_V1 = "AUDIO_V1"
VIDEO_V1 = "VIDEO_V1"


def _canon_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def schema_fingerprint(schema_obj: Any) -> str:
    return hashlib.sha256(_canon_json(schema_obj).encode("utf-8")).hexdigest()


def canonical_schema_for_contract(contract: str) -> Dict[str, Any]:
    c = str(contract or "").strip().upper()
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
            fallback = TABLE_ANY_V1
        elif source_kind == "api":
            fallback = JSON_ANY_V1
        else:
            fallback = TABLE_ANY_V1
        return _contract_from_output_mode(output_mode, fallback)

    if kind == "transform":
        return TABLE_ANY_V1

    if kind == "llm":
        return _contract_from_output_mode(output_mode, TEXT_V1)

    if kind == "tool":
        return _contract_from_output_mode(output_mode, JSON_ANY_V1)

    return BINARY_V1
