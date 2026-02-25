from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional


_SENSITIVE_PARAM_KEYS = {
    "authorization",
    "api_key",
    "apikey",
    "token",
    "password",
    "secret",
    "access_token",
    "refresh_token",
    "credentials",
}


def _is_sensitive_key(key: str) -> bool:
    k = (key or "").lower()
    return any(s in k for s in _SENSITIVE_PARAM_KEYS)


def _sanitize(obj: Any) -> Any:
    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            if str(k).startswith("_"):
                continue
            if _is_sensitive_key(str(k)):
                continue
            out[k] = _sanitize(v)
        return out
    if isinstance(obj, list):
        return [_sanitize(x) for x in obj]
    return obj


def _canon_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def _sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def build_source_fingerprint(node: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    data = (node.get("data", {}) if isinstance(node, dict) else {}) or {}
    source_kind = str(data.get("sourceKind") or params.get("source_type") or "file")
    p = params or {}
    fp: Dict[str, Any] = {"source_kind": source_kind}
    if source_kind == "file":
        fp.update(
            {
                "file_path": p.get("file_path"),
                "file_format": p.get("file_format"),
                "encoding": p.get("encoding"),
                "delimiter": p.get("delimiter"),
                "sheet_name": p.get("sheet_name"),
                "sample_size": p.get("sample_size"),
            }
        )
    elif source_kind == "database":
        fp.update(
            {
                "connection_ref": p.get("connection_ref"),
                "connection_string": p.get("connection_string"),
                "table_name": p.get("table_name"),
                "query": p.get("query"),
                "limit": p.get("limit"),
            }
        )
    elif source_kind == "api":
        fp.update(
            {
                "url": p.get("url"),
                "method": p.get("method"),
                "headers": p.get("headers"),
                "body": p.get("body"),
                "timeout_seconds": p.get("timeout_seconds"),
            }
        )
    return _sanitize(fp)


def build_node_state_hash(
    *,
    node: Dict[str, Any],
    params: Dict[str, Any],
    execution_version: str,
    source_fingerprint: Optional[Dict[str, Any]] = None,
) -> str:
    data = (node.get("data", {}) if isinstance(node, dict) else {}) or {}
    kind = str(data.get("kind") or "")
    state: Dict[str, Any] = {
        "execution_version": execution_version,
        "node_kind": kind,
        "ports": _sanitize(data.get("ports") or {}),
        "schema": _sanitize(data.get("schema") or data.get("contract") or {}),
        "settings": _sanitize(data.get("settings") or {}),
        "params": _sanitize(params or {}),
    }
    if kind == "source":
        state["source_fingerprint"] = _sanitize(
            source_fingerprint if isinstance(source_fingerprint, dict) else build_source_fingerprint(node, params or {})
        )
    return _sha256_text(_canon_json(state))
