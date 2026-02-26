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


def _normalize_determinism_env(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _normalize_determinism_env(v) for k, v in sorted(obj.items(), key=lambda kv: str(kv[0]))}
    if isinstance(obj, list):
        return [_normalize_determinism_env(x) for x in obj]
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    return str(obj)


def _normalize_input_refs(input_refs: Optional[list[tuple[str, str]]]) -> list[dict[str, str]]:
    refs = input_refs or []
    norm = [(str(port), str(aid)) for port, aid in refs]
    norm.sort(key=lambda x: (x[0], x[1]))
    return [{"port": port, "artifact_id": aid} for port, aid in norm]


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


def build_exec_key(
    *,
    graph_id: str,
    node_id: str,
    node_kind: str,
    node_state_hash: str,
    upstream_artifact_ids: Optional[list[str]] = None,
    input_refs: Optional[list[tuple[str, str]]] = None,
    determinism_env: Optional[Dict[str, Any]] = None,
    execution_version: str,
) -> str:
    if not str(graph_id or "").strip():
        raise ValueError("graph_id is required for exec_key generation")
    if not str(node_id or "").strip():
        raise ValueError("node_id is required for exec_key generation")
    if not str(node_state_hash or "").strip():
        raise ValueError("node_state_hash is required for exec_key generation")
    payload = {
        "build_version": execution_version,
        "graph_id": str(graph_id or ""),
        "node_id": str(node_id or ""),
        "node_kind": str(node_kind or ""),
        "node_state_hash": str(node_state_hash or ""),
        "upstream_artifact_keys": sorted(str(aid) for aid in (upstream_artifact_ids or [])),
        "input_bindings": _normalize_input_refs(input_refs),
        "determinism_env": _normalize_determinism_env(determinism_env or {}),
    }
    return _sha256_text(_canon_json(payload))
