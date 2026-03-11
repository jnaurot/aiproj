from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


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

_NON_SENSITIVE_TOKEN_KEYS = {
    "max_tokens",
}


def _is_sensitive_key(key: str) -> bool:
    k = (key or "").lower()
    if k in _NON_SENSITIVE_TOKEN_KEYS:
        return False
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


def _sorted_string_map(value: Optional[Dict[str, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in (value or {}).items():
        out[str(k)] = str(v if v is not None else "")
    return {k: out[k] for k in sorted(out.keys())}


def _merge_url_query(url: str, query: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    raw_url = str(url or "")
    split = urlsplit(raw_url)
    url_query = {k: v for k, v in parse_qsl(split.query, keep_blank_values=True)}
    editor_query = _sorted_string_map(query)
    merged = {**url_query, **editor_query}
    merged_sorted = {k: merged[k] for k in sorted(merged.keys())}
    normalized_base = urlunsplit((split.scheme, split.netloc, split.path, "", split.fragment))
    return {"base_url": normalized_base, "query": merged_sorted}


def _redact_header_map(headers: Optional[Dict[str, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in (headers or {}).items():
        key = str(k)
        lv = key.lower()
        if lv == "authorization":
            continue
        out[key] = "[REDACTED]" if _is_sensitive_key(key) else str(v if v is not None else "")
    return {k: out[k] for k in sorted(out.keys())}


def _redact_body_map(body: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (body or {}).items():
        key = str(k)
        out[key] = "[REDACTED]" if _is_sensitive_key(key) else _sanitize(v)
    return {k: out[k] for k in sorted(out.keys())}


def _schema_declared_type_from_node(node: Dict[str, Any]) -> Optional[str]:
    data = (node.get("data", {}) if isinstance(node, dict) else {}) or {}
    schema_env = data.get("schema") if isinstance(data.get("schema"), dict) else {}
    if not isinstance(schema_env, dict):
        return None
    for key in ("expectedSchema", "inferredSchema", "observedSchema"):
        obs = schema_env.get(key)
        if not isinstance(obs, dict):
            continue
        typed = obs.get("typedSchema")
        if not isinstance(typed, dict):
            continue
        t = str(typed.get("type") or "").strip().lower()
        if t == "string":
            t = "text"
        if t in {"table", "json", "text", "binary", "embeddings"}:
            return t
    return None


def build_source_fingerprint(node: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    data = (node.get("data", {}) if isinstance(node, dict) else {}) or {}
    source_kind = str(data.get("sourceKind") or params.get("source_type") or "file")
    p = params or {}
    source_output_type = _schema_declared_type_from_node(node)
    fp: Dict[str, Any] = {"source_kind": source_kind}
    if source_kind == "file":
        snapshot_id = p.get("snapshot_id") or p.get("snapshotId")
        if isinstance(snapshot_id, str) and snapshot_id.strip():
            fp.update(
                {
                    "snapshot_id": str(snapshot_id).strip().lower(),
                    "file_format": p.get("file_format"),
                    "encoding": p.get("encoding"),
                    "delimiter": p.get("delimiter"),
                    "sheet_name": p.get("sheet_name"),
                    "output_type": source_output_type,
                    "output_schema": p.get("output_schema") or ((p.get("output") or {}).get("schema")),
                }
            )
            return _sanitize(fp)
        rel_path = p.get("rel_path") or p.get("rootId") or "."
        filename = p.get("filename") or p.get("relPath")
        if not filename and isinstance(p.get("file_path"), str):
            legacy = Path(str(p.get("file_path")))
            rel_path = str(legacy.parent) if str(legacy.parent) not in {"", "."} else "."
            filename = legacy.name or str(legacy)
        root_base = Path(str(rel_path or ".")).expanduser().resolve()
        file_path_raw = None
        if isinstance(filename, str) and filename.strip():
            try:
                leaf = Path(str(filename)).expanduser()
                candidate = leaf.resolve() if leaf.is_absolute() else (root_base / leaf).resolve()
                file_path_raw = str(candidate)
            except Exception:
                file_path_raw = None
        file_stat: Dict[str, Any] = {
            "exists": False,
            "resolved_path": None,
            "size_bytes": None,
            "mtime_ns": None,
        }
        if isinstance(file_path_raw, str) and file_path_raw.strip():
            candidate = Path(file_path_raw).expanduser()
            try:
                resolved = str(candidate.resolve())
            except Exception:
                resolved = str(candidate)
            file_stat["resolved_path"] = resolved
            try:
                st = os.stat(candidate)
                file_stat.update(
                    {
                        "exists": True,
                        "size_bytes": int(st.st_size),
                        "mtime_ns": int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000))),
                    }
                )
            except Exception:
                pass
        fp.update(
            {
                "rel_path": rel_path,
                "filename": filename,
                "file_format": p.get("file_format"),
                "encoding": p.get("encoding"),
                "delimiter": p.get("delimiter"),
                "sheet_name": p.get("sheet_name"),
                "output_type": source_output_type,
                "output_schema": p.get("output_schema") or ((p.get("output") or {}).get("schema")),
                "file_stat": file_stat,
            }
        )
    elif source_kind == "database":
        connection_ref = p.get("connection_ref")
        connection_string = str(p.get("connection_string") or "")
        parsed_conn: Dict[str, Any] = {}
        if connection_string:
            try:
                from urllib.parse import urlparse

                u = urlparse(connection_string)
                parsed_conn = {
                    "scheme": u.scheme or None,
                    "host": u.hostname or None,
                    "port": u.port,
                    "dbname": (u.path or "").lstrip("/") or None,
                    "user": u.username or None,
                }
            except Exception:
                parsed_conn = {"value": "redacted"}
        fp.update(
            {
                "connection_ref": connection_ref,
                "connection_redacted": parsed_conn if not connection_ref else None,
                "table_name": p.get("table_name"),
                "query": p.get("query"),
                "limit": p.get("limit"),
                "output_type": source_output_type,
                "output_schema": p.get("output_schema") or ((p.get("output") or {}).get("schema")),
            }
        )
    elif source_kind == "api":
        content_type = p.get("content_type") or p.get("contentType")
        body_mode = str(p.get("body_mode") or p.get("bodyMode") or "none")
        merged_q = _merge_url_query(str(p.get("url") or ""), p.get("query"))
        headers = _redact_header_map(p.get("headers") or {})
        if content_type:
            headers["Content-Type"] = str(content_type)
            headers = {k: headers[k] for k in sorted(headers.keys())}

        body: Any = None
        if body_mode == "json":
            body = _redact_body_map(p.get("body_json") or p.get("bodyJson"))
        elif body_mode in {"form", "multipart"}:
            body = _redact_body_map(p.get("body_form") or p.get("bodyForm"))
        elif body_mode == "raw":
            body = str(p.get("body_raw") or p.get("bodyRaw") or "")

        fp.update(
            {
                "url": merged_q.get("base_url"),
                "query": merged_q.get("query"),
                "method": p.get("method"),
                "headers": headers,
                "content_type": content_type,
                "body_mode": body_mode,
                "body": body,
                "timeout_seconds": p.get("timeout_seconds"),
                "cache_policy": p.get("cache_policy") or {"mode": "default"},
                "output_type": source_output_type,
                "output_schema": p.get("output_schema") or ((p.get("output") or {}).get("schema")),
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
    if kind == "llm":
        output_obj = params.get("output") if isinstance(params.get("output"), dict) else {}
        output_type = _schema_declared_type_from_node(node) or "text"
        output_strict = (
            params.get("output_strict")
            if "output_strict" in params
            else (output_obj.get("strict") if "strict" in output_obj else True)
        )
        output_schema = params.get("output_schema")
        if output_schema is None and "jsonSchema" in output_obj:
            output_schema = output_obj.get("jsonSchema")
        embedding_contract = params.get("embedding_contract")
        if embedding_contract is None and "embedding" in output_obj:
            embedding_contract = output_obj.get("embedding")

        state = {
            "execution_version": execution_version,
            "node_kind": kind,
            "schema": {
                "output_type": output_type,
                "output_strict": bool(output_strict),
                "output_schema": _sanitize(output_schema),
                "embedding_contract": _sanitize(embedding_contract),
            },
            "settings": {},
            "params": _sanitize(params or {}),
        }
        return _sha256_text(_canon_json(state))

    state: Dict[str, Any] = {
        "execution_version": execution_version,
        "node_kind": kind,
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
    node_impl_version: str = "1",
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
        "node_impl_version": str(node_impl_version or "1"),
        "node_state_hash": str(node_state_hash or ""),
        "upstream_artifact_keys": sorted(str(aid) for aid in (upstream_artifact_ids or [])),
        "input_bindings": _normalize_input_refs(input_refs),
        "determinism_env": _normalize_determinism_env(determinism_env or {}),
    }
    return _sha256_text(_canon_json(payload))
