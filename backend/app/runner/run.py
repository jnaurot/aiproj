import asyncio
import json
import re
import traceback
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.runner.nodes.transform import (
    normalize_transform_params,
    canonical_json,
    load_table_from_artifact_bytes,
    run_transform,
    sha256_hex,
)


from .compile import compile_plan
from .components import ComponentExpansionError, expand_graph_components
from .events import RunEventBus
from .validator import GraphValidator
from .metadata import GraphContext, NodeOutput
from .artifacts import Artifact, MemoryArtifactStore, RunBindings
from .cache import ExecutionCache
from .node_state import build_exec_key, build_node_state_hash, build_source_fingerprint
from .capabilities import allowed_ports
from .contracts import (
    TABLE_V1,
    canonical_table_columns,
    canonical_schema_for_contract,
    default_contract_for_node,
    schema_fingerprint as contract_schema_fingerprint,
)

from ..executors.source import exec_source
from ..executors.llm import exec_llm
from ..executors.tool import exec_tool

logger = logging.getLogger(__name__)


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def node_map(graph: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {n["id"]: n for n in graph.get("nodes", [])}


def edge_map(graph: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {e["id"]: e for e in graph.get("edges", []) if "id" in e}


def upstream_node_ids(edges: Dict[str, Dict[str, Any]], node_id: str) -> list[str]:
    return [e["source"] for e in edges.values() if e.get("target") == node_id]

def resolve_input_refs(
    edges: Dict[str, Dict[str, Any]],
    node_id: str,
    get_current_artifact,
) -> list[tuple[str, str]]:
    """
    Returns stable (port, upstream_artifact_id) pairs for edges targeting node_id.
    Port name is taken from edge.targetHandle if present; else 'in'.
    Only includes edges whose source node has produced an artifact via bindings.
    """
    refs: list[tuple[str, str]] = []
    for e in edges.values():
        if e.get("target") != node_id:
            continue
        src = e.get("source")
        if not src:
            continue
        aid = get_current_artifact(src)
        if not aid:
            continue
        port = e.get("targetHandle") or "in"
        refs.append((port, aid))
    # stable order
    refs.sort(key=lambda x: (x[0], x[1]))
    return refs


SENSITIVE_PARAM_KEYS = {
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
    return any(s in k for s in SENSITIVE_PARAM_KEYS)


def _sanitize_for_fingerprint(obj: Any) -> Any:
    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            if str(k).startswith("_"):
                continue
            if _is_sensitive_key(str(k)):
                continue
            out[k] = _sanitize_for_fingerprint(v)
        return out
    if isinstance(obj, list):
        return [_sanitize_for_fingerprint(x) for x in obj]
    return obj


def _tool_side_effect_mode(params: Dict[str, Any]) -> str:
    mode = (params.get("side_effect_mode") or "pure").lower()
    if mode not in ("pure", "idempotent", "effectful"):
        return "pure"
    return mode


def _normalized_params_for_exec_key(
    *,
    kind: str,
    node: Dict[str, Any],
    params: Dict[str, Any],
) -> Dict[str, Any]:
    p = dict(params or {})
    if kind == "llm":
        from .schemas import normalize_llm_params_frontend

        return normalize_llm_params_frontend(p)
    if kind == "source":
        from .schemas import normalize_source_params_frontend

        p = normalize_source_params_frontend(p)
        for ui_key in (
            "recentSnapshotIds",
            "recent_snapshot_ids",
            "snapshotMetadata",
            "snapshot_metadata",
            "recentSnapshots",
            "snapshotHistory",
        ):
            p.pop(ui_key, None)
        source_kind = (node.get("data", {}).get("sourceKind") or p.get("source_type") or "file")
        p["source_type"] = source_kind
        if source_kind == "file" and isinstance(p.get("file_path"), str) and not p.get("filename"):
            from pathlib import Path as _P

            _fp = _P(str(p.get("file_path")))
            p.setdefault("rel_path", str(_fp.parent) if str(_fp.parent) not in {"", "."} else ".")
            p.setdefault("filename", _fp.name or str(_fp))
        return p
    if kind == "transform":
        return normalize_transform_params(
            p,
            default_op=(node.get("data", {}) or {}).get("transformKind"),
        )
    return p


def _tool_exec_key(
    *,
    params: Dict[str, Any],
    input_refs: list[tuple[str, str]],
    execution_version: str,
    determinism_env: Optional[Dict[str, Any]] = None,
    graph_id: str = "test_graph",
    node_id: str = "tool",
) -> str:
    node = {"data": {"kind": "tool", "ports": {}, "schema": {}, "settings": {}}}
    node_state_hash = build_node_state_hash(
        node=node,
        params=params or {},
        execution_version=execution_version,
    )
    return build_exec_key(
        graph_id=graph_id,
        node_id=node_id,
        node_kind="tool",
        node_state_hash=node_state_hash,
        upstream_artifact_ids=[aid for _, aid in sorted(input_refs)],
        input_refs=input_refs,
        determinism_env=determinism_env,
        execution_version=execution_version,
        node_impl_version="TOOL@1",
    )


def _transform_exec_key(
    *,
    normalized_params: Dict[str, Any],
    input_refs: list[tuple[str, str]],
    execution_version: str,
    determinism_env: Optional[Dict[str, Any]] = None,
    graph_id: str = "test_graph",
    node_id: str = "transform",
) -> str:
    node = {"data": {"kind": "transform", "ports": {}, "schema": {}, "settings": {}}}
    node_state_hash = build_node_state_hash(
        node=node,
        params=normalized_params or {},
        execution_version=execution_version,
    )
    return build_exec_key(
        graph_id=graph_id,
        node_id=node_id,
        node_kind="transform",
        node_state_hash=node_state_hash,
        upstream_artifact_ids=[aid for _, aid in sorted(input_refs)],
        input_refs=input_refs,
        determinism_env=determinism_env,
        execution_version=execution_version,
        node_impl_version="TRANSFORM@1",
    )


def _node_impl_version(kind: str) -> str:
    mapping = {
        "source": "SOURCE@1",
        "transform": "TRANSFORM@1",
        "llm": "LLM@1",
        "tool": "TOOL@1",
    }
    return mapping.get(str(kind or ""), "GENERIC@1")


def _tool_is_armed(params: Dict[str, Any]) -> bool:
    return bool(params.get("armed", False))


def _table_payload_schema_from_rows(rows: list[dict[str, Any]]) -> Dict[str, Any]:
    first_seen: list[str] = []
    seen: set[str] = set()
    types: Dict[str, str] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        for k, v in row.items():
            col = str(k)
            if col not in seen:
                seen.add(col)
                first_seen.append(col)
            if v is None:
                continue
            vtype = type(v).__name__.lower().strip() or "unknown"
            prev = types.get(col)
            if prev is None:
                types[col] = vtype
            elif prev != vtype:
                types[col] = "unknown"
    columns = [{"name": col, "type": types.get(col, "unknown")} for col in first_seen]
    return {"schema_version": 1, "type": "table", "columns": columns, "row_count": len(rows or [])}


def _table_schema_envelope(
    *,
    columns: list[Dict[str, Any]],
    row_count: Optional[int] = None,
    provenance: Optional[Dict[str, Any]] = None,
    coercion: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    env: Dict[str, Any] = {
        "contract": TABLE_V1,
        "version": 1,
        "table": {"columns": canonical_table_columns(columns)},
    }
    if isinstance(coercion, dict) and coercion:
        env["table"]["coercion"] = {
            "mode": str(coercion.get("mode") or "native"),
            **({"lossy": bool(coercion.get("lossy"))} if "lossy" in coercion else {}),
            **({"notes": str(coercion.get("notes"))} if coercion.get("notes") else {}),
        }
    if row_count is not None:
        env["stats"] = {"rowCount": int(row_count)}
    if isinstance(provenance, dict) and provenance:
        env["provenance"] = provenance
    return env


def _table_schema_fingerprint_from_envelope(schema_env: Dict[str, Any]) -> str:
    return contract_schema_fingerprint(schema_env)


def _compact_typed_columns(cols: list[Dict[str, Any]] | None) -> list[str]:
    out: list[str] = []
    for c in cols or []:
        if not isinstance(c, dict):
            continue
        name = str(c.get("name") or "").strip()
        if not name:
            continue
        ctype = str(c.get("type") or "unknown").strip() or "unknown"
        out.append(f"{name}:{ctype}")
    return out


def _compact_expected_actual(details: Dict[str, Any]) -> tuple[str, str]:
    expected = details.get("expected")
    actual = details.get("actual")
    try:
        exp_s = json.dumps(expected, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except Exception:
        exp_s = str(expected)
    try:
        act_s = json.dumps(actual, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except Exception:
        act_s = str(actual)
    return exp_s, act_s


def _extract_table_columns_from_payload_schema(payload_schema: Any) -> list[Dict[str, Any]]:
    if not isinstance(payload_schema, dict):
        return []
    schema_env = payload_schema.get("schema")
    if isinstance(schema_env, dict):
        table = schema_env.get("table")
        if isinstance(table, dict):
            cols = table.get("columns")
            if isinstance(cols, list):
                return canonical_table_columns(cols)
    cols = payload_schema.get("columns")
    if isinstance(cols, list):
        return canonical_table_columns(cols)
    return []


def _transform_output_columns(
    *,
    op: str,
    norm: Dict[str, Any],
    primary_cols: list[str],
    other_cols: Optional[list[str]] = None,
) -> list[Dict[str, str]]:
    primary = [str(c) for c in (primary_cols or [])]
    other = [str(c) for c in (other_cols or [])]
    op_l = str(op or "").lower()

    if op_l == "select":
        spec = norm.get("select") or {}
        mode = str(spec.get("mode") or "include").lower().strip()
        keep_order = str(spec.get("keepOrder") or ("input" if mode == "exclude" else "custom")).lower().strip()
        cols = [str(c) for c in (spec.get("columns") or [])]
        selected = set(cols)
        primary_set = set(primary)
        if mode == "exclude":
            out = [c for c in primary if c not in selected]
        elif keep_order == "input":
            out = [c for c in primary if c in selected]
        else:
            out = [c for c in cols if c in primary_set]
        return canonical_table_columns([{"name": c, "type": "unknown"} for c in out])
    if op_l == "rename":
        rename_map = (norm.get("rename") or {}).get("map") or {}
        out = [str(rename_map.get(c, c)) for c in primary]
        return canonical_table_columns([{"name": c, "type": "unknown"} for c in out])
    if op_l == "derive":
        derive_cols = ((norm.get("derive") or {}).get("columns") or [])
        appended = [str(d.get("name")) for d in derive_cols if isinstance(d, dict) and d.get("name")]
        out = primary + [c for c in appended if c not in primary]
        return canonical_table_columns([{"name": c, "type": "unknown"} for c in out])
    if op_l == "aggregate":
        group_by = [str(c) for c in ((norm.get("aggregate") or {}).get("groupBy") or [])]
        metrics = [str(m.get("name")) for m in ((norm.get("aggregate") or {}).get("metrics") or []) if isinstance(m, dict) and m.get("name")]
        out = group_by + [m for m in metrics if m not in group_by]
        return canonical_table_columns([{"name": c, "type": "unknown"} for c in out])
    if op_l == "join":
        out = list(primary)
        for col in other:
            if col not in out:
                out.append(col)
            else:
                suffix = "_right"
                cand = f"{col}{suffix}"
                n = 2
                while cand in out:
                    cand = f"{col}{suffix}{n}"
                    n += 1
                out.append(cand)
        return canonical_table_columns([{"name": c, "type": "unknown"} for c in out])
    if op_l == "split":
        spec = norm.get("split") or {}
        out_col = str(spec.get("outColumn") or "part")
        emit_index = bool(spec.get("emitIndex", True))
        emit_source_row = bool(spec.get("emitSourceRow", True))
        out: list[dict[str, str]] = [{"name": out_col, "type": "string"}]
        if emit_index:
            out.append({"name": "index", "type": "int"})
        if emit_source_row:
            out.append({"name": "source_row", "type": "int"})
        return canonical_table_columns(out)
    if op_l in {"sort", "limit", "dedupe", "filter", "sql"}:
        # sql may differ but keep deterministic fallback if no parser.
        return canonical_table_columns([{"name": c, "type": "unknown"} for c in primary])
    return canonical_table_columns([{"name": c, "type": "unknown"} for c in primary])


def _source_payload_schema(
    out_contract: Optional[str],
    data_value: Any,
    source_metadata: Optional[Any] = None,
) -> Optional[Dict[str, Any]]:
    if out_contract == "table" and isinstance(data_value, list):
        payload = _table_payload_schema_from_rows(data_value)
        if isinstance(getattr(source_metadata, "data_schema", None), dict):
            ds = getattr(source_metadata, "data_schema", {}) or {}
            table_columns = ds.get("table_columns")
            if isinstance(table_columns, list):
                payload["columns"] = canonical_table_columns(table_columns)
            coercion = ds.get("table_coercion")
            if isinstance(coercion, dict):
                payload["coercion"] = coercion
        resolved_columns = payload.get("columns")
        if isinstance(resolved_columns, list):
            node_id = str(getattr(source_metadata, "node_id", "") or "")
            compact = [
                f"{str(c.get('name') or '').strip()}:{str(c.get('type') or 'unknown').strip() or 'unknown'}"
                for c in resolved_columns
                if isinstance(c, dict)
            ]
            print(f"[schema-types] nodeId={node_id} schema.table.columns={compact}")
            logger.info("[schema-types] nodeId=%s schema.table.columns=%s", node_id, compact)
        return payload
    if out_contract == "json":
        schema = _json_payload_value_schema(data_value)
        root_type = str(schema.get("type") or "unknown")
        json_shape = "object" if root_type == "object" else "array" if root_type == "array" else "unknown"
        out: Dict[str, Any] = {
            "schema_version": 1,
            "type": "json",
            "json_shape": json_shape,
            "schema": schema,
        }
        if isinstance(data_value, dict):
            out["keys_sample"] = sorted(list(data_value.keys()))
        return out
    if out_contract == "text":
        return {"schema_version": 1, "type": "text", "encoding": "utf-8"}
    if out_contract == "binary":
        return {"schema_version": 1, "type": "binary"}
    return None


def _llm_payload_schema(mime_type: str, data_value: Any) -> Optional[Dict[str, Any]]:
    mt = (mime_type or "").lower()
    if isinstance(data_value, dict) and str(data_value.get("mode") or "").lower() == "embeddings":
        out = {"schema_version": 1, "type": "embeddings"}
        if "dims" in data_value:
            out["dims"] = data_value.get("dims")
        if "dtype" in data_value:
            out["dtype"] = data_value.get("dtype")
        if "layout" in data_value:
            out["layout"] = data_value.get("layout")
        return out
    if "application/json" in mt:
        parsed = data_value
        if isinstance(parsed, str):
            try:
                parsed = json.loads(parsed)
            except Exception:
                return {"schema_version": 1, "type": "json", "json_shape": "unknown"}
        schema = _json_payload_value_schema(parsed)
        root_type = str(schema.get("type") or "unknown")
        json_shape = "object" if root_type == "object" else "array" if root_type == "array" else "unknown"
        out: Dict[str, Any] = {
            "schema_version": 1,
            "type": "json",
            "json_shape": json_shape,
            "schema": schema,
        }
        if isinstance(parsed, dict):
            out["keys_sample"] = sorted(list(parsed.keys()))
        return out
    if "text/markdown" in mt:
        return {"schema_version": 1, "type": "text", "encoding": "utf-8", "format": "markdown"}
    if "text/plain" in mt:
        return {"schema_version": 1, "type": "text", "encoding": "utf-8"}
    return None


def _json_schema_sort_key(schema: Dict[str, Any]) -> str:
    try:
        return json.dumps(schema, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except Exception:
        return str(schema)


def _json_payload_value_schema(value: Any, *, depth: int = 0, max_depth: int = 24) -> Dict[str, Any]:
    if depth >= max_depth:
        return {"type": "unknown", "reason": "max_depth"}

    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "boolean"}
    if isinstance(value, int):
        return {"type": "integer"}
    if isinstance(value, float):
        return {"type": "number"}
    if isinstance(value, str):
        return {"type": "string"}

    if isinstance(value, dict):
        props: Dict[str, Any] = {}
        required: list[str] = []
        for raw_key in sorted(value.keys(), key=lambda k: str(k)):
            key = str(raw_key)
            props[key] = _json_payload_value_schema(value[raw_key], depth=depth + 1, max_depth=max_depth)
            required.append(key)
        return {
            "type": "object",
            "properties": props,
            "required": required,
            "additionalProperties": False,
        }

    if isinstance(value, list):
        if not value:
            return {"type": "array", "items": {"type": "unknown"}}

        unique: Dict[str, Dict[str, Any]] = {}
        for item in value:
            item_schema = _json_payload_value_schema(item, depth=depth + 1, max_depth=max_depth)
            unique[_json_schema_sort_key(item_schema)] = item_schema

        ordered = [unique[k] for k in sorted(unique.keys())]
        if len(ordered) == 1:
            items_schema: Dict[str, Any] = ordered[0]
        else:
            items_schema = {"type": "union", "anyOf": ordered}

        return {"type": "array", "items": items_schema}

    return {"type": "unknown"}


def _sample_external_payload(value: Any, *, depth: int = 0, max_depth: int = 2, max_items: int = 3) -> Any:
    if depth >= max_depth:
        return "<truncated>"

    if value is None or isinstance(value, (bool, int, float)):
        return value

    if isinstance(value, str):
        return value if len(value) <= 200 else f"{value[:200]}...(len={len(value)})"

    if isinstance(value, bytes):
        return {
            "bytes_len": len(value),
            "bytes_head_hex": value[:16].hex(),
        }

    if isinstance(value, list):
        return [_sample_external_payload(v, depth=depth + 1, max_depth=max_depth, max_items=max_items) for v in value[:max_items]]

    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for k in sorted(value.keys(), key=lambda x: str(x))[:max_items]:
            out[str(k)] = _sample_external_payload(value[k], depth=depth + 1, max_depth=max_depth, max_items=max_items)
        return out

    return str(value)


def _emit_external_schema_debug(*, kind: str, node_id: str, schema: Dict[str, Any], payload: Any) -> None:
    if kind not in {"source", "llm", "tool"}:
        return
    try:
        schema_json = json.dumps(schema, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except Exception:
        schema_json = str(schema)
    try:
        sample_json = json.dumps(_sample_external_payload(payload), ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except Exception:
        sample_json = str(_sample_external_payload(payload))
    print(f"[external-schema] kind={kind} nodeId={node_id} schema={schema_json} sample={sample_json}")


def _tool_payload_schema(envelope_kind: str, payload: Any) -> Optional[Dict[str, Any]]:
    if envelope_kind == "json":
        schema = _json_payload_value_schema(payload)
        root_type = str(schema.get("type") or "unknown")
        if root_type == "object":
            json_shape = "object"
        elif root_type == "array":
            json_shape = "array"
        elif root_type == "unknown":
            json_shape = "unknown"
        else:
            json_shape = "scalar"

        out: Dict[str, Any] = {
            "schema_version": 1,
            "type": "json",
            "json_shape": json_shape,
            "schema": schema,
        }
        if isinstance(payload, dict):
            out["keys_sample"] = sorted(list(payload.keys()))
        return out
    if envelope_kind == "text":
        return {"schema_version": 1, "type": "text", "encoding": "utf-8"}
    if envelope_kind == "binary":
        return {"schema_version": 1, "type": "binary"}
    return None


def _artifact_metadata_v1(
    *,
    exec_key: str,
    node_id: str,
    node_type: str,
    node_impl_version: str,
    params_fingerprint: str,
    upstream_artifact_ids: list[str],
    contract_fingerprint: str,
    schema_fingerprint: str,
    mime_type: str,
    port_type: Optional[str],
    schema: Optional[Dict[str, Any]],
    created_at_iso: str,
    run_id: Optional[str],
    graph_id: Optional[str],
    component_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out = {
        "metadataVersion": 1,
        "execKey": exec_key,
        "nodeId": node_id,
        "nodeType": node_type,
        "nodeImplVersion": node_impl_version,
        "paramsFingerprint": params_fingerprint,
        "upstreamArtifactIds": list(upstream_artifact_ids),
        "contractFingerprint": contract_fingerprint,
        "schemaFingerprint": schema_fingerprint,
        "mimeType": mime_type,
        "portType": str(port_type or ""),
        "createdAt": created_at_iso,
        "runId": run_id,
        "graphId": graph_id,
    }
    if isinstance(schema, dict):
        out["schema"] = schema
    if isinstance(component_context, dict) and component_context:
        out["component"] = {
            "componentId": str(component_context.get("componentId") or ""),
            "componentRevisionId": str(component_context.get("componentRevisionId") or ""),
            "instanceNodeId": str(component_context.get("instanceNodeId") or ""),
            "internalNodeId": str(component_context.get("internalNodeId") or ""),
        }
    return out


def _is_contract_mismatch_error(message: str) -> bool:
    m = (message or "").lower()
    return ("contract mismatch" in m) or ("payload schema mismatch" in m)


_CACHE_DECISIONS = {"cache_hit", "cache_miss", "cache_hit_contract_mismatch"}
_CACHE_REASONS = {
    "CACHE_HIT",
    "CACHE_ENTRY_MISSING",
    "INPUTS_UNRESOLVED",
    "PARAMS_CHANGED",
    "INPUT_CHANGED",
    "ENV_CHANGED",
    "BUILD_CHANGED",
    "UNCACHEABLE_EFFECTFUL_TOOL",
    "SOURCE_CACHE_POLICY_NEVER",
    "GLOBAL_FORCE_OFF",
    "NODE_POLICY_PREFER_OFF",
    "NODE_POLICY_FORCE_OFF",
    "CONTRACT_MISMATCH",
}
_DEFAULT_REASON_BY_DECISION = {
    "cache_hit": "CACHE_HIT",
    "cache_miss": "CACHE_ENTRY_MISSING",
    "cache_hit_contract_mismatch": "CONTRACT_MISMATCH",
}


def _global_cache_mode(runtime_ref: Any) -> str:
    if runtime_ref is None:
        return "default_on"
    if hasattr(runtime_ref, "get_global_cache_mode"):
        mode = str(runtime_ref.get_global_cache_mode() or "").strip().lower()
    else:
        mode = "default_on" if bool(getattr(runtime_ref, "global_cache_enabled", True)) else "force_off"
    if mode not in {"default_on", "force_off", "force_on"}:
        return "default_on"
    return mode


def _node_cache_policy_mode(
    *,
    kind: str,
    source_kind: str,
    params: Dict[str, Any],
) -> str:
    """
    Normalized node policy:
    - inherit: default behavior
    - prefer_off: node asks to bypass cache in normal mode
    - force_off: explicit never-cache policy
    """
    cache_policy = params.get("cache_policy") if isinstance(params.get("cache_policy"), dict) else {}
    policy_mode = str(cache_policy.get("mode") or "").strip().lower()
    if policy_mode in {"force_off", "never"}:
        return "force_off"
    if policy_mode in {"prefer_off", "off", "disabled"}:
        return "prefer_off"
    if kind == "source" and source_kind == "file" and (not bool(params.get("cache_enabled", True))):
        return "prefer_off"
    if kind == "tool" and (not bool(params.get("cache_enabled", True))):
        return "prefer_off"
    return "inherit"


class ContractMismatchError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "CONTRACT_MISMATCH",
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or {}


def _sorted_unique_strings(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    out = [str(v) for v in values if v is not None and str(v) != ""]
    return sorted(set(out))


def _stable_unique_strings(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for v in values:
        if v is None:
            continue
        s = str(v)
        if s == "" or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _contract_details(
    *,
    missing_columns: Optional[list[str]] = None,
    expected: Optional[Dict[str, Any]] = None,
    actual: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    # Canonical, stable shape for deterministic errors/tests.
    details: Dict[str, Any] = {
        "missingColumns": _sorted_unique_strings(missing_columns or []),
        "expected": expected or {},
        "actual": actual or {},
    }
    return details


def _available_columns_for_port(
    *,
    port: str,
    input_schema_cols_by_port: Dict[str, list[Dict[str, Any]]],
    input_columns: Dict[str, list[str]],
) -> tuple[list[str], str]:
    schema_cols = input_schema_cols_by_port.get(port) or []
    schema_names = [
        str(c.get("name"))
        for c in schema_cols
        if isinstance(c, dict) and str(c.get("name") or "").strip()
    ]
    if schema_names:
        return _stable_unique_strings(schema_names), "schema"
    inferred = _stable_unique_strings(input_columns.get(port) or [])
    return inferred, "inferred"


def _missing_column_details(
    *,
    op: str,
    param_path: str,
    missing_columns: list[str],
    available_columns: list[str],
    available_source: str,
) -> Dict[str, Any]:
    missing = _stable_unique_strings(missing_columns)
    available = _stable_unique_strings(available_columns)
    return {
        "errorCode": "MISSING_COLUMN",
        "op": str(op),
        "paramPath": str(param_path),
        "missingColumns": missing,
        "availableColumns": available,
        "availableColumnsSource": str(available_source),
        "message": (
            f"Missing column(s): {missing}. "
            f"Available columns: {available}"
        ),
    }


def _extract_quoted_identifiers(expr: str) -> list[str]:
    # Deterministic conservative parse: only quoted/backticked column refs are checked.
    if not isinstance(expr, str) or not expr.strip():
        return []
    names = re.findall(r'"([^"]+)"|`([^`]+)`', expr)
    out: list[str] = []
    for a, b in names:
        n = (a or b or "").strip()
        if n:
            out.append(n)
    return sorted(set(out))


def _infer_artifact_port_type(artifact: Artifact) -> str:
    if artifact.port_type:
        return str(artifact.port_type)
    ps = artifact.payload_schema if isinstance(artifact.payload_schema, dict) else {}
    ps_type = str(ps.get("type") or "").lower()
    if ps_type == "string":
        ps_type = "text"
    if ps_type in {"table", "json", "text", "binary", "embeddings"}:
        return ps_type
    mt = (artifact.mime_type or "").lower()
    if "json" in mt:
        return "json"
    if "markdown" in mt or mt.startswith("text/"):
        return "text"
    if "csv" in mt or "tsv" in mt or "parquet" in mt:
        return "table"
    return "binary"


def _explicit_schema_from_node(node: Dict[str, Any]) -> Optional[Any]:
    params = (node.get("data", {}).get("params", {}) or {}) if isinstance(node, dict) else {}
    schema_obj = (
        params.get("output_schema")
        or ((params.get("output") or {}).get("schema"))
        or params.get("json_schema")
        or ((params.get("output") or {}).get("jsonSchema"))
    )
    if isinstance(schema_obj, (dict, list)):
        return _sanitize_for_fingerprint(schema_obj)
    return None


def _source_table_provenance(node: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    data = (node.get("data", {}) if isinstance(node, dict) else {}) or {}
    source_kind = str(data.get("sourceKind") or params.get("source_type") or "file").lower()
    out: Dict[str, Any] = {"sourceKind": source_kind}
    if source_kind == "file":
        filename = str(params.get("filename") or "").strip()
        if filename:
            out["tableName"] = filename
    elif source_kind == "database":
        conn = str(params.get("connection_string") or "")
        table_name = str(params.get("table_name") or "").strip()
        query = str(params.get("query") or "").strip()
        if table_name:
            out["tableName"] = table_name
        if query:
            out["query"] = query
        if conn:
            try:
                from urllib.parse import urlparse

                parsed = urlparse(conn)
                if parsed.hostname:
                    out["dbName"] = parsed.hostname
                db_schema = (parsed.path or "").lstrip("/")
                if db_schema:
                    out["dbSchema"] = db_schema
            except Exception:
                pass
    elif source_kind == "api":
        endpoint = str(params.get("url") or "").strip()
        if endpoint:
            try:
                parsed = urlsplit(endpoint)
                out["endpoint"] = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", parsed.fragment))
            except Exception:
                out["endpoint"] = endpoint
    return out


def _expected_schema_contract_for_node(node: Dict[str, Any]) -> Dict[str, Any]:
    explicit_schema = _explicit_schema_from_node(node)
    if explicit_schema is not None:
        canonical = {"schema_version": 1, "explicit_schema": explicit_schema}
        return {
            "schemaObject": canonical,
            "schemaFingerprint": contract_schema_fingerprint(canonical),
            "schemaSource": "explicit",
        }
    default_contract = default_contract_for_node(node)
    canonical = canonical_schema_for_contract(default_contract)
    return {
        "schemaObject": canonical,
        "schemaFingerprint": contract_schema_fingerprint(canonical),
        "schemaSource": f"default:{default_contract}",
    }


def _expected_mime_for_port(port: str) -> str:
    p = str(port or "").strip().lower()
    if p == "json":
        return "application/json"
    if p == "table":
        return "text/csv; charset=utf-8"
    if p == "text":
        return "text/plain; charset=utf-8"
    if p == "embeddings":
        return "application/json"
    return "application/octet-stream"


def _artifact_schema_fingerprint(artifact: Artifact) -> str:
    ps = artifact.payload_schema if isinstance(artifact.payload_schema, dict) else {}
    meta = ps.get("artifactMetadataV1") if isinstance(ps, dict) else None
    if isinstance(meta, dict):
        v = str(meta.get("schemaFingerprint") or meta.get("contractFingerprint") or "").strip()
        if v:
            return v
    payload_without_meta = dict(ps) if isinstance(ps, dict) else {}
    if isinstance(payload_without_meta, dict):
        payload_without_meta.pop("artifactMetadataV1", None)
    return contract_schema_fingerprint(payload_without_meta)


def _normalize_mime_strict(mime_type: str) -> str:
    return str(mime_type or "").strip().lower()


def _declared_out_port(kind: str, node: Dict[str, Any]) -> Optional[str]:
    ports = (node.get("data", {}).get("ports", {}) or {})
    if kind == "llm":
        params = (node.get("data", {}).get("params", {}) or {})
        output_mode = str((params.get("output_mode") or ((params.get("output") or {}).get("mode")) or "text"))
        if output_mode == "json":
            return "json"
        if output_mode == "embeddings":
            return "embeddings"
        return "text"
    declared = ports.get("out")
    if declared:
        return str(declared)
    if kind == "transform":
        return "table"
    if kind == "tool":
        return "json"
    return None


def _allowed_ports(kind: str, direction: str, provider: Optional[str] = None) -> set[str]:
    return allowed_ports(kind, direction, provider=provider)


def _cached_artifact_contract_mismatch(
    kind: str,
    node: Dict[str, Any],
    artifact: Artifact,
    expected_schema: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    declared = _declared_out_port(kind, node) or "unknown"
    expected_schema_fingerprint = str(expected_schema.get("schemaFingerprint") or "")
    expected_schema_source = str(expected_schema.get("schemaSource") or "unknown")
    if not expected_schema_fingerprint:
        return None
    actual_schema_fingerprint = _artifact_schema_fingerprint(artifact)
    expected_mime = _normalize_mime_strict(_expected_mime_for_port(declared))
    actual_mime = _normalize_mime_strict(artifact.mime_type or "")
    expected_contract = {
        "schemaFingerprint": expected_schema_fingerprint,
        "mimeType": expected_mime,
    }
    actual_contract = {
        "schemaFingerprint": actual_schema_fingerprint,
        "mimeType": actual_mime,
    }
    schema_mismatch = expected_schema_fingerprint != actual_schema_fingerprint
    if schema_mismatch:
        mime_matches = expected_mime == actual_mime
        return {
            "message": (
                "Contract mismatch (cache hit): "
                f"[schemaSource={expected_schema_source}] declared out='{declared}' expected schema '{expected_schema_fingerprint[:12]}...' "
                f"but cached artifact had '{actual_schema_fingerprint[:12]}...'"
            ),
            "artifactId": artifact.artifact_id,
            "producerExecKey": artifact.exec_key,
            "mismatchKind": "schema_fingerprint",
            "mimeMatches": mime_matches,
            "expectedSchemaSource": expected_schema_source,
            "expectedSchemaFingerprint": expected_schema_fingerprint,
            "actualSchemaFingerprint": actual_schema_fingerprint,
            "expectedMimeType": expected_mime,
            "actualMimeType": actual_mime,
            "expectedContractFingerprint": sha256_hex(canonical_json(expected_contract).encode("utf-8")),
            "actualContractFingerprint": sha256_hex(canonical_json(actual_contract).encode("utf-8")),
        }
    return None


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = str(__import__("os").environ.get(name, "")).strip()
    if not raw:
        return default
    try:
        val = int(raw)
    except Exception:
        return default
    return max(minimum, val)


def _env_bool(name: str, default: bool) -> bool:
    raw = str(__import__("os").environ.get(name, "")).strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _determinism_env_for_node(kind: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if kind == "llm":
        output_mode = str((params.get("output_mode") or ((params.get("output") or {}).get("mode")) or "text"))
        input_encoding = str((params.get("input_encoding") or params.get("inputEncoding") or "text"))
        return {
            "llm_input_format": "artifact_input_v2",
            "llm_input_encoding": input_encoding,
            "llm_table_format": "csv_v1",
            "llm_table_max_rows": _env_int("LLM_TABLE_MAX_ROWS", 200),
            "llm_table_max_cols": _env_int("LLM_TABLE_MAX_COLS", 50),
            "llm_prompt_max_chars": _env_int("LLM_PROMPT_MAX_CHARS", 20000),
            "llm_table_sort_rows": _env_bool("LLM_TABLE_SORT_ROWS", True),
            "llm_output_mode": output_mode,
        }
    if kind == "transform":
        return {"transform_engine": "duckdb"}
    return {}


def _plan_levels(plan, edges: Dict[str, Dict[str, Any]]) -> list[list[str]]:
    sub = set(plan.subgraph)
    indeg: Dict[str, int] = {nid: 0 for nid in sub}
    adj: Dict[str, list[str]] = {nid: [] for nid in sub}
    for e in edges.values():
        s = e.get("source")
        t = e.get("target")
        if s in sub and t in sub:
            adj[s].append(t)
            indeg[t] += 1

    order_index = {nid: i for i, nid in enumerate(plan.order)}
    ready = sorted([nid for nid, d in indeg.items() if d == 0], key=lambda n: order_index.get(n, 10**9))
    levels: list[list[str]] = []
    seen = 0
    while ready:
        level = list(ready)
        levels.append(level)
        seen += len(level)
        nxt: list[str] = []
        for cur in level:
            for nb in adj.get(cur, []):
                indeg[nb] -= 1
                if indeg[nb] == 0:
                    nxt.append(nb)
        ready = sorted(nxt, key=lambda n: order_index.get(n, 10**9))
    if seen != len(sub):
        raise ValueError("Graph is not a DAG (cycle detected)")
    return levels


async def _record_consumers(
    *,
    context: GraphContext,
    input_artifact_ids: list[str],
    consumer_run_id: str,
    consumer_node_id: str,
    consumer_exec_key: Optional[str],
    output_artifact_id: str,
) -> None:
    if not input_artifact_ids:
        return
    record_fn = getattr(context.artifact_store, "record_consumers", None)
    if not callable(record_fn):
        return
    await record_fn(
        input_artifact_ids=sorted(set(input_artifact_ids)),
        consumer_run_id=consumer_run_id,
        consumer_node_id=consumer_node_id,
        consumer_exec_key=consumer_exec_key,
        output_artifact_id=output_artifact_id,
    )



async def run_graph(
    run_id: str, 
    graph: Dict[str, Any], 
    run_from: Optional[str], 
    bus: RunEventBus, 
    run_mode: Optional[str] = None,
    artifact_store=None, 
    cache=None,
    cancel_event: Optional[asyncio.Event] = None,
    runtime_ref: Optional[Any] = None,
    graph_id: Optional[str] = None,
    ):
    # ---- Create execution context ONCE (do not recreate later) ----
    artifact_store = artifact_store or MemoryArtifactStore()
    if not str(graph_id or "").strip():
        raise ValueError("graph_id is required")
    graph_id = str(graph_id)
    bindings = RunBindings(run_id, graph_id=graph_id)

    context = GraphContext(
        graph_id=graph_id,
        run_id=run_id,
        bus=bus,
        artifact_store=artifact_store,
        bindings=bindings,
        runtime_ref=runtime_ref,
    )
    
    print("[context]", type(context.bus), type(context.artifact_store), type(context.bindings))
    context.bus.graph_id = graph_id

    cache = cache or ExecutionCache()
    cache_stats = {"hit": 0, "miss": 0, "hit_contract_mismatch": 0}
    cache_summary_emitted = False

    async def _emit_cache_decision(
        *,
        node_id: str,
        node_kind: str,
        decision: str,
        exec_key: str,
        artifact_id: Optional[str] = None,
        expected_port_type: Optional[str] = None,
        actual_port_type: Optional[str] = None,
        producer_exec_key: Optional[str] = None,
        expected_schema_source: Optional[str] = None,
        expected_contract_fingerprint: Optional[str] = None,
        actual_contract_fingerprint: Optional[str] = None,
        mismatch_kind: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> None:
        d = decision if decision in _CACHE_DECISIONS else "cache_miss"
        # Contract note: reason is required on the wire; we always resolve one.
        # Keep schema_version=1 for additive/non-breaking changes and only bump for
        # breaking payload changes.
        resolved_reason = str(reason or _DEFAULT_REASON_BY_DECISION.get(d, "CACHE_ENTRY_MISSING"))
        if resolved_reason not in _CACHE_REASONS:
            resolved_reason = "CACHE_ENTRY_MISSING"
        evt = {
            "type": "cache_decision",
            "schema_version": 1,
            "runId": run_id,
            "at": iso_now(),
            "nodeId": node_id,
            "nodeKind": node_kind,
            "decision": d,
            "reason": resolved_reason,
            "execKey": exec_key,
        }
        if artifact_id:
            evt["artifactId"] = artifact_id
        if expected_port_type:
            evt["expectedPortType"] = expected_port_type
        if actual_port_type:
            evt["actualPortType"] = actual_port_type
        if producer_exec_key is not None:
            evt["producerExecKey"] = producer_exec_key
        if expected_schema_source:
            evt["expectedSchemaSource"] = expected_schema_source
        if expected_contract_fingerprint:
            evt["expectedContractFingerprint"] = expected_contract_fingerprint
        if actual_contract_fingerprint:
            evt["actualContractFingerprint"] = actual_contract_fingerprint
        if mismatch_kind:
            evt["mismatchKind"] = mismatch_kind
        await context.bus.emit(evt)

    async def _emit_cache_summary_once() -> None:
        nonlocal cache_summary_emitted
        if cache_summary_emitted:
            return
        cache_summary_emitted = True
        await context.bus.emit({
            "type": "cache_summary",
            "schema_version": 1,
            "runId": run_id,
            "at": iso_now(),
            "cache_hit": int(cache_stats["hit"]),
            "cache_miss": int(cache_stats["miss"]),
            "cache_hit_contract_mismatch": int(cache_stats["hit_contract_mismatch"]),
        })

    # ===== PHASE 1: PRE-EXECUTION VALIDATION =====
    validator = GraphValidator()
    validation = validator.validate_pre_execution(graph)

    if not validation.valid:
        for error in validation.errors:
            await context.bus.emit({
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "error",
                "message": f"[{error.code}] {error.message}",
                "nodeId": error.node_id
            })
        await context.bus.emit({
            "type": "run_finished",
            "runId": run_id,
            "at": iso_now(),
            "status": "failed"
        })
        return

    for warning in validation.warnings:
        await context.bus.emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "warn",
            "message": f"[{warning.code}] {warning.message}",
            "nodeId": warning.node_id
        })

    # ===== PHASE 1.5: COMPONENT EXPANSION =====
    component_expansion = None
    execution_graph = graph
    component_store = getattr(runtime_ref, "component_revisions", None) if runtime_ref is not None else None
    try:
        component_expansion = expand_graph_components(
            graph,
            component_store=component_store,
            max_depth=5,
        )
        execution_graph = component_expansion.graph
    except ComponentExpansionError as ex:
        await context.bus.emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "error",
            "message": f"[{ex.code}] {str(ex)}",
            "code": ex.code,
            "details": ex.details,
        })
        await context.bus.emit({
            "type": "run_finished",
            "runId": run_id,
            "at": iso_now(),
            "status": "failed",
            "error": str(ex),
            "errorCode": ex.code,
            "errorDetails": ex.details,
        })
        return

    # ===== PHASE 2: EXECUTION =====
    try:
        plan = compile_plan(execution_graph, run_from, run_mode=run_mode)
        context.planner_ref = plan
        effective_run_mode = "from_start" if run_from is None else (str(run_mode or "from_selected_onward"))
        planned_node_ids = sorted(list(plan.subgraph))
        if component_expansion:
            parent_ids = {
                component_expansion.internal_to_parent.get(nid)
                for nid in planned_node_ids
                if nid in component_expansion.internal_to_parent
            }
            planned_node_ids = sorted({*planned_node_ids, *{p for p in parent_ids if p}})
        await context.bus.emit({
            "type": "run_started",
            "runId": run_id,
            "at": iso_now(),
            "runFrom": run_from,
            "runMode": effective_run_mode,
            "plannedNodeIds": planned_node_ids,
        })
        nodes = node_map(execution_graph)
        edges = edge_map(execution_graph)
        get_current_artifact = context.bindings.get_current_artifact
        component_runtime_state: Dict[str, Dict[str, Any]] = {}
        component_parent_for_internal: Dict[str, str] = {}
        component_meta_by_parent: Dict[str, Dict[str, Any]] = {}

        if component_expansion:
            component_parent_for_internal = dict(component_expansion.internal_to_parent)
            component_meta_by_parent = dict(component_expansion.parent_component_meta)
            for parent_node_id, internal_nodes in component_expansion.parent_to_internal.items():
                planned_internal = [nid for nid in internal_nodes if nid in plan.subgraph]
                if not planned_internal:
                    continue
                component_runtime_state[parent_node_id] = {
                    "remaining": len(planned_internal),
                    "started": False,
                    "failed": False,
                    "started_at": None,
                }

        async def _component_mark_node_start(node_id: str) -> None:
            parent_node_id = component_parent_for_internal.get(node_id)
            if not parent_node_id:
                return
            state = component_runtime_state.get(parent_node_id)
            if not state or state.get("started"):
                return
            state["started"] = True
            state["started_at"] = asyncio.get_running_loop().time()
            meta = component_meta_by_parent.get(parent_node_id, {})
            await context.bus.emit({
                "type": "component_started",
                "runId": run_id,
                "at": iso_now(),
                "nodeId": parent_node_id,
                "componentId": str(meta.get("componentId") or ""),
                "componentRevisionId": str(meta.get("componentRevisionId") or ""),
            })
            await context.bus.emit({
                "type": "node_started",
                "runId": run_id,
                "at": iso_now(),
                "nodeId": parent_node_id,
            })

        async def _component_mark_node_finish(node_id: str, *, ok: bool, error: Optional[str] = None) -> None:
            parent_node_id = component_parent_for_internal.get(node_id)
            if not parent_node_id:
                return
            state = component_runtime_state.get(parent_node_id)
            if not state:
                return
            remaining = int(state.get("remaining") or 0)
            state["remaining"] = max(0, remaining - 1)
            if not ok and not state.get("failed"):
                state["failed"] = True
                meta = component_meta_by_parent.get(parent_node_id, {})
                elapsed_ms = max(
                    0.0,
                    (asyncio.get_running_loop().time() - float(state.get("started_at") or asyncio.get_running_loop().time())) * 1000.0,
                )
                await context.bus.emit({
                    "type": "component_failed",
                    "runId": run_id,
                    "at": iso_now(),
                    "nodeId": parent_node_id,
                    "componentId": str(meta.get("componentId") or ""),
                    "componentRevisionId": str(meta.get("componentRevisionId") or ""),
                    "error": str(error or "Component internal node failed"),
                })
                await context.bus.emit({
                    "type": "node_finished",
                    "runId": run_id,
                    "at": iso_now(),
                    "nodeId": parent_node_id,
                    "status": "failed",
                    "error": str(error or "Component internal node failed"),
                    "execution_time_ms": elapsed_ms,
                })
                return

            if state["remaining"] == 0 and not state.get("failed"):
                meta = component_meta_by_parent.get(parent_node_id, {})
                elapsed_ms = max(
                    0.0,
                    (asyncio.get_running_loop().time() - float(state.get("started_at") or asyncio.get_running_loop().time())) * 1000.0,
                )
                await context.bus.emit({
                    "type": "component_finished",
                    "runId": run_id,
                    "at": iso_now(),
                    "nodeId": parent_node_id,
                    "componentId": str(meta.get("componentId") or ""),
                    "componentRevisionId": str(meta.get("componentRevisionId") or ""),
                    "status": "succeeded",
                })
                await context.bus.emit({
                    "type": "node_finished",
                    "runId": run_id,
                    "at": iso_now(),
                    "nodeId": parent_node_id,
                    "status": "succeeded",
                    "execution_time_ms": elapsed_ms,
                })

        def _binding_snapshot(node_id: str) -> tuple[Optional[str], Optional[str]]:
            b = context.bindings.get(node_id)
            if b is None:
                return (None, None)
            return (b.artifact_id, b.status)

        def _assert_binding_unchanged(
            *,
            node_id: str,
            snapshot: tuple[Optional[str], Optional[str]],
            phase: str,
        ) -> None:
            current = _binding_snapshot(node_id)
            if current == snapshot:
                return
            raise RuntimeError(
                "Binding changed during execution before commit "
                f"(node_id={node_id}, phase={phase}, expected={snapshot}, actual={current})"
            )

        async def _resolve_node_execution(node_id: str) -> Dict[str, Any]:
            n = nodes[node_id]
            kind = n["data"]["kind"]
            params = n["data"].get("params", {}) or {}
            ports = (n.get("data", {}).get("ports", {}) or {})
            tool_provider = str(params.get("provider") or "") if kind == "tool" else None
            determinism_env = _determinism_env_for_node(kind, params)
            component_ctx = (
                (n.get("data", {}) or {}).get("_componentContext")
                if isinstance((n.get("data", {}) or {}).get("_componentContext"), dict)
                else None
            )
            if isinstance(component_ctx, dict):
                determinism_env = {
                    **dict(determinism_env or {}),
                    "component_instance": {
                        "component_id": str(component_ctx.get("componentId") or ""),
                        "component_revision_id": str(component_ctx.get("componentRevisionId") or ""),
                        "instance_node_id": str(component_ctx.get("instanceNodeId") or ""),
                        "component_config": component_ctx.get("componentConfig") if isinstance(component_ctx.get("componentConfig"), dict) else {},
                        "component_bindings": component_ctx.get("componentBindings") if isinstance(component_ctx.get("componentBindings"), dict) else {},
                    },
                }
            tool_mode = _tool_side_effect_mode(params) if kind == "tool" else None
            source_kind = str(n.get("data", {}).get("sourceKind") or params.get("source_type") or "")
            global_cache_mode = _global_cache_mode(runtime_ref)
            node_cache_policy = _node_cache_policy_mode(
                kind=kind,
                source_kind=source_kind,
                params=params,
            )
            tool_uncacheable = kind == "tool" and (tool_mode == "effectful")
            node_force_off = node_cache_policy == "force_off"
            node_prefer_off = node_cache_policy == "prefer_off"
            cache_bypass_reason: Optional[str] = None
            if tool_uncacheable:
                cache_bypass_reason = "UNCACHEABLE_EFFECTFUL_TOOL"
            elif global_cache_mode == "force_off":
                cache_bypass_reason = "GLOBAL_FORCE_OFF"
            elif node_force_off:
                cache_bypass_reason = "NODE_POLICY_FORCE_OFF"
            elif global_cache_mode == "default_on" and node_prefer_off:
                cache_bypass_reason = "NODE_POLICY_PREFER_OFF"
            use_cache_for_node = cache_bypass_reason is None

            up_nodes = sorted(upstream_node_ids(edges, node_id))
            upstream_ids = [aid for aid in (get_current_artifact(nid) for nid in up_nodes) if aid]
            input_refs = resolve_input_refs(edges, node_id, get_current_artifact)

            normalized_params_for_hash = _normalized_params_for_exec_key(
                kind=kind,
                node=n,
                params=params,
            )
            if kind == "source":
                debug_payload = {
                    "nodeId": node_id,
                    "sourceKind": source_kind,
                    "globalCacheMode": global_cache_mode,
                    "nodeCachePolicy": node_cache_policy,
                    "useCacheForNode": use_cache_for_node,
                    "snapshotId": normalized_params_for_hash.get("snapshot_id"),
                    "keys": sorted(list(normalized_params_for_hash.keys())),
                }
                print("[debug-exec-inputs]", json.dumps(debug_payload, sort_keys=True))
            source_fp = build_source_fingerprint(n, normalized_params_for_hash) if kind == "source" else None
            node_state_hash = build_node_state_hash(
                node=n,
                params=normalized_params_for_hash,
                execution_version=context.execution_version,
                source_fingerprint=source_fp,
            )
            exec_key = build_exec_key(
                graph_id=context.graph_id,
                node_id=node_id,
                node_kind=kind,
                node_state_hash=node_state_hash,
                upstream_artifact_ids=upstream_ids,
                input_refs=input_refs,
                determinism_env=determinism_env,
                execution_version=context.execution_version,
                node_impl_version=_node_impl_version(kind),
            )
            expected_schema = _expected_schema_contract_for_node(n)
            cached_artifact_id = exec_key if (use_cache_for_node and await context.artifact_store.exists(exec_key)) else None
            cache_resolution = "CACHE_HIT" if cached_artifact_id else "CACHE_MISS"
            logger.debug(
                "resolve_phase run_id=%s node_id=%s exec_key=%s cache_resolution=%s",
                run_id,
                node_id,
                exec_key,
                cache_resolution,
            )
            return {
                "node": n,
                "kind": kind,
                "params": params,
                "ports": ports,
                "tool_provider": tool_provider,
                "determinism_env": determinism_env,
                "tool_mode": tool_mode,
                "cache_bypass_reason": cache_bypass_reason,
                "use_cache_for_node": use_cache_for_node,
                "upstream_ids": upstream_ids,
                "input_refs": input_refs,
                "node_state_hash": node_state_hash,
                "exec_key": exec_key,
                "artifact_id": exec_key,
                "expected_schema": expected_schema,
                "cache_resolution": cache_resolution,
                "cached_artifact_id": cached_artifact_id,
            }

        async def _execute_node(node_id: str, *, cache_only: bool = False) -> Dict[str, Any]:
            node_started_t = asyncio.get_running_loop().time()
            binding_snapshot = _binding_snapshot(node_id)
            await context.bus.emit({
                "type": "node_started",
                "runId": run_id,
                "at": iso_now(),
                "nodeId": node_id
            })

            # Activate incoming edges
            for edge_id in plan.incoming_edges.get(node_id, []):
                await context.bus.emit({
                    "type": "edge_exec",
                    "runId": run_id,
                    "at": iso_now(),
                    "edgeId": edge_id,
                    "exec": "active"
                })

            resolved = await _resolve_node_execution(node_id)
            n = resolved["node"]
            kind = resolved["kind"]
            params = resolved["params"]
            ports = resolved["ports"]
            tool_provider = resolved["tool_provider"]
            determinism_env = resolved["determinism_env"]
            tool_mode = resolved["tool_mode"]
            cache_bypass_reason = resolved["cache_bypass_reason"]
            use_cache_for_node = resolved["use_cache_for_node"]
            upstream_ids = resolved["upstream_ids"]
            input_refs = resolved["input_refs"]
            node_state_hash = resolved["node_state_hash"]
            exec_key = resolved["exec_key"]
            artifact_id = resolved["artifact_id"]
            expected_schema = resolved["expected_schema"]
            cache_resolution = resolved["cache_resolution"]
            cached_artifact_id = resolved["cached_artifact_id"]
            expected_schema_source = str((expected_schema or {}).get("schemaSource") or "")

            if expected_schema_source.startswith("default:"):
                await context.bus.emit({
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "info",
                    "message": (
                        f"Schema defaulted: default={expected_schema_source.split(':', 1)[1]} "
                        "(no explicit schema provided)"
                    ),
                    "nodeId": node_id,
                    "schemaSource": expected_schema_source,
                    "expectedSchemaFingerprint": (expected_schema or {}).get("schemaFingerprint"),
                })

            allowed_in = _allowed_ports(kind, "in", provider=tool_provider)
            allowed_out = _allowed_ports(kind, "out", provider=tool_provider)
            declared_in = ports.get("in")
            declared_out = ports.get("out")
            preflight_error: Optional[ContractMismatchError] = None
            if kind == "source":
                if declared_in not in (None, "", "null"):
                    preflight_error = ContractMismatchError(
                        "Source output contract mismatch: source nodes do not support input ports",
                        details=_contract_details(
                            expected={"inPortType": None},
                            actual={"inPortType": str(declared_in)},
                        ),
                    )
            elif declared_in is not None and str(declared_in) not in allowed_in:
                preflight_error = ContractMismatchError(
                    f"{kind.capitalize()} output contract mismatch: unsupported input port '{declared_in}'",
                    details=_contract_details(
                        expected={"allowedInPortTypes": sorted(allowed_in)},
                        actual={"inPortType": str(declared_in)},
                    ),
                )

            if (preflight_error is None) and declared_out is not None and str(declared_out) not in allowed_out:
                preflight_error = ContractMismatchError(
                    f"{kind.capitalize()} output contract mismatch: unsupported output port '{declared_out}'",
                    details=_contract_details(
                        expected={"allowedOutPortTypes": sorted(allowed_out)},
                        actual={"outPortType": str(declared_out)},
                    ),
                )

            logger.debug(
                "exec_key_generated run_id=%s node_id=%s kind=%s exec_key=%s run_from=%s run_mode=%s",
                run_id,
                node_id,
                kind,
                exec_key,
                run_from,
                run_mode,
            )
            print(f"[debug-exec-key] graphId={context.graph_id} nodeId={node_id} exec_key={exec_key}")
            if not use_cache_for_node:
                await _emit_cache_decision(
                    node_id=node_id,
                    node_kind=kind,
                    decision="cache_miss",
                    exec_key=exec_key,
                    reason=str(cache_bypass_reason or "CACHE_ENTRY_MISSING"),
                )
                if cache_only:
                    msg = (
                        "Selected-only run requires cache for ancestor nodes, "
                        f"but node '{node_id}' cannot use cache in this run."
                    )
                    await context.bus.emit({
                        "type": "node_finished",
                        "runId": run_id,
                        "at": iso_now(),
                        "nodeId": node_id,
                        "status": "failed",
                        "execution_time_ms": max(
                            0.0, (asyncio.get_running_loop().time() - node_started_t) * 1000.0
                        ),
                        "error": msg,
                        "cached": False,
                    })
                    return {"ok": False, "cached": False}

            # ---- Resolve phase result ----
            if cache_resolution == "CACHE_HIT" and cached_artifact_id:
                await _emit_cache_decision(
                    node_id=node_id,
                    node_kind=kind,
                    decision="cache_hit",
                    exec_key=exec_key,
                    artifact_id=cached_artifact_id,
                )

                # Verification (you asked for checks)
                print(f"[cache-hit] node={node_id} artifact={cached_artifact_id[:10]}...")

                cached_art = await context.artifact_store.get(cached_artifact_id)
                if cached_art.graph_id and str(cached_art.graph_id) != str(context.graph_id):
                    raise RuntimeError(
                        f"Cache graph mismatch for node '{node_id}': artifact graph_id={cached_art.graph_id} run graph_id={context.graph_id}"
                    )
                mismatch_error = _cached_artifact_contract_mismatch(kind, n, cached_art, expected_schema)
                if mismatch_error:
                    # Contract mismatch on cached artifact is a cache rejection, not a successful cache hit.
                    cache_stats["hit_contract_mismatch"] += 1
                    await _emit_cache_decision(
                        node_id=node_id,
                        node_kind=kind,
                        decision="cache_hit_contract_mismatch",
                        exec_key=exec_key,
                        artifact_id=cached_artifact_id,
                        producer_exec_key=mismatch_error.get("producerExecKey"),
                        expected_schema_source=mismatch_error.get("expectedSchemaSource"),
                        expected_contract_fingerprint=mismatch_error.get("expectedContractFingerprint"),
                        actual_contract_fingerprint=mismatch_error.get("actualContractFingerprint"),
                        mismatch_kind=mismatch_error.get("mismatchKind"),
                        reason="CONTRACT_MISMATCH",
                    )
                    await context.bus.emit({
                        "type": "log",
                        "runId": run_id,
                        "at": iso_now(),
                        "level": "error",
                        "message": mismatch_error["message"],
                        "nodeId": node_id,
                        "code": "CONTRACT_MISMATCH",
                        "artifactId": mismatch_error["artifactId"],
                        "producerExecKey": mismatch_error.get("producerExecKey"),
                        "expectedMimeType": mismatch_error.get("expectedMimeType"),
                        "actualMimeType": mismatch_error.get("actualMimeType"),
                        "expectedSchemaFingerprint": mismatch_error.get("expectedSchemaFingerprint"),
                        "expectedSchemaSource": mismatch_error.get("expectedSchemaSource"),
                        "actualSchemaFingerprint": mismatch_error.get("actualSchemaFingerprint"),
                        "expectedContractFingerprint": mismatch_error.get("expectedContractFingerprint"),
                        "actualContractFingerprint": mismatch_error.get("actualContractFingerprint"),
                        "mismatchKind": mismatch_error.get("mismatchKind"),
                    })
                    # Continue as a miss; do not emit node_output/node_finished here.
                    cache_stats["miss"] += 1
                    await _emit_cache_decision(
                        node_id=node_id,
                        node_kind=kind,
                        decision="cache_miss",
                        exec_key=exec_key,
                        reason="CONTRACT_MISMATCH",
                    )
                    if cache_only:
                        msg = (
                            "Selected-only run requires cached ancestors, "
                            f"but cached entry was rejected for node '{node_id}' due to contract mismatch."
                        )
                        await context.bus.emit({
                            "type": "node_finished",
                            "runId": run_id,
                            "at": iso_now(),
                            "nodeId": node_id,
                            "status": "failed",
                            "execution_time_ms": max(
                                0.0, (asyncio.get_running_loop().time() - node_started_t) * 1000.0
                            ),
                            "error": msg,
                            "cached": False,
                        })
                        return {"ok": False, "cached": False}
                else:
                    cache_stats["hit"] += 1
                    _assert_binding_unchanged(
                        node_id=node_id,
                        snapshot=binding_snapshot,
                        phase="cache_hit_bind",
                    )
                    context.bindings.bind(node_id=node_id, artifact_id=cached_artifact_id, status="cached")
                    await _record_consumers(
                        context=context,
                        input_artifact_ids=upstream_ids,
                        consumer_run_id=run_id,
                        consumer_node_id=node_id,
                        consumer_exec_key=exec_key,
                        output_artifact_id=cached_artifact_id,
                    )
                    await context.bus.emit({
                        "type": "node_output",
                        "runId": run_id,
                        "nodeId": node_id,
                        "at": iso_now(),
                        "artifactId": cached_artifact_id,
                        "mimeType": cached_art.mime_type,
                        "portType": cached_art.port_type or ((n.get("data", {}).get("ports", {}) or {}).get("out")),
                        "cached": True,
                    })

                    await context.bus.emit({
                        "type": "node_finished",
                        "runId": run_id,
                        "at": iso_now(),
                        "nodeId": node_id,
                        "status": "succeeded",
                        "execution_time_ms": max(0.0, (asyncio.get_running_loop().time() - node_started_t) * 1000.0),
                        "cached": True
                    })

                    # Mark incoming edges as done
                    for edge_id in plan.incoming_edges.get(node_id, []):
                        await context.bus.emit({
                            "type": "edge_exec",
                            "runId": run_id,
                            "at": iso_now(),
                            "edgeId": edge_id,
                            "exec": "done"
                        })
                    await asyncio.sleep(0.05)
                    return {"ok": True, "cached": True}
            if use_cache_for_node and cache_resolution == "CACHE_MISS":
                cache_stats["miss"] += 1
                await _emit_cache_decision(
                    node_id=node_id,
                    node_kind=kind,
                    decision="cache_miss",
                    exec_key=exec_key,
                )
                if cache_only:
                    msg = (
                        "Selected-only run requires cached ancestors, "
                        f"but cache entry was missing for node '{node_id}'."
                    )
                    await context.bus.emit({
                        "type": "node_finished",
                        "runId": run_id,
                        "at": iso_now(),
                        "nodeId": node_id,
                        "status": "failed",
                        "execution_time_ms": max(
                            0.0, (asyncio.get_running_loop().time() - node_started_t) * 1000.0
                        ),
                        "error": msg,
                        "cached": False,
                    })
                    return {"ok": False, "cached": False}

            # ---- Execute node ----
            try:
                if preflight_error is not None:
                    raise preflight_error
                await asyncio.sleep(0.5)  # visual delay

                if kind == "source":
                    output = await exec_source(run_id, n, context, upstream_artifact_ids=upstream_ids)
                    print("[run_graph] bound artifact", artifact_id[:10], "to node", node_id)
###
                elif kind == "transform":
                    ports = (n.get("data", {}).get("ports", {}) or {})
                    in_contract = ports.get("in") or "table"
                    out_contract = ports.get("out") or "table"
                    if in_contract != "table":
                        raise ContractMismatchError(
                            f"Transform output contract mismatch: in must be 'table', got '{in_contract}'",
                            details=_contract_details(
                                expected={"portType": "table"},
                                actual={"portType": str(in_contract)},
                            ),
                        )
                    if out_contract != "table":
                        raise ContractMismatchError(
                            f"Transform output contract mismatch: out must be 'table', got '{out_contract}'",
                            details=_contract_details(
                                expected={"portType": "table"},
                                actual={"portType": str(out_contract)},
                            ),
                        )

                    await context.bus.emit({
                        "type": "log",
                        "runId": run_id,
                        "at": iso_now(),
                        "level": "info",
                        "message": "transform: start",
                        "nodeId": node_id,
                    })
                    transform_kind = str((n.get("data", {}) or {}).get("transformKind") or "").lower()
                    op_hint = str((params or {}).get("op") or "").lower()
                    is_join_transform = transform_kind == "join" or op_hint == "join"
                    if is_join_transform:
                        join_edges = [e for e in edges.values() if e.get("target") == node_id]
                        if not join_edges:
                            msg = "join: ack input node=<none> sourceNode=<none> inPort=<none> artifact=<none> cols=[]"
                            await context.bus.emit({
                                "type": "log",
                                "runId": run_id,
                                "at": iso_now(),
                                "level": "info",
                                "message": msg,
                                "nodeId": node_id,
                            })
                            print(
                                f"[join-ack] runId={run_id} nodeId={node_id} "
                                "node=<none> sourceNode=<none> inPort=<none> artifact=<none> cols=[]"
                            )
                        else:
                            for e in join_edges:
                                source_node = str(e.get("source") or "<unknown>")
                                in_port = str(e.get("targetHandle") or "in")
                                source_artifact = get_current_artifact(source_node)
                                artifact_label = str(source_artifact or "<none>")
                                if source_node.startswith("n_"):
                                    short_node = f"n_{source_node[2:10]}"
                                else:
                                    short_node = source_node[:10]
                                cols: list[str] = []
                                if source_artifact:
                                    try:
                                        art = await context.artifact_store.get(source_artifact)
                                        schema_cols = _extract_table_columns_from_payload_schema(
                                            getattr(art, "payload_schema", None)
                                        )
                                        if schema_cols:
                                            cols = _stable_unique_strings(
                                                [c.get("name") for c in schema_cols if isinstance(c, dict)]
                                            )
                                        else:
                                            try:
                                                raw = await context.artifact_store.read(source_artifact)
                                                df = load_table_from_artifact_bytes(art.mime_type or "", raw)
                                                cols = _stable_unique_strings(
                                                    [str(c) for c in list(getattr(df, "columns", []))]
                                                )
                                            except Exception:
                                                cols = []
                                    except Exception:
                                        cols = []
                                cols_label = "[" + ",".join(cols) + "]"
                                msg = (
                                    f"join: ack input node={short_node} sourceNode={source_node} "
                                    f"inPort={in_port} artifact={artifact_label} cols={cols_label}"
                                )
                                await context.bus.emit({
                                    "type": "log",
                                    "runId": run_id,
                                    "at": iso_now(),
                                    "level": "info",
                                    "message": msg,
                                    "nodeId": node_id,
                                })
                                print(
                                    f"[join-ack] runId={run_id} nodeId={node_id} "
                                    f"node={short_node} sourceNode={source_node} "
                                    f"inPort={in_port} artifact={artifact_label} cols={cols_label}"
                                )

                    if not params.get("enabled", True):
                        await context.bus.emit({
                            "type": "log",
                            "runId": run_id,
                            "at": iso_now(),
                            "level": "info",
                            "message": "transform: disabled; skipping",
                            "nodeId": node_id,
                        })
                        # Create a no-op NodeOutput (or mark succeeded with no artifact).
                        # Here: succeed but emit node_finished; keep artifact binding unchanged.
                        output = NodeOutput(status="succeeded", data=None, metadata=None, execution_time_ms=0.0)
                    else:
                        # 1) collect upstream artifacts (port -> artifact_id)
                        input_refs = resolve_input_refs(edges, node_id, get_current_artifact)  # [(port, artifact_id), ...]
                        input_tables = {}  # port -> DataFrame
                        input_columns: dict[str, list[str]] = {}
                        input_schema_cols_by_port: dict[str, list[Dict[str, Any]]] = {}
                        input_provenance_by_port: dict[str, Dict[str, Any]] = {}
                        required_cols_by_artifact: dict[str, list[str]] = {}
                        upstream_source_by_artifact: dict[str, str] = {}
                        upstream_port_by_artifact: dict[str, str] = {}

                        for e in edges.values():
                            if e.get("target") != node_id:
                                continue
                            src = e.get("source")
                            if not src:
                                continue
                            src_artifact_id = get_current_artifact(src)
                            if not src_artifact_id:
                                continue
                            upstream_source_by_artifact[src_artifact_id] = str(src)
                            upstream_port_by_artifact[src_artifact_id] = str(e.get("targetHandle") or "in")
                            contract = (e.get("data", {}) or {}).get("contract", {}) or {}
                            payload = contract.get("payload", {}) if isinstance(contract, dict) else {}
                            target_hint = payload.get("target", {}) if isinstance(payload, dict) else {}
                            req_cols = target_hint.get("required_columns") if isinstance(target_hint, dict) else None
                            if isinstance(req_cols, list) and req_cols:
                                required_cols_by_artifact[src_artifact_id] = req_cols

                        for port, upstream_artifact_id in input_refs:
                            art = await context.artifact_store.get(upstream_artifact_id)
                            ps = getattr(art, "payload_schema", None) or {}
                            ps_type_raw = ps.get("type") if isinstance(ps, dict) else None
                            ps_type = str(ps_type_raw or "").lower()
                            if ps_type == "string":
                                ps_type = "text"
                            if ps_type and ps_type != "table":
                                raise ContractMismatchError(
                                    (
                                        "Transform payload schema mismatch: "
                                        f"expected table input but got '{ps_type}'"
                                    ),
                                    code="PAYLOAD_SCHEMA_MISMATCH",
                                    details=_contract_details(
                                        expected={"payloadType": "table"},
                                        actual={
                                            "payloadType": ps_type,
                                            "artifactId": upstream_artifact_id,
                                        },
                                    ),
                                )
                            req_cols = required_cols_by_artifact.get(upstream_artifact_id)
                            if req_cols:
                                payload_schema = getattr(art, "payload_schema", None) or {}
                                src_cols = _extract_table_columns_from_payload_schema(payload_schema)
                                src_col_names = []
                                if isinstance(src_cols, list):
                                    src_col_names = [c.get("name") if isinstance(c, dict) else c for c in src_cols]
                                if src_col_names:
                                    missing = [c for c in req_cols if c not in src_col_names]
                                    if missing:
                                        raise ContractMismatchError(
                                            f"Edge payload schema mismatch: missing required columns {missing}",
                                            code="PAYLOAD_SCHEMA_MISMATCH",
                                            details=_contract_details(
                                                missing_columns=missing,
                                                expected={"requiredColumns": _sorted_unique_strings(req_cols)},
                                                actual={
                                                    "availableColumns": _sorted_unique_strings(src_col_names),
                                                    "artifactId": upstream_artifact_id,
                                                },
                                            ),
                                        )
                            b = await context.artifact_store.read(upstream_artifact_id)
                            df = load_table_from_artifact_bytes(art.mime_type or "application/octet-stream", b)
                            input_tables[port] = df
                            input_columns[port] = [str(c) for c in list(getattr(df, "columns", []))]
                            schema_cols = _extract_table_columns_from_payload_schema(getattr(art, "payload_schema", None))
                            input_schema_cols_by_port[port] = (
                                schema_cols
                                if schema_cols
                                else canonical_table_columns(
                                    [{"name": c, "type": "unknown"} for c in input_columns[port]]
                                )
                            )
                            input_provenance_by_port[port] = {
                                "sourceKind": "upstream",
                                "upstream": {
                                    "nodeId": upstream_source_by_artifact.get(upstream_artifact_id),
                                    "port": upstream_port_by_artifact.get(upstream_artifact_id, port),
                                },
                            }

                        op_preview = str((_normalized_params_for_exec_key(kind=kind, node=n, params=params).get("op") or "")).lower()
                        input_schema_summary = [
                            {
                                "port": port,
                                "artifact": aid,
                                "columns": _compact_typed_columns(input_schema_cols_by_port.get(port) or []),
                            }
                            for port, aid in input_refs
                        ]
                        input_schema_msg = (
                            f"transform: input-schema op={op_preview or '<none>'} "
                            f"inputs={json.dumps(input_schema_summary, ensure_ascii=False, separators=(',', ':'))}"
                        )
                        await context.bus.emit({
                            "type": "log",
                            "runId": run_id,
                            "at": iso_now(),
                            "level": "info",
                            "message": input_schema_msg,
                            "nodeId": node_id,
                        })
                        print(f"[transform-input-schema] runId={run_id} nodeId={node_id} {input_schema_msg}")

                        # join lookup (node_id -> DataFrame), best-effort
                        join_lookup: dict[str, Any] = {}
                        for upstream_node_id in nodes.keys():
                            upstream_artifact_id = get_current_artifact(upstream_node_id)
                            if not upstream_artifact_id:
                                continue
                            art = await context.artifact_store.get(upstream_artifact_id)
                            b = await context.artifact_store.read(upstream_artifact_id)
                            try:
                                join_lookup[upstream_node_id] = load_table_from_artifact_bytes(art.mime_type or "", b)
                            except Exception:
                                pass

                        # Reuse the same normalized transform params contract used for hashing.
                        norm = _normalized_params_for_exec_key(kind=kind, node=n, params=params)

                        # 3) execute (cache resolve already happened before node execution)
                        op = str(norm.get("op") or "")
                        primary_port = "in" if "in" in input_tables else (next(iter(input_tables.keys())) if input_tables else "in")
                        primary_cols = input_columns.get(primary_port, [])
                        primary_cols_set = set(primary_cols)
                        input_artifact_ids = [aid for _, aid in input_refs]

                        if op == "select":
                            select_spec = (norm.get("select") or {})
                            mode = str(select_spec.get("mode") or "include").strip().lower()
                            strict = bool(select_spec.get("strict", True))
                            expected_cols = [str(c) for c in (select_spec.get("columns") or [])]
                            seen_cols: set[str] = set()
                            duplicate_cols: list[str] = []
                            for col in expected_cols:
                                if col in seen_cols and col not in duplicate_cols:
                                    duplicate_cols.append(col)
                                seen_cols.add(col)
                            if duplicate_cols:
                                available_cols, available_source = _available_columns_for_port(
                                    port=primary_port,
                                    input_schema_cols_by_port=input_schema_cols_by_port,
                                    input_columns=input_columns,
                                )
                                raise ContractMismatchError(
                                    f"Transform payload schema mismatch: select has duplicate columns {duplicate_cols}",
                                    code="DUPLICATE_COLUMN",
                                    details={
                                        "errorCode": "DUPLICATE_COLUMN",
                                        "op": "select",
                                        "paramPath": "select.columns",
                                        "missingColumns": _stable_unique_strings(duplicate_cols),
                                        "availableColumns": _stable_unique_strings(available_cols),
                                        "availableColumnsSource": available_source,
                                    },
                                )
                            available_cols, available_source = _available_columns_for_port(
                                port=primary_port,
                                input_schema_cols_by_port=input_schema_cols_by_port,
                                input_columns=input_columns,
                            )
                            available_set = set(available_cols)
                            missing_cols = [c for c in expected_cols if c not in available_set]
                            if strict and missing_cols:
                                raise ContractMismatchError(
                                    f"Transform payload schema mismatch: select references missing columns {missing_cols}",
                                    code="MISSING_COLUMN",
                                    details=_missing_column_details(
                                        op="select",
                                        param_path="select.columns",
                                        missing_columns=missing_cols,
                                        available_columns=available_cols,
                                        available_source=available_source,
                                    ),
                                )
                        elif op == "rename":
                            rename_map = (norm.get("rename") or {}).get("map") or {}
                            expected_cols = [str(c) for c in rename_map.keys()]
                            available_cols, available_source = _available_columns_for_port(
                                port=primary_port,
                                input_schema_cols_by_port=input_schema_cols_by_port,
                                input_columns=input_columns,
                            )
                            available_set = set(available_cols)
                            missing_cols = [c for c in expected_cols if c not in available_set]
                            if missing_cols:
                                raise ContractMismatchError(
                                    f"Transform payload schema mismatch: rename references missing columns {missing_cols}",
                                    code="MISSING_COLUMN",
                                    details=_missing_column_details(
                                        op="rename",
                                        param_path="rename.map",
                                        missing_columns=missing_cols,
                                        available_columns=available_cols,
                                        available_source=available_source,
                                    ),
                                )
                        elif op == "derive":
                            derive_cols = ((norm.get("derive") or {}).get("columns") or [])
                            expected_cols: list[str] = []
                            for d in derive_cols:
                                if not isinstance(d, dict):
                                    continue
                                expected_cols.extend(_extract_quoted_identifiers(str(d.get("expr") or "")))
                            expected_cols = sorted(set(expected_cols))
                            if expected_cols:
                                available_cols, available_source = _available_columns_for_port(
                                    port=primary_port,
                                    input_schema_cols_by_port=input_schema_cols_by_port,
                                    input_columns=input_columns,
                                )
                                available_set = set(available_cols)
                                missing_cols = [c for c in expected_cols if c not in available_set]
                                if missing_cols:
                                    raise ContractMismatchError(
                                        f"Transform payload schema mismatch: derive references missing columns {missing_cols}",
                                        code="MISSING_COLUMN",
                                        details=_missing_column_details(
                                            op="derive",
                                            param_path="derive.columns",
                                            missing_columns=missing_cols,
                                            available_columns=available_cols,
                                            available_source=available_source,
                                        ),
                                    )
                        elif op == "aggregate":
                            aggregate_spec = norm.get("aggregate") or {}
                            group_by_cols = [
                                str(c).strip()
                                for c in (aggregate_spec.get("groupBy") or [])
                                if str(c).strip()
                            ]
                            metrics = aggregate_spec.get("metrics") or []
                            available_cols, available_source = _available_columns_for_port(
                                port=primary_port,
                                input_schema_cols_by_port=input_schema_cols_by_port,
                                input_columns=input_columns,
                            )
                            available_set = set(available_cols)
                            missing_group = [c for c in group_by_cols if c not in available_set]
                            if missing_group:
                                raise ContractMismatchError(
                                    f"Transform payload schema mismatch: aggregate groupBy references missing columns {missing_group}",
                                    code="MISSING_COLUMN",
                                    details=_missing_column_details(
                                        op="aggregate",
                                        param_path="params.aggregate.groupBy",
                                        missing_columns=missing_group,
                                        available_columns=available_cols,
                                        available_source=available_source,
                                    ),
                                )
                            needs_column_ops = {
                                "count",
                                "count_distinct",
                                "min",
                                "max",
                                "sum",
                                "mean",
                                "avg_length",
                                "min_length",
                                "max_length",
                            }
                            missing_metrics: list[str] = []
                            missing_metric_indices: list[int] = []
                            if isinstance(metrics, list):
                                for i, metric in enumerate(metrics):
                                    if not isinstance(metric, dict):
                                        continue
                                    op_name = str(metric.get("op") or "").strip()
                                    col_name = str(metric.get("column") or "").strip()
                                    if op_name in needs_column_ops and col_name and col_name not in available_set:
                                        missing_metrics.append(col_name)
                                        missing_metric_indices.append(i)
                            if missing_metrics:
                                metric_idx = missing_metric_indices[0] if missing_metric_indices else 0
                                raise ContractMismatchError(
                                    f"Transform payload schema mismatch: aggregate metrics reference missing columns {missing_metrics}",
                                    code="MISSING_COLUMN",
                                    details=_missing_column_details(
                                        op="aggregate",
                                        param_path=f"params.aggregate.metrics.{metric_idx}.column",
                                        missing_columns=missing_metrics,
                                        available_columns=available_cols,
                                        available_source=available_source,
                                    ),
                                )
                        elif op == "join":
                            join_spec = norm.get("join") or {}
                            clauses = join_spec.get("clauses") or []
                            if not isinstance(clauses, list) or len(clauses) == 0:
                                raise ContractMismatchError(
                                    "Transform output contract mismatch: join.clauses must be a non-empty array",
                                    details=_contract_details(
                                        expected={"clausesCountMin": 1},
                                        actual={"clausesCount": int(len(clauses) if isinstance(clauses, list) else 0)},
                                    ),
                                )
                            connected_nodes = {
                                str(upstream_source_by_artifact.get(aid, ""))
                                for aid in input_artifact_ids
                                if str(upstream_source_by_artifact.get(aid, "")).strip()
                            }
                            node_columns: dict[str, list[str]] = {}
                            for connected_node in connected_nodes:
                                df = join_lookup.get(connected_node)
                                node_columns[connected_node] = (
                                    [str(c) for c in list(getattr(df, "columns", []))]
                                    if df is not None
                                    else []
                                )

                            missing_qualified: list[str] = []
                            disconnected_nodes: list[str] = []
                            for idx, clause in enumerate(clauses):
                                if not isinstance(clause, dict):
                                    missing_qualified.append(f"clause[{idx}]")
                                    continue
                                left_node = str(clause.get("leftNodeId") or "")
                                right_node = str(clause.get("rightNodeId") or "")
                                left_col = str(clause.get("leftCol") or "")
                                right_col = str(clause.get("rightCol") or "")
                                if left_node not in connected_nodes:
                                    disconnected_nodes.append(left_node)
                                if right_node not in connected_nodes:
                                    disconnected_nodes.append(right_node)
                                left_cols = set(node_columns.get(left_node, []))
                                right_cols = set(node_columns.get(right_node, []))
                                if left_col and left_col not in left_cols:
                                    missing_qualified.append(f"{left_node}.{left_col}")
                                if right_col and right_col not in right_cols:
                                    missing_qualified.append(f"{right_node}.{right_col}")

                            if disconnected_nodes:
                                raise ContractMismatchError(
                                    "Transform output contract mismatch: join clause references unconnected node(s)",
                                    code="MISSING_COLUMN",
                                    details={
                                        "errorCode": "MISSING_COLUMN",
                                        "op": "join",
                                        "paramPath": "params.join.clauses",
                                        "missingColumns": _stable_unique_strings(disconnected_nodes),
                                        "availableColumns": _stable_unique_strings(sorted(connected_nodes)),
                                        "availableColumnsSource": "schema",
                                        "expected": {"connectedNodes": _stable_unique_strings(sorted(connected_nodes))},
                                        "actual": {"referencedNodes": _stable_unique_strings(disconnected_nodes)},
                                    },
                                )
                            joined_nodes_for_plan: set[str] = set()
                            for idx, clause in enumerate(clauses):
                                if not isinstance(clause, dict):
                                    continue
                                left_node = str(clause.get("leftNodeId") or "")
                                right_node = str(clause.get("rightNodeId") or "")
                                if idx == 0:
                                    joined_nodes_for_plan.update([left_node, right_node])
                                    continue
                                left_in = left_node in joined_nodes_for_plan
                                right_in = right_node in joined_nodes_for_plan
                                if left_in == right_in:
                                    raise ContractMismatchError(
                                        "Transform output contract mismatch: each join clause must add exactly one new node",
                                        code="MISSING_COLUMN",
                                        details={
                                            "errorCode": "MISSING_COLUMN",
                                            "op": "join",
                                            "paramPath": "params.join.clauses",
                                            "missingColumns": [f"clause[{idx}]"],
                                            "availableColumns": _stable_unique_strings(sorted(connected_nodes)),
                                            "availableColumnsSource": "schema",
                                            "expected": {"clauseAddsOneNewNode": True},
                                            "actual": {
                                                "leftNodeId": left_node,
                                                "rightNodeId": right_node,
                                                "joinedNodes": _stable_unique_strings(sorted(joined_nodes_for_plan)),
                                            },
                                        },
                                    )
                                joined_nodes_for_plan.update([left_node, right_node])
                            if missing_qualified:
                                available_qualified = _stable_unique_strings([
                                    f"{nid}.{col}"
                                    for nid, cols in node_columns.items()
                                    for col in cols
                                ])
                                raise ContractMismatchError(
                                    f"Transform payload schema mismatch: join references missing columns {missing_qualified}",
                                    code="MISSING_COLUMN",
                                    details={
                                        "errorCode": "MISSING_COLUMN",
                                        "op": "join",
                                        "paramPath": "params.join.clauses",
                                        "missingColumns": _stable_unique_strings(missing_qualified),
                                        "availableColumns": available_qualified,
                                        "availableColumnsSource": "schema",
                                    },
                                )
                        elif op == "dedupe":
                            dedupe_spec = norm.get("dedupe") or {}
                            by_cols = [str(c) for c in (dedupe_spec.get("by") or []) if str(c).strip()]
                            all_columns = bool(dedupe_spec.get("allColumns", len(by_cols) == 0))
                            available_cols, available_source = _available_columns_for_port(
                                port=primary_port,
                                input_schema_cols_by_port=input_schema_cols_by_port,
                                input_columns=input_columns,
                            )
                            print(
                                "[dedupe-debug] "
                                f"runId={run_id} nodeId={node_id} "
                                f"column_names={by_cols} "
                                f"availableColumns={available_cols} "
                                f"allColumns={all_columns} "
                                f"availableColumnsSource={available_source}"
                            )
                            if (not all_columns) and len(by_cols) == 0:
                                raise ContractMismatchError(
                                    "Transform payload schema mismatch: dedupe requires selecting one or more valid columns",
                                    code="COLUMN_SELECTION_REQUIRED",
                                    details={
                                        "errorCode": "COLUMN_SELECTION_REQUIRED",
                                        "op": "dedupe",
                                        "paramPath": "params.dedupe.by",
                                        "missingColumns": [],
                                        "availableColumns": _stable_unique_strings(available_cols),
                                        "availableColumnsSource": available_source,
                                        "nodeId": str(node_id),
                                        "schemaFingerprint": str((expected_schema or {}).get("schemaFingerprint") or ""),
                                        "message": "Select one or more columns for dedupe.",
                                    },
                                )
                            available_set = set(available_cols)
                            missing_cols = [c for c in by_cols if c not in available_set]
                            if missing_cols:
                                raise ContractMismatchError(
                                    f"Transform payload schema mismatch: dedupe references missing columns {missing_cols}",
                                    code="MISSING_COLUMN",
                                    details=_missing_column_details(
                                        op="dedupe",
                                        param_path="params.dedupe.by",
                                        missing_columns=missing_cols,
                                        available_columns=available_cols,
                                        available_source=available_source,
                                    ),
                                )
                        elif op == "split":
                            split_spec = norm.get("split") or {}
                            source_col = str(split_spec.get("sourceColumn") or "text")
                            available_cols, available_source = _available_columns_for_port(
                                port=primary_port,
                                input_schema_cols_by_port=input_schema_cols_by_port,
                                input_columns=input_columns,
                            )
                            available_set = set(available_cols)
                            if source_col not in available_set:
                                raise ContractMismatchError(
                                    f"Transform payload schema mismatch: split sourceColumn missing '{source_col}'",
                                    code="MISSING_COLUMN",
                                    details=_missing_column_details(
                                        op="split",
                                        param_path="split.sourceColumn",
                                        missing_columns=[source_col],
                                        available_columns=available_cols,
                                        available_source=available_source,
                                    ),
                                )

                        try:
                            res = run_transform(params=norm, input_tables=input_tables, join_lookup=join_lookup)
                        except Exception as transform_ex:
                            if op == "derive":
                                # Best-effort precheck can miss complex SQL semantics.
                                raise ContractMismatchError(
                                    "Transform expression invalid: derive expression rejected by engine",
                                    code="EXPR_INVALID",
                                    details=_contract_details(
                                        expected={"op": "derive", "engine": "duckdb"},
                                        actual={"engineError": str(transform_ex)[:500]},
                                    ),
                                ) from transform_ex
                            raise

                        await context.bus.emit({
                            "type": "log",
                            "runId": run_id,
                            "at": iso_now(),
                            "level": "info",
                            "message": f"transform: produced {len(res.payload_bytes)} bytes, content_hash={res.meta.get('content_hash')}",
                            "nodeId": node_id,
                        })

                        # 5) store artifact bytes + cache
                        artifact_id = exec_key  # keep your convention

                        llm_output_mode = str((norm_params.get("output_mode") or ((norm_params.get("output") or {}).get("mode")) or "text")) if kind == "llm" else ""
                        llm_port_type = "text"
                        if llm_output_mode == "json":
                            llm_port_type = "json"
                        elif llm_output_mode == "embeddings":
                            llm_port_type = "embeddings"
                        created_at_dt = datetime.now(timezone.utc)
                        primary_cols_for_schema = input_columns.get(primary_port, [])
                        other_cols_for_schema: list[str] = []
                        if op == "join":
                            join_spec = norm.get("join") or {}
                            clauses = join_spec.get("clauses") or []
                            seen_nodes: list[str] = []
                            if isinstance(clauses, list):
                                for clause in clauses:
                                    if not isinstance(clause, dict):
                                        continue
                                    ln = str(clause.get("leftNodeId") or "")
                                    rn = str(clause.get("rightNodeId") or "")
                                    if ln and ln not in seen_nodes:
                                        seen_nodes.append(ln)
                                    if rn and rn not in seen_nodes:
                                        seen_nodes.append(rn)
                            merged_cols: list[str] = []
                            for nid in seen_nodes:
                                df = join_lookup.get(nid)
                                if df is None:
                                    continue
                                merged_cols.extend([str(c) for c in list(getattr(df, "columns", []))])
                            other_cols_for_schema = _stable_unique_strings(merged_cols)
                        output_cols_core = _transform_output_columns(
                            op=op,
                            norm=norm,
                            primary_cols=primary_cols_for_schema,
                            other_cols=other_cols_for_schema,
                        )
                        runtime_cols = canonical_table_columns(
                            [{"name": c, "type": "unknown"} for c in (res.meta.get("columns") or [])]
                        )
                        # Prefer runtime columns when available; deterministic fallback otherwise.
                        output_cols = runtime_cols if runtime_cols else output_cols_core

                        # Preserve input schema types for transforms where output columns are structurally derived
                        # from known input columns.
                        primary_type_by_name: dict[str, str] = {
                            str(c.get("name") or ""): str(c.get("type") or "unknown")
                            for c in (input_schema_cols_by_port.get(primary_port) or [])
                            if isinstance(c, dict) and str(c.get("name") or "").strip()
                        }
                        all_name_counts: dict[str, int] = {}
                        all_name_type: dict[str, str] = {}
                        for cols in input_schema_cols_by_port.values():
                            for c in cols or []:
                                if not isinstance(c, dict):
                                    continue
                                ncol = str(c.get("name") or "").strip()
                                if not ncol:
                                    continue
                                all_name_counts[ncol] = int(all_name_counts.get(ncol, 0)) + 1
                                if ncol not in all_name_type:
                                    all_name_type[ncol] = str(c.get("type") or "unknown")

                        rename_map = ((norm.get("rename") or {}).get("map") or {}) if op == "rename" else {}
                        rename_inv: dict[str, str] = {}
                        if isinstance(rename_map, dict):
                            for src in primary_cols_for_schema:
                                dst = str(rename_map.get(src, src))
                                rename_inv[dst] = str(src)

                        derive_names = set()
                        if op == "derive":
                            for d in ((norm.get("derive") or {}).get("columns") or []):
                                if isinstance(d, dict):
                                    name = str(d.get("name") or "").strip()
                                    if name:
                                        derive_names.add(name)

                        aggregate_group_by = set()
                        if op == "aggregate":
                            aggregate_group_by = {
                                str(c).strip()
                                for c in (((norm.get("aggregate") or {}).get("groupBy") or []))
                                if str(c).strip()
                            }

                        enriched_cols: list[dict[str, str]] = []
                        passthrough_ops = {"filter", "sort", "limit", "dedupe", "sql", "select"}
                        for c in output_cols:
                            if not isinstance(c, dict):
                                continue
                            col_name = str(c.get("name") or "").strip()
                            col_type = str(c.get("type") or "unknown").strip() or "unknown"
                            next_type = col_type
                            if col_name and col_type == "unknown":
                                if op == "rename":
                                    src = rename_inv.get(col_name, col_name)
                                    next_type = primary_type_by_name.get(src, "unknown")
                                elif op == "derive":
                                    if col_name not in derive_names:
                                        next_type = primary_type_by_name.get(col_name, "unknown")
                                elif op == "aggregate":
                                    if col_name in aggregate_group_by:
                                        next_type = primary_type_by_name.get(col_name, "unknown")
                                elif op in passthrough_ops:
                                    next_type = primary_type_by_name.get(col_name, "unknown")
                                # generic fallback: if a column name is unique across all inputs, preserve its type
                                if next_type == "unknown" and all_name_counts.get(col_name, 0) == 1:
                                    next_type = all_name_type.get(col_name, "unknown")
                            enriched_cols.append({"name": col_name, "type": next_type})
                        output_cols = canonical_table_columns(enriched_cols if enriched_cols else output_cols)
                        upstream_refs = [
                            {
                                "nodeId": input_provenance_by_port.get(port, {})
                                .get("upstream", {})
                                .get("nodeId"),
                                "port": input_provenance_by_port.get(port, {})
                                .get("upstream", {})
                                .get("port", port),
                            }
                            for port, _ in input_refs
                        ]
                        dedupe_provenance: dict[str, Any] = {}
                        if op == "dedupe":
                            dedupe_spec = norm.get("dedupe") or {}
                            by_cols = dedupe_spec.get("by") if isinstance(dedupe_spec.get("by"), list) else []
                            dedupe_provenance = {
                                "op": "dedupe",
                                "by": [str(c) for c in by_cols if str(c).strip()],
                                "allColumns": len([str(c) for c in by_cols if str(c).strip()]) == 0,
                                "keep": "first",
                            }

                        table_schema_env = _table_schema_envelope(
                            columns=output_cols,
                            row_count=(res.meta.get("row_count") if isinstance(res.meta, dict) else None),
                            provenance={
                                "sourceKind": "transform",
                                "upstream": upstream_refs,
                                **dedupe_provenance,
                            },
                        )
                        output_schema_cols = _compact_typed_columns((table_schema_env.get("table", {}) or {}).get("columns") or [])
                        output_row_count = (
                            ((table_schema_env.get("stats") or {}).get("rowCount"))
                            if isinstance(table_schema_env, dict)
                            else None
                        )
                        output_schema_msg = (
                            f"transform: output-schema op={op} rowCount={output_row_count if output_row_count is not None else 'unknown'} "
                            f"columns={output_schema_cols}"
                        )
                        await context.bus.emit({
                            "type": "log",
                            "runId": run_id,
                            "at": iso_now(),
                            "level": "info",
                            "message": output_schema_msg,
                            "nodeId": node_id,
                        })
                        print(f"[transform-output-schema] runId={run_id} nodeId={node_id} {output_schema_msg}")
                        base_payload_schema = {
                            "schema_version": 1,
                            "type": "table",
                            "columns": output_cols,
                            "schema": table_schema_env,
                        }
                        schema_fp = str((expected_schema or {}).get("schemaFingerprint") or "")
                        if not schema_fp:
                            schema_fp = contract_schema_fingerprint(
                                canonical_schema_for_contract(default_contract_for_node(n))
                            )
                        contract_fingerprint = schema_fp
                        base_payload_schema["artifactMetadataV1"] = _artifact_metadata_v1(
                            exec_key=exec_key,
                            node_id=node_id,
                            node_type=kind,
                            node_impl_version=_node_impl_version(kind),
                            params_fingerprint=node_state_hash,
                            upstream_artifact_ids=sorted([aid for _, aid in input_refs]),
                            contract_fingerprint=contract_fingerprint,
                            schema_fingerprint=schema_fp,
                            mime_type=res.mime_type,
                            port_type="table",
                            schema=table_schema_env,
                            created_at_iso=created_at_dt.isoformat(),
                            run_id=run_id,
                            graph_id=context.graph_id,
                            component_context=(
                                (n.get("data", {}) or {}).get("_componentContext")
                                if isinstance((n.get("data", {}) or {}).get("_componentContext"), dict)
                                else None
                            ),
                        )
                        artifact = Artifact(
                            artifact_id=artifact_id,
                            node_kind=kind,
                            params_hash=node_state_hash,
                            upstream_ids=sorted([aid for _, aid in input_refs]),
                            created_at=created_at_dt,
                            execution_version=context.execution_version,
                            mime_type=res.mime_type,
                            port_type="table",
                            size_bytes=len(res.payload_bytes),
                            storage_uri=f"artifact://{artifact_id}",
                            payload_schema=base_payload_schema,
                            run_id=run_id,
                            graph_id=context.graph_id,
                            node_id=node_id,
                            exec_key=exec_key,
                        )

                        committed_artifact_id = await context.artifact_store.write(artifact, res.payload_bytes)
                        await _record_consumers(
                            context=context,
                            input_artifact_ids=[aid for _, aid in input_refs],
                            consumer_run_id=run_id,
                            consumer_node_id=node_id,
                            consumer_exec_key=exec_key,
                            output_artifact_id=committed_artifact_id,
                        )

                        # bind artifact
                        _assert_binding_unchanged(
                            node_id=node_id,
                            snapshot=binding_snapshot,
                            phase="post_write_bind_transform",
                        )
                        context.bindings.bind(node_id=node_id, artifact_id=committed_artifact_id, status="computed")

                        # cache index
                        await cache.store_artifact_id(exec_key, committed_artifact_id)

                        print(f"[artifact] transform node={node_id} bytes={len(res.payload_bytes)} id={artifact_id[:10]}...")

                        # emit node_output (UI fetches by artifactId)
                        await context.bus.emit({
                            "type": "node_output",
                            "runId": run_id,
                            "nodeId": node_id,
                            "at": iso_now(),
                            "artifactId": committed_artifact_id,
                            "mimeType": res.mime_type,
                            "portType": "table",
                        })

                        # return a NodeOutput for legacy metadata flow
                        output = NodeOutput(
                            status="succeeded",
                            data=None,
                            metadata=None,
                            # metadata={
                            #     **(res.meta or {}),
                            #     "artifact_id": artifact_id,
                            #     "mime_type": res.mime_type,
                            #     "exec_key": exec_key,
                            # },
                            execution_time_ms=0.0
                        )
                elif kind == "llm":
                    print("[run_graph] LLM upstream_ids:", upstream_ids)
                    print("[run_graph] bound node ids:", [b.node_id for b in context.bindings.all()])

                    llm_in_contract = str((ports.get("in") or "text"))
                    llm_allowed_in = _allowed_ports("llm", "in")

                    if llm_in_contract not in llm_allowed_in:
                        raise ContractMismatchError(
                            f"LLM input contract mismatch: unsupported input port '{llm_in_contract}'",
                            details=_contract_details(
                                expected={"allowedInPortTypes": sorted(llm_allowed_in)},
                                actual={"inPortType": llm_in_contract},
                            ),
                        )

                    # Canonical upstream artifact list (preserve port mapping order if present)
                    llm_upstream_ids = [aid for _, aid in input_refs] if input_refs else upstream_ids

                    for upstream_id in llm_upstream_ids:
                        upstream_art = await context.artifact_store.get(upstream_id)
                        upstream_pt = _infer_artifact_port_type(upstream_art)

                        if upstream_pt not in llm_allowed_in:
                            raise ContractMismatchError(
                                f"LLM input contract mismatch: upstream artifact port_type '{upstream_pt}' is not supported",
                                details=_contract_details(
                                    expected={"allowedInPortTypes": sorted(llm_allowed_in)},
                                    actual={"artifactId": upstream_id, "portType": upstream_pt},
                                ),
                            )

                        if upstream_pt != llm_in_contract:
                            raise ContractMismatchError(
                                "LLM input contract mismatch: upstream artifact port_type does not match node in port",
                                details=_contract_details(
                                    expected={"inPortType": llm_in_contract},
                                    actual={"artifactId": upstream_id, "portType": upstream_pt},
                                ),
                            )

                    output = await exec_llm(
                        run_id,
                        n,
                        context,
                        upstream_artifact_ids=llm_upstream_ids,
                    )
                elif kind == "tool":
                    if tool_mode == "effectful" and not _tool_is_armed(params):
                        raise RuntimeError("Effectful tool requires armed=true")
                    tool_upstream_ids = [aid for _, aid in input_refs] if input_refs else upstream_ids
                    tool_in_contract = str((ports.get("in") or "json"))
                    tool_allowed_in = _allowed_ports("tool", "in", provider=tool_provider)
                    if tool_in_contract not in tool_allowed_in:
                        raise ContractMismatchError(
                            f"Tool output contract mismatch: unsupported input port '{tool_in_contract}'",
                            details=_contract_details(
                                expected={"allowedInPortTypes": sorted(tool_allowed_in)},
                                actual={"inPortType": tool_in_contract},
                                ),
                            )
                    for upstream_id in tool_upstream_ids:
                        upstream_art = await context.artifact_store.get(upstream_id)
                        upstream_pt = _infer_artifact_port_type(upstream_art)
                        if upstream_pt not in tool_allowed_in:
                            raise ContractMismatchError(
                                f"Tool output contract mismatch: upstream artifact port_type '{upstream_pt}' is not supported",
                                details=_contract_details(
                                    expected={"allowedInPortTypes": sorted(tool_allowed_in)},
                                    actual={"artifactId": upstream_id, "portType": upstream_pt},
                                ),
                            )
                        if upstream_pt != tool_in_contract:
                            raise ContractMismatchError(
                                "Tool output contract mismatch: upstream artifact port_type does not match node in port",
                                details=_contract_details(
                                    expected={"inPortType": tool_in_contract},
                                    actual={"artifactId": upstream_id, "portType": upstream_pt},
                                ),
                            )

                    tool_params_runtime = dict(params)
                    tool_params_runtime["_request_fingerprint"] = exec_key
                    if tool_mode == "idempotent":
                        tool_params_runtime["_idempotency_key"] = exec_key

                    tool_node = dict(n)
                    tool_data = dict(n.get("data", {}))
                    tool_data["params"] = tool_params_runtime
                    tool_node["data"] = tool_data

                    output = await exec_tool(
                        run_id,
                        tool_node,
                        context,
                        upstream_artifact_ids=tool_upstream_ids,
                    )
                else:
                    raise RuntimeError(f"Unknown node kind: {kind}")

                # Validate output
                if output.status == "failed":
                    raise RuntimeError(output.error or "Node execution failed")

                # Store output for legacy flow / UI
                context.outputs[node_id] = output

                if kind == "transform":
                    await context.bus.emit({
                        "type": "node_finished",
                        "runId": run_id,
                        "at": iso_now(),
                        "nodeId": node_id,
                        "status": output.status,
                        "execution_time_ms": max(0.0, float(getattr(output, "execution_time_ms", 0.0) or 0.0)),
                    })
                else:
                    # ---- Artifact write + binding ----
                    mime_type = "application/octet-stream"
                    payload_bytes: bytes
                    data_value = getattr(output, "data", None)

                    if kind == "source":
                        declared_source_out = (((n.get("data", {}) or {}).get("ports", {}) or {}).get("out"))
                        out_contract = str(
                            (declared_source_out if declared_source_out else None)
                            or (params.get("output_mode") if isinstance(params, dict) else None)
                            or (
                                (params.get("output") or {}).get("mode")
                                if isinstance(params, dict) and isinstance(params.get("output"), dict)
                                else None
                            )
                            or "table"
                        )

                        if out_contract == "table":
                            rows = data_value
                            if not isinstance(rows, list) or any(not isinstance(r, dict) for r in rows):
                                raise RuntimeError(
                                    f"Source output contract mismatch: out=table expects list[dict], got {type(rows)}"
                                )

                            import io
                            import pandas as pd

                            df = pd.DataFrame(rows)
                            buf = io.StringIO()
                            df.to_csv(buf, index=False, lineterminator="\n")
                            payload_bytes = buf.getvalue().encode("utf-8")
                            mime_type = "text/csv; charset=utf-8"
                        elif out_contract == "json":
                            if not isinstance(data_value, (dict, list)):
                                raise RuntimeError(
                                    f"Source output contract mismatch: out=json expects object/array, got {type(data_value)}"
                                )
                            payload_bytes = json.dumps(data_value, ensure_ascii=False).encode("utf-8")
                            mime_type = "application/json"
                        elif out_contract == "text":
                            if not isinstance(data_value, str):
                                raise RuntimeError(
                                    f"Source output contract mismatch: out=text expects str, got {type(data_value)}"
                                )
                            payload_bytes = data_value.encode("utf-8")
                            mime_type = "text/plain; charset=utf-8"
                        elif out_contract == "binary":
                            if not isinstance(data_value, (bytes, bytearray)):
                                raise RuntimeError(
                                    f"Source output contract mismatch: out=binary expects bytes, got {type(data_value)}"
                                )
                            payload_bytes = bytes(data_value)
                            source_meta = getattr(output, "metadata", None)
                            source_meta_mime = (
                                str(getattr(source_meta, "mime_type", "")).strip()
                                if source_meta is not None
                                else ""
                            )
                            mime_type = source_meta_mime or "application/octet-stream"
                        else:
                            raise RuntimeError(
                                f"Source output contract mismatch: unsupported out port '{out_contract}'"
                            )

                    elif kind == "llm":
                        output_cfg = params.get("output") if isinstance(params, dict) else {}
                        output_mode = str(
                            (params.get("output_mode") if isinstance(params, dict) else None)
                            or (output_cfg.get("mode") if isinstance(output_cfg, dict) else None)
                            or "text"
                        )
                        out_contract = "text"
                        if output_mode == "json":
                            out_contract = "json"
                        elif output_mode == "embeddings":
                            out_contract = "embeddings"

                        if out_contract == "json":
                            if data_value is None:
                                raise RuntimeError("LLM output contract mismatch: out=json expects non-empty JSON content")
                            if isinstance(data_value, bytes):
                                try:
                                    raw_json = data_value.decode("utf-8")
                                except Exception:
                                    raise RuntimeError("LLM output contract mismatch: out=json requires utf-8 decodable bytes")
                            else:
                                raw_json = data_value if isinstance(data_value, str) else json.dumps(data_value, ensure_ascii=False)
                            try:
                                parsed_json = json.loads(raw_json)
                            except Exception:
                                raise RuntimeError("LLM output contract mismatch: out=json expects valid JSON")
                            payload_bytes = json.dumps(parsed_json, ensure_ascii=False).encode("utf-8")
                            mime_type = "application/json"
                            data_value = parsed_json
                        elif out_contract == "embeddings":
                            if data_value is None:
                                raise RuntimeError("LLM output contract mismatch: out=embeddings expects JSON payload")
                            if isinstance(data_value, bytes):
                                try:
                                    raw_json = data_value.decode("utf-8")
                                except Exception:
                                    raise RuntimeError(
                                        "LLM output contract mismatch: out=embeddings requires utf-8 decodable bytes"
                                    )
                                try:
                                    parsed_embeddings = json.loads(raw_json)
                                except Exception:
                                    raise RuntimeError(
                                        "LLM output contract mismatch: out=embeddings expects valid JSON payload"
                                    )
                            elif isinstance(data_value, str):
                                try:
                                    parsed_embeddings = json.loads(data_value)
                                except Exception:
                                    raise RuntimeError(
                                        "LLM output contract mismatch: out=embeddings expects valid JSON payload"
                                    )
                            elif isinstance(data_value, dict):
                                parsed_embeddings = data_value
                            else:
                                raise RuntimeError(
                                    f"LLM output contract mismatch: out=embeddings expects object/string, got {type(data_value)}"
                                )
                            payload_bytes = json.dumps(parsed_embeddings, ensure_ascii=False).encode("utf-8")
                            mime_type = "application/json"
                            data_value = parsed_embeddings
                        elif out_contract == "text":
                            if data_value is None:
                                raise RuntimeError("LLM output contract mismatch: out=text expects str content")
                            if isinstance(data_value, bytes):
                                try:
                                    text_value = data_value.decode("utf-8")
                                except Exception:
                                    raise RuntimeError("LLM output contract mismatch: out=text requires utf-8 decodable bytes")
                            elif isinstance(data_value, str):
                                text_value = data_value
                            else:
                                raise RuntimeError(f"LLM output contract mismatch: out=text expects str, got {type(data_value)}")
                            payload_bytes = text_value.encode("utf-8")
                            mime_type = "text/plain; charset=utf-8"
                            data_value = text_value
                        else:
                            raise RuntimeError(
                                f"LLM output contract mismatch: unsupported out port '{out_contract}'"
                            )

                    elif kind == "tool":
                        ports = (n.get("data", {}).get("ports", {}) or {})
                        out_contract = ports.get("out") or "json"
                        envelope = data_value if isinstance(data_value, dict) else {
                            "kind": "json",
                            "payload": data_value,
                            "meta": {"status": "ok"},
                        }
                        envelope_kind = str(envelope.get("kind") or "json")
                        payload = envelope.get("payload")
                        envelope_mime = envelope.get("mime") or envelope.get("content_type")
                        envelope_mime = (
                            str(envelope_mime).strip()
                            if isinstance(envelope_mime, str) and str(envelope_mime).strip()
                            else None
                        )

                        if out_contract == "json" and envelope_kind != "json":
                            raise RuntimeError(
                                f"Tool output contract mismatch: out=json expects envelope kind json, got {envelope_kind}"
                            )
                        if out_contract == "text" and envelope_kind != "text":
                            raise RuntimeError(
                                f"Tool output contract mismatch: out=text expects envelope kind text, got {envelope_kind}"
                            )
                        if out_contract == "binary" and envelope_kind != "binary":
                            raise RuntimeError(
                                f"Tool output contract mismatch: out=binary expects envelope kind binary, got {envelope_kind}"
                            )

                        if envelope_kind == "binary":
                            if isinstance(payload, bytes):
                                payload_bytes = payload
                            elif isinstance(payload, str):
                                payload_bytes = payload.encode("utf-8")
                            else:
                                payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                            mime_type = envelope_mime or "application/octet-stream"
                        elif envelope_kind == "text":
                            if isinstance(payload, str):
                                payload_bytes = payload.encode("utf-8")
                            else:
                                payload_bytes = str(payload).encode("utf-8")
                            mime_type = envelope_mime or "text/plain; charset=utf-8"
                        else:
                            payload_bytes = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
                            mime_type = "application/json"

                    else:
                        if isinstance(data_value, bytes):
                            payload_bytes = data_value
                            mime_type = "application/octet-stream"
                        elif isinstance(data_value, str):
                            payload_bytes = data_value.encode("utf-8")
                            mime_type = "text/plain; charset=utf-8"
                        elif data_value is None:
                            payload_bytes = b""
                            mime_type = "application/json"
                        else:
                            payload_bytes = json.dumps(data_value, ensure_ascii=False).encode("utf-8")
                            mime_type = "application/json"

                    artifact_params_hash = (
                        node_state_hash
                    )
                    artifact_port_type = ((n.get("data", {}).get("ports", {}) or {}).get("out"))
                    if kind == "source":
                        declared_source_out = (((n.get("data", {}) or {}).get("ports", {}) or {}).get("out"))
                        artifact_port_type = str(
                            (declared_source_out if declared_source_out else None)
                            or (params.get("output_mode") if isinstance(params, dict) else None)
                            or (
                                (params.get("output") or {}).get("mode")
                                if isinstance(params, dict) and isinstance(params.get("output"), dict)
                                else None
                            )
                            or artifact_port_type
                            or "table"
                        )
                    if kind == "llm":
                        llm_output_mode = str((params.get("output_mode") or ((params.get("output") or {}).get("mode")) or "text"))
                        if llm_output_mode == "json":
                            artifact_port_type = "json"
                        elif llm_output_mode == "embeddings":
                            artifact_port_type = "embeddings"
                        else:
                            artifact_port_type = "text"

                    base_payload_schema = (
                        _source_payload_schema(
                            str(artifact_port_type or "table"),
                            data_value,
                            output.metadata if kind == "source" else None,
                        )
                        if kind == "source"
                        else _llm_payload_schema(mime_type, data_value)
                        if kind == "llm"
                        else _tool_payload_schema(
                            str(data_value.get("kind") or "json") if isinstance(data_value, dict) else "json",
                            data_value.get("payload") if isinstance(data_value, dict) else data_value,
                        )
                        if kind == "tool"
                        else None
                    ) or {}
                    if kind in {"source", "llm", "tool"}:
                        debug_payload = data_value
                        if kind == "tool" and isinstance(data_value, dict):
                            debug_payload = data_value.get("payload")
                        _emit_external_schema_debug(
                            kind=str(kind),
                            node_id=str(node_id),
                            schema=base_payload_schema if isinstance(base_payload_schema, dict) else {},
                            payload=debug_payload,
                        )
                    table_schema_env: Optional[Dict[str, Any]] = None
                    if kind == "source" and str(artifact_port_type or "").lower() == "table":
                        payload_cols = _extract_table_columns_from_payload_schema(base_payload_schema)
                        row_count = None
                        if isinstance(base_payload_schema.get("row_count"), int):
                            row_count = int(base_payload_schema.get("row_count"))
                        elif isinstance(data_value, list):
                            row_count = len(data_value)
                        table_schema_env = _table_schema_envelope(
                            columns=payload_cols,
                            row_count=row_count,
                            provenance=_source_table_provenance(n, params if isinstance(params, dict) else {}),
                            coercion=base_payload_schema.get("coercion")
                            if isinstance(base_payload_schema.get("coercion"), dict)
                            else None,
                        )
                        base_payload_schema["schema"] = table_schema_env
                    created_at_dt = datetime.now(timezone.utc)
                    schema_fp = str((expected_schema or {}).get("schemaFingerprint") or "")
                    if not schema_fp:
                        schema_fp = contract_schema_fingerprint(
                            canonical_schema_for_contract(default_contract_for_node(n))
                        )
                    contract_fingerprint = schema_fp
                    base_payload_schema["artifactMetadataV1"] = _artifact_metadata_v1(
                        exec_key=exec_key,
                        node_id=node_id,
                        node_type=kind,
                        node_impl_version=_node_impl_version(kind),
                        params_fingerprint=artifact_params_hash,
                        upstream_artifact_ids=sorted(upstream_ids),
                        contract_fingerprint=contract_fingerprint,
                        schema_fingerprint=schema_fp,
                        mime_type=mime_type,
                        port_type=artifact_port_type,
                        schema=table_schema_env,
                        created_at_iso=created_at_dt.isoformat(),
                        run_id=run_id,
                        graph_id=context.graph_id,
                        component_context=(
                            (n.get("data", {}) or {}).get("_componentContext")
                            if isinstance((n.get("data", {}) or {}).get("_componentContext"), dict)
                            else None
                        ),
                    )

                    artifact = Artifact(
                        artifact_id=artifact_id,
                        node_kind=kind,
                        params_hash=artifact_params_hash,
                        upstream_ids=sorted(upstream_ids),
                        created_at=created_at_dt,
                        execution_version=context.execution_version,
                        mime_type=mime_type,
                        port_type=artifact_port_type,
                        size_bytes=len(payload_bytes),
                        storage_uri=f"artifact://{artifact_id}",
                        payload_schema=base_payload_schema,
                        run_id=run_id,
                        graph_id=context.graph_id,
                        node_id=node_id,
                        exec_key=exec_key,
                    )

                    committed_artifact_id = await context.artifact_store.write(artifact, payload_bytes)
                    await _record_consumers(
                        context=context,
                        input_artifact_ids=upstream_ids,
                        consumer_run_id=run_id,
                        consumer_node_id=node_id,
                        consumer_exec_key=exec_key,
                        output_artifact_id=committed_artifact_id,
                    )
                    _assert_binding_unchanged(
                        node_id=node_id,
                        snapshot=binding_snapshot,
                        phase="post_write_bind",
                    )
                    context.bindings.bind(node_id=node_id, artifact_id=committed_artifact_id, status="computed")
                    await context.bus.emit({
                        "type": "node_output",
                        "runId": run_id,
                        "nodeId": node_id,
                        "at": iso_now(),
                        "artifactId": committed_artifact_id,
                        "mimeType": artifact.mime_type,
                        "portType": artifact.port_type,
                    })

                    # Update cache index
                    if use_cache_for_node:
                        await cache.store_artifact_id(exec_key, committed_artifact_id)

                    # Verification print
                    print(f"[artifact] node={node_id} kind={kind} bytes={len(payload_bytes)} \n\tid={artifact_id}...")

                    await context.bus.emit({
                        "type": "node_finished",
                        "runId": run_id,
                        "at": iso_now(),
                        "nodeId": node_id,
                        "status": output.status,
                        "execution_time_ms": max(0.0, float(getattr(output, "execution_time_ms", 0.0) or 0.0)),
                    })

            except asyncio.CancelledError:
                await context.bus.emit({
                    "type": "node_cancelled",
                    "runId": run_id,
                    "at": iso_now(),
                    "nodeId": node_id,
                    "status": "cancelled",
                })
                await context.bus.emit({
                    "type": "node_finished",
                    "runId": run_id,
                    "at": iso_now(),
                    "nodeId": node_id,
                    "status": "cancelled",
                    "execution_time_ms": max(0.0, (asyncio.get_running_loop().time() - node_started_t) * 1000.0),
                })
                return {"ok": False, "cached": False, "cancelled": True}
            except Exception as ex:
                traceback.print_exc()
                error_message = str(ex)
                error_details: Dict[str, Any] = {}
                error_code: Optional[str] = None
                if isinstance(ex, ContractMismatchError):
                    error_code = ex.code
                    error_details = dict(ex.details or {})
                elif _is_contract_mismatch_error(error_message):
                    error_code = (
                        "PAYLOAD_SCHEMA_MISMATCH"
                        if "payload schema mismatch" in error_message.lower()
                        else "CONTRACT_MISMATCH"
                    )
                if kind == "transform" and error_code in {"PAYLOAD_SCHEMA_MISMATCH", "CONTRACT_MISMATCH", "MISSING_COLUMN", "DUPLICATE_COLUMN", "COLUMN_SELECTION_REQUIRED", "EXPR_INVALID"}:
                    expected_s, actual_s = _compact_expected_actual(error_details)
                    mismatch_msg = (
                        f"transform: schema-mismatch code={error_code} "
                        f"missingColumns={_stable_unique_strings(error_details.get('missingColumns') or [])} "
                        f"paramPath={str(error_details.get('paramPath') or '')} "
                        f"expected={expected_s} actual={actual_s}"
                    )
                    await context.bus.emit({
                        "type": "log",
                        "runId": run_id,
                        "at": iso_now(),
                        "level": "error",
                        "message": mismatch_msg,
                        "nodeId": node_id,
                    })
                    print(f"[schema-mismatch] runId={run_id} nodeId={node_id} {mismatch_msg}")

                await context.bus.emit({
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "error",
                    "message": error_message,
                    "nodeId": node_id
                })
                await context.bus.emit({
                    "type": "node_finished",
                    "runId": run_id,
                    "at": iso_now(),
                    "nodeId": node_id,
                    "status": "failed",
                    "error": error_message,
                    **({"errorCode": error_code} if error_code else {}),
                    **({"errorDetails": error_details} if error_details else {}),
                    "execution_time_ms": max(0.0, (asyncio.get_running_loop().time() - node_started_t) * 1000.0),
                })
                if error_details:
                    await context.bus.emit({
                        "type": "log",
                        "runId": run_id,
                        "at": iso_now(),
                        "level": "error",
                        "message": json.dumps(
                            {
                                "code": (ex.code if isinstance(ex, ContractMismatchError) else "CONTRACT_MISMATCH"),
                                "errorCode": error_code,
                                "missingColumns": error_details.get("missingColumns", []),
                                "availableColumns": error_details.get("availableColumns", []),
                                "paramPath": error_details.get("paramPath"),
                                "expected": error_details.get("expected"),
                                "actual": error_details.get("actual"),
                            },
                            ensure_ascii=False,
                        ),
                        "nodeId": node_id,
                    })
                return {"ok": False, "cached": False}

            # Mark incoming edges as done
            for edge_id in plan.incoming_edges.get(node_id, []):
                await context.bus.emit({
                    "type": "edge_exec",
                    "runId": run_id,
                    "at": iso_now(),
                    "edgeId": edge_id,
                    "exec": "done"
                })

            await asyncio.sleep(0.05)
            return {"ok": True, "cached": False}

        levels = _plan_levels(plan, edges)
        run_t0 = asyncio.get_running_loop().time()
        max_inflight = _env_int("RUNNER_MAX_CONCURRENCY", 4, minimum=1)
        max_source = _env_int("RUNNER_MAX_SOURCE", 2, minimum=1)
        max_transform = _env_int("RUNNER_MAX_TRANSFORM", 2, minimum=1)
        max_llm = _env_int("RUNNER_MAX_LLM", 2, minimum=1)
        max_tool = _env_int("RUNNER_MAX_TOOL", 2, minimum=1)
        global_sem = asyncio.Semaphore(max_inflight)
        kind_sems = {
            "source": asyncio.Semaphore(max_source),
            "transform": asyncio.Semaphore(max_transform),
            "llm": asyncio.Semaphore(max_llm),
            "tool": asyncio.Semaphore(max_tool),
        }

        inflight_current = 0
        peak_concurrency = 0
        total_cached = 0
        total_succeeded = 0
        total_failed = 0

        async def _run_with_limits(node_id: str) -> Dict[str, Any]:
            nonlocal inflight_current, peak_concurrency
            n = nodes[node_id]
            kind = n.get("data", {}).get("kind")
            kind_sem = kind_sems.get(kind)
            t0 = asyncio.get_running_loop().time()
            await _component_mark_node_start(node_id)
            async with global_sem:
                inflight_current += 1
                if inflight_current > peak_concurrency:
                    peak_concurrency = inflight_current
                if kind_sem is None:
                    try:
                        result = await _execute_node(node_id)
                        await _component_mark_node_finish(
                            node_id,
                            ok=bool(result.get("ok")),
                            error=None if result.get("ok") else str(result.get("error") or ""),
                        )
                        return result
                    except asyncio.CancelledError:
                        await context.bus.emit({
                            "type": "node_finished",
                            "runId": run_id,
                            "at": iso_now(),
                            "nodeId": node_id,
                            "status": "cancelled",
                            "execution_time_ms": max(0.0, (asyncio.get_running_loop().time() - t0) * 1000.0),
                        })
                        await _component_mark_node_finish(node_id, ok=False, error="cancelled")
                        return {"ok": False, "cached": False, "cancelled": True}
                    except Exception as ex:
                        await context.bus.emit({
                            "type": "node_finished",
                            "runId": run_id,
                            "at": iso_now(),
                            "nodeId": node_id,
                            "status": "failed",
                            "error": str(ex),
                            "execution_time_ms": max(0.0, (asyncio.get_running_loop().time() - t0) * 1000.0),
                        })
                        await _component_mark_node_finish(node_id, ok=False, error=str(ex))
                        return {"ok": False, "cached": False}
                    finally:
                        inflight_current -= 1
                try:
                    async with kind_sem:
                        try:
                            result = await _execute_node(
                                node_id,
                                cache_only=(node_id in plan.cache_only_nodes),
                            )
                            await _component_mark_node_finish(
                                node_id,
                                ok=bool(result.get("ok")),
                                error=None if result.get("ok") else str(result.get("error") or ""),
                            )
                            return result
                        except asyncio.CancelledError:
                            await context.bus.emit({
                                "type": "node_finished",
                                "runId": run_id,
                                "at": iso_now(),
                                "nodeId": node_id,
                                "status": "cancelled",
                                "execution_time_ms": max(0.0, (asyncio.get_running_loop().time() - t0) * 1000.0),
                            })
                            await _component_mark_node_finish(node_id, ok=False, error="cancelled")
                            return {"ok": False, "cached": False, "cancelled": True}
                        except Exception as ex:
                            await context.bus.emit({
                                "type": "node_finished",
                                "runId": run_id,
                                "at": iso_now(),
                                "nodeId": node_id,
                                "status": "failed",
                                "error": str(ex),
                                "execution_time_ms": max(0.0, (asyncio.get_running_loop().time() - t0) * 1000.0),
                            })
                            await _component_mark_node_finish(node_id, ok=False, error=str(ex))
                            return {"ok": False, "cached": False}
                finally:
                    inflight_current -= 1

        run_failed = False
        for level_idx, level_nodes in enumerate(levels, start=1):
            if cancel_event and cancel_event.is_set():
                await context.bus.emit({
                    "type": "scheduler_cancelled",
                    "runId": run_id,
                    "at": iso_now(),
                    "levelIndex": level_idx,
                    "scheduled": 0,
                    "inflightCancelled": 0,
                    "completedBeforeCancel": 0,
                })
                await context.bus.emit({
                    "type": "run_cancelled",
                    "runId": run_id,
                    "at": iso_now(),
                })
                await context.bus.emit({
                    "type": "run_finished",
                    "runId": run_id,
                    "at": iso_now(),
                    "status": "cancelled"
                })
                await _emit_cache_summary_once()
                return

            kind_counts: Dict[str, int] = {"source": 0, "transform": 0, "llm": 0, "tool": 0}
            for nid in level_nodes:
                k = (nodes.get(nid, {}).get("data", {}) or {}).get("kind")
                if k in kind_counts:
                    kind_counts[k] += 1

            await context.bus.emit({
                "type": "level_started",
                "runId": run_id,
                "at": iso_now(),
                "levelIndex": level_idx,
                "nodesInLevel": len(level_nodes),
                "globalCap": max_inflight,
                "caps": {
                    "source": max_source,
                    "transform": max_transform,
                    "llm": max_llm,
                    "tool": max_tool,
                },
                "kindCounts": kind_counts,
            })
            await context.bus.emit({
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "info",
                "message": (
                    f"[scheduler] level {level_idx} start nodes={len(level_nodes)} "
                    f"caps(g={max_inflight},s={max_source},t={max_transform},l={max_llm},tool={max_tool}) "
                    f"kinds={kind_counts}"
                ),
            })

            level_t0 = asyncio.get_running_loop().time()
            tasks = []
            for nid in level_nodes:
                if cancel_event and cancel_event.is_set():
                    break
                tasks.append(asyncio.create_task(_run_with_limits(nid)))

            cancelled_tasks = 0
            if cancel_event and cancel_event.is_set():
                for t in tasks:
                    if not t.done():
                        t.cancel()
                        cancelled_tasks += 1

            raw_results = []
            if tasks:
                pending = set(tasks)
                while pending:
                    if cancel_event and cancel_event.is_set():
                        for t in pending:
                            if not t.done():
                                t.cancel()
                                cancelled_tasks += 1
                    done, pending = await asyncio.wait(
                        pending,
                        timeout=0.05,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for d in done:
                        try:
                            raw_results.append(d.result())
                        except asyncio.CancelledError as ce:
                            raw_results.append(ce)
                        except Exception as ex:
                            raw_results.append(ex)
            results = []
            for r in raw_results:
                if isinstance(r, asyncio.CancelledError):
                    results.append({"ok": False, "cached": False, "cancelled": True})
                elif isinstance(r, Exception):
                    raise r
                else:
                    results.append(r)
            level_cached = sum(1 for r in results if r.get("cached"))
            level_failed = sum(1 for r in results if not r.get("ok"))
            level_cancelled = sum(1 for r in results if r.get("cancelled"))
            level_succeeded = len(results) - level_failed - level_cached
            total_cached += level_cached
            total_failed += level_failed
            total_succeeded += level_succeeded
            level_elapsed_ms = int((asyncio.get_running_loop().time() - level_t0) * 1000)

            await context.bus.emit({
                "type": "level_finished",
                "runId": run_id,
                "at": iso_now(),
                "levelIndex": level_idx,
                "succeeded": level_succeeded,
                "failed": level_failed,
                "skipped": level_cached,
                "elapsedMs": level_elapsed_ms,
            })
            await context.bus.emit({
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "info",
                "message": (
                    f"[scheduler] level {level_idx} done ok={level_succeeded} "
                    f"failed={level_failed} cached={level_cached} elapsed_ms={level_elapsed_ms}"
                ),
            })

            if cancel_event and cancel_event.is_set():
                await context.bus.emit({
                    "type": "scheduler_cancelled",
                    "runId": run_id,
                    "at": iso_now(),
                    "levelIndex": level_idx,
                    "scheduled": len(tasks),
                    "inflightCancelled": max(cancelled_tasks, level_cancelled),
                    "completedBeforeCancel": max(0, len(results) - level_cancelled),
                })
                await context.bus.emit({
                    "type": "run_cancelled",
                    "runId": run_id,
                    "at": iso_now(),
                })
                await context.bus.emit({
                    "type": "run_finished",
                    "runId": run_id,
                    "at": iso_now(),
                    "status": "cancelled"
                })
                await _emit_cache_summary_once()
                return

            if level_failed > 0:
                run_failed = True
                break

        total_runtime_ms = int((asyncio.get_running_loop().time() - run_t0) * 1000)
        await context.bus.emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "info",
            "message": (
                f"[scheduler] summary executed={total_succeeded + total_failed} "
                f"cached={total_cached} failed={total_failed} "
                f"peak_concurrency={peak_concurrency} runtime_ms={total_runtime_ms}"
            ),
        })
        await _emit_cache_summary_once()

        if run_failed:
            await context.bus.emit({
                "type": "run_finished",
                "runId": run_id,
                "at": iso_now(),
                "status": "failed"
            })
            await _emit_cache_summary_once()
            return

        await context.bus.emit({
            "type": "run_finished",
            "runId": run_id,
            "at": iso_now(),
            "status": "succeeded"
        })
        await _emit_cache_summary_once()
    except asyncio.CancelledError:
        await context.bus.emit({
            "type": "run_cancelled",
            "runId": run_id,
            "at": iso_now(),
        })
        await context.bus.emit({
            "type": "run_finished",
            "runId": run_id,
            "at": iso_now(),
            "status": "cancelled"
        })
        await _emit_cache_summary_once()
        return
    except Exception as ex:
        traceback.print_exc()
        await context.bus.emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "error",
            "message": str(ex)
        })
        await context.bus.emit({
            "type": "run_finished",
            "runId": run_id,
            "at": iso_now(),
            "status": "failed"
        })
        await _emit_cache_summary_once()
