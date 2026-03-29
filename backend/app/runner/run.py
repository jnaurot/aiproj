import asyncio
import copy
import contextvars
import hashlib
import inspect
import json
import re
import traceback
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.runner.nodes.transform import (
    normalize_transform_params,
    canonical_json,
    load_table_from_artifact_bytes,
    load_table_from_json_bytes,
    load_table_from_text_bytes,
    run_transform,
    sha256_hex,
)


from .compile import compile_plan
from .components import ComponentExpansionError, expand_graph_components
from .events import RunEventBus
from .validator import GraphValidator, collect_transitive_descendants
from .metadata import GraphContext, NodeOutput
from .artifacts import Artifact, MemoryArtifactStore, RunBindings
from .cache import ExecutionCache
from .queues import QueueLimits, QueueRegistry, next_nonempty_key
from .node_state import build_exec_key, build_node_state_hash, build_source_fingerprint
from .contracts import (
    TABLE_V1,
    canonical_table_columns,
    canonical_schema_for_contract,
    default_contract_for_node,
    schema_fingerprint as contract_schema_fingerprint,
)
from .schema_infer import get_schema_infer_stats, infer_json_schema_cached

from ..executors.source import exec_source
from ..executors.llm import exec_llm
from ..executors.tool import exec_tool
from ..executors.builtin_profiles import (
    BUILTIN_PROFILE_INSTALL_TARGETS,
    BUILTIN_PROFILE_PACKAGES,
    missing_packages_for_packages,
    resolve_builtin_environment,
)
from ..feature_flags import get_feature_flags

logger = logging.getLogger(__name__)

_EXECUTOR_CODE_HASH_CACHE: Dict[tuple[str, int], str] = {}


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def node_map(graph: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {n["id"]: n for n in graph.get("nodes", [])}


def edge_map(graph: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {e["id"]: e for e in graph.get("edges", []) if "id" in e}


def upstream_node_ids(edges: Dict[str, Dict[str, Any]], node_id: str) -> list[str]:
    return [e["source"] for e in edges.values() if e.get("target") == node_id]

def _resolve_component_output_artifact_from_bindings(
    *,
    src_node: Dict[str, Any],
    component_instance_node_id: str,
    output_name: str,
    get_current_artifact,
) -> Dict[str, Any]:
    params = (src_node.get("data") or {}).get("params")
    if not isinstance(params, dict):
        return {"artifactId": None, "hasBinding": False, "runtimeNodeId": None}
    bindings = params.get("bindings") if isinstance(params.get("bindings"), dict) else {}
    outputs = bindings.get("outputs") if isinstance(bindings.get("outputs"), dict) else {}
    binding = outputs.get(output_name) if isinstance(outputs, dict) else None
    if not isinstance(binding, dict):
        return {"artifactId": None, "hasBinding": False, "runtimeNodeId": None}
    bound_node_id = str(binding.get("nodeId") or "").strip()
    if not bound_node_id:
        return {"artifactId": None, "hasBinding": True, "runtimeNodeId": None}
    runtime_node_id = (
        bound_node_id if bound_node_id.startswith("cmp:") else f"cmp:{component_instance_node_id}:{bound_node_id}"
    )
    aid = get_current_artifact(runtime_node_id)
    resolved = str(aid or "").strip() or None
    return {"artifactId": resolved, "hasBinding": True, "runtimeNodeId": runtime_node_id}


def _resolve_component_output_artifact_from_output_edges(
    *,
    edges: Dict[str, Dict[str, Any]],
    component_instance_node_id: str,
    output_name: str,
    get_current_artifact,
) -> Dict[str, Any]:
    candidates: list[Dict[str, Any]] = []
    for e in edges.values():
        if str(e.get("target") or "").strip() != component_instance_node_id:
            continue
        handle = str(e.get("targetHandle") or "out").strip() or "out"
        if handle != output_name:
            continue
        src = str(e.get("source") or "").strip()
        if not src:
            continue
        candidates.append(e)
    if not candidates:
        return {"artifactId": None, "runtimeNodeId": None, "edgeId": None, "edgeCount": 0}
    candidates.sort(key=lambda edge: str(edge.get("id") or ""))
    chosen = candidates[0]
    runtime_node_id = str(chosen.get("source") or "").strip()
    aid = get_current_artifact(runtime_node_id) if runtime_node_id else None
    resolved = str(aid or "").strip() or None
    return {
        "artifactId": resolved,
        "runtimeNodeId": runtime_node_id or None,
        "edgeId": str(chosen.get("id") or ""),
        "edgeCount": len(candidates),
    }


def _normalize_typed_schema_for_runtime(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    typed_type = str(raw.get("type") or "").strip().lower()
    if typed_type == "string":
        typed_type = "text"
    if typed_type not in {"table", "json", "text", "binary", "embeddings", "unknown"}:
        return None
    fields_raw = raw.get("fields") if isinstance(raw.get("fields"), list) else []
    fields = []
    for field in fields_raw:
        if not isinstance(field, dict):
            continue
        name = str(field.get("name") or "").strip()
        if not name:
            continue
        f_type = str(field.get("type") or "unknown").strip().lower() or "unknown"
        if f_type == "string":
            f_type = "text"
        fields.append(
            {
                "name": name,
                "type": f_type,
                "nullable": bool(field.get("nullable", True)),
            }
        )
    if typed_type != "table":
        fields = []
    return {"type": typed_type, "fields": fields}


def _typed_schema_type_to_payload_type(typed_schema: Optional[Dict[str, Any]]) -> str:
    t = str((typed_schema or {}).get("type") or "").strip().lower()
    if t in {"table", "json", "text", "binary", "embeddings", "image", "audio", "video"}:
        return t
    return "unknown"


async def _component_wrapper_output_typed_schema(
    *,
    artifact_store: Any,
    artifact_id: str,
    output_name: str,
) -> Optional[Dict[str, Any]]:
    if artifact_store is None or not hasattr(artifact_store, "read"):
        return None
    aid = str(artifact_id or "").strip()
    out_name = str(output_name or "").strip()
    if not aid or not out_name:
        return None
    try:
        payload_bytes = await artifact_store.read(aid)
    except Exception:
        return None
    if not isinstance(payload_bytes, (bytes, bytearray)):
        return None
    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception:
        return None
    root = payload if isinstance(payload, dict) else {}
    outputs = root.get("outputs") if isinstance(root.get("outputs"), dict) else {}
    out = outputs.get(out_name) if isinstance(outputs, dict) else None
    if not isinstance(out, dict):
        return None
    observed = _normalize_typed_schema_for_runtime(out.get("typedSchemaObserved"))
    if observed is not None:
        return observed
    declared = _normalize_typed_schema_for_runtime(out.get("typedSchemaExpected"))
    if declared is not None:
        return declared
    fallback = _normalize_typed_schema_for_runtime(out.get("typedSchema"))
    return fallback


async def resolve_input_refs(
    edges: Dict[str, Dict[str, Any]],
    node_id: str,
    get_current_artifact,
    get_node_by_id,
    artifact_store,
) -> list[tuple[str, str]]:
    """
    Returns stable (inputHandle, upstreamArtifactId) pairs for edges targeting node_id.
    inputHandle is taken from edge.targetHandle if present; else 'in'.
    Only includes edges whose source node has produced an artifact via bindings.
    """
    refs: list[tuple[str, str]] = []
    def _get_artifact_for_source(node_id: str, source_handle: str = "out"):
        # Backward-compatible: some tests and callers still pass a single-arg getter.
        try:
            return get_current_artifact(node_id, source_handle)
        except TypeError:
            return get_current_artifact(node_id)

    for e in edges.values():
        if e.get("target") != node_id:
            continue
        src = e.get("source")
        if not src:
            continue
        source_handle = str(e.get("sourceHandle") or "out").strip() or "out"
        aid = _get_artifact_for_source(src, source_handle)
        if not aid:
            continue
        src_node = get_node_by_id(src) or {}
        src_kind = str(((src_node.get("data") or {}).get("kind") or "")).strip().lower()
        if src_kind == "component":
            params = (src_node.get("data") or {}).get("params")
            api = params.get("api") if isinstance(params, dict) else None
            outputs = (
                api.get("outputs")
                if isinstance(api, dict) and isinstance(api.get("outputs"), list)
                else []
            )
            declared_output_names = {
                str((o or {}).get("name") or "").strip()
                for o in outputs
                if isinstance(o, dict) and str((o or {}).get("name") or "").strip()
            }
            source_handle = str(e.get("sourceHandle") or "out").strip() or "out"
            if source_handle == "out" and len(declared_output_names) == 1:
                source_handle = next(iter(declared_output_names))
            if declared_output_names and source_handle not in declared_output_names:
                raise ContractMismatchError(
                    "Component edge references undeclared output handle",
                    code="COMPONENT_OUTPUT_HANDLE_UNRESOLVED",
                    details=_contract_details(
                        expected={
                            "sourceHandle": "declared output",
                            "outputNames": sorted(declared_output_names),
                        },
                        actual={
                            "edgeId": str(e.get("id") or ""),
                            "sourceHandle": source_handle,
                            "resolvedArtifact": False,
                        },
                    ),
                )
            bridge = _resolve_component_output_artifact_from_output_edges(
                edges=edges,
                component_instance_node_id=str(src),
                output_name=source_handle,
                get_current_artifact=get_current_artifact,
            )
            if bridge.get("artifactId"):
                aid = str(bridge["artifactId"])
            elif bridge.get("runtimeNodeId"):
                raise ContractMismatchError(
                    f"Component output '{source_handle}' could not be resolved from bindings",
                    code="COMPONENT_OUTPUT_HANDLE_UNRESOLVED",
                    details=_contract_details(
                        expected={"sourceHandle": source_handle, "resolvedArtifact": True},
                        actual={
                            "edgeId": str(e.get("id") or ""),
                            "componentArtifactId": str(aid),
                            "boundRuntimeNodeId": str(bridge.get("runtimeNodeId") or ""),
                            "outputEdgeId": str(bridge.get("edgeId") or ""),
                            "resolvedArtifact": False,
                        },
                    ),
                )

            direct = _resolve_component_output_artifact_from_bindings(
                src_node=src_node,
                component_instance_node_id=str(src),
                output_name=source_handle,
                get_current_artifact=get_current_artifact,
            )
            if bridge.get("edgeCount"):
                # Expanded component output edges are the source of truth for runtime IDs.
                # If the bridge exists and produced no artifact, we already raised above.
                pass
            elif direct.get("artifactId"):
                aid = str(direct["artifactId"])
            elif bool(direct.get("hasBinding")):
                raise ContractMismatchError(
                    f"Component output '{source_handle}' could not be resolved from bindings",
                    code="COMPONENT_OUTPUT_HANDLE_UNRESOLVED",
                    details=_contract_details(
                        expected={"sourceHandle": source_handle, "resolvedArtifact": True},
                        actual={
                            "edgeId": str(e.get("id") or ""),
                            "componentArtifactId": str(aid),
                            "boundRuntimeNodeId": str(direct.get("runtimeNodeId") or ""),
                            "resolvedArtifact": False,
                        },
                    ),
                )
            else:
                raise ContractMismatchError(
                    f"Component output '{source_handle}' requires explicit bound artifact",
                    code="COMPONENT_OUTPUT_HANDLE_UNRESOLVED",
                    details=_contract_details(
                        expected={"sourceHandle": source_handle, "resolvedArtifact": True},
                        actual={
                            "edgeId": str(e.get("id") or ""),
                            "componentArtifactId": str(aid),
                            "resolvedArtifact": False,
                        },
                    ),
                )
        input_handle = e.get("targetHandle") or "in"
        refs.append((input_handle, aid))
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


def _node_runtime_param_mode(node: Dict[str, Any]) -> str:
    data = (node.get("data") or {}) if isinstance(node, dict) else {}
    params = (data.get("params") or {}) if isinstance(data.get("params"), dict) else {}
    candidates = [
        data.get("runtimeParamMode"),
        params.get("runtime_param_mode"),
        params.get("runtimeParamMode"),
    ]
    for value in candidates:
        mode = str(value or "").strip().lower()
        if mode in {"read_once", "dynamic"}:
            return mode
    return "read_once"


def _is_node_or_edge_fatal(
    *,
    node: Dict[str, Any],
    incoming_edge_ids: List[str],
    edges: Dict[str, Dict[str, Any]],
) -> bool:
    data = (node.get("data") or {}) if isinstance(node, dict) else {}
    params = (data.get("params") or {}) if isinstance(data.get("params"), dict) else {}
    if bool(data.get("fatal")) or bool(params.get("fatal")):
        return True
    for edge_id in incoming_edge_ids:
        edge = edges.get(edge_id) or {}
        edge_data = edge.get("data") if isinstance(edge.get("data"), dict) else {}
        if bool(edge_data.get("fatal")):
            return True
    return False


def _resolve_retry_policy(
    *,
    graph: Dict[str, Any],
    node: Dict[str, Any],
    op_name: Optional[str] = None,
) -> Dict[str, Any]:
    defaults = {"max_attempts": 1, "backoff_seconds": 0.0, "jitter_seconds": 0.0}
    graph_retry = graph.get("retry") if isinstance(graph.get("retry"), dict) else {}
    node_params = ((node.get("data") or {}).get("params") or {}) if isinstance(((node.get("data") or {}).get("params")), dict) else {}
    node_retry = node_params.get("retry") if isinstance(node_params.get("retry"), dict) else {}
    op_retry: Dict[str, Any] = {}
    if op_name:
        op_payload = node_params.get(op_name) if isinstance(node_params.get(op_name), dict) else {}
        op_retry = op_payload.get("retry") if isinstance(op_payload.get("retry"), dict) else {}
    merged: Dict[str, Any] = {}
    for src in (defaults, graph_retry, node_retry, op_retry):
        for key, value in src.items():
            merged[str(key)] = value
    try:
        merged["max_attempts"] = max(1, int(merged.get("max_attempts", 1)))
    except Exception:
        merged["max_attempts"] = 1
    try:
        merged["backoff_seconds"] = max(0.0, float(merged.get("backoff_seconds", 0.0)))
    except Exception:
        merged["backoff_seconds"] = 0.0
    try:
        merged["jitter_seconds"] = max(0.0, float(merged.get("jitter_seconds", 0.0)))
    except Exception:
        merged["jitter_seconds"] = 0.0
    return merged


def _normalize_consume_mode(raw: Any) -> str:
    mode = str(raw or "once").strip().lower()
    if mode in {"read_once", "once"}:
        return "once"
    if mode in {"continuous", "single_item"}:
        return "single_item"
    if mode == "batch":
        return "batch"
    return "once"


def _node_processing_policy(node: Dict[str, Any], input_handle: Optional[str] = None) -> Dict[str, Any]:
    data = (node.get("data") or {}) if isinstance(node, dict) else {}
    params = (data.get("params") or {}) if isinstance(data.get("params"), dict) else {}
    policy = {}
    if isinstance(data.get("processingPolicy"), dict):
        policy = data.get("processingPolicy") or {}
    elif isinstance(params.get("processing_policy"), dict):
        policy = params.get("processing_policy") or {}
    elif isinstance(params.get("processingPolicy"), dict):
        policy = params.get("processingPolicy") or {}
    consume_mode = _normalize_consume_mode(policy.get("consume_mode") or policy.get("consumeMode") or "once")
    read_once = bool(policy.get("read_once") or policy.get("readOnce") or consume_mode == "once")
    try:
        batch_size = max(1, int(policy.get("batch_size") or policy.get("batchSize") or 1))
    except Exception:
        batch_size = 1
    try:
        max_inflight = max(1, int(policy.get("max_inflight") or policy.get("maxInflight") or 1))
    except Exception:
        max_inflight = 1
    on_error_raw = (
        policy.get("on_error")
        or policy.get("onError")
        or params.get("on_error")
        or params.get("onError")
        or "fail_fast"
    )
    on_error = str(on_error_raw or "fail_fast").strip().lower()
    if on_error not in {"fail_fast", "skip_failed"}:
        on_error = "fail_fast"
    if input_handle:
        by_handle = policy.get("input_handles") if isinstance(policy.get("input_handles"), dict) else {}
        handle_policy = by_handle.get(str(input_handle)) if isinstance(by_handle, dict) else {}
        if isinstance(handle_policy, dict):
            consume_mode = _normalize_consume_mode(
                handle_policy.get("consume_mode")
                or handle_policy.get("consumeMode")
                or consume_mode
            )
            read_once = bool(handle_policy.get("read_once") or handle_policy.get("readOnce") or consume_mode == "once")
            try:
                batch_size = max(
                    1,
                    int(handle_policy.get("batch_size") or handle_policy.get("batchSize") or batch_size),
                )
            except Exception:
                batch_size = max(1, batch_size)
            try:
                max_inflight = max(
                    1,
                    int(handle_policy.get("max_inflight") or handle_policy.get("maxInflight") or max_inflight),
                )
            except Exception:
                max_inflight = max(1, max_inflight)
    if read_once:
        consume_mode = "once"
    return {
        "consume_mode": consume_mode,
        "batch_size": batch_size,
        "max_inflight": max_inflight,
        "read_once": read_once,
        "on_error": on_error,
    }


def _edge_work_item_mode(edge: Dict[str, Any]) -> str:
    data = edge.get("data") if isinstance(edge.get("data"), dict) else {}
    work = data.get("work") if isinstance(data.get("work"), dict) else {}
    mode = str(work.get("item_mode") or work.get("itemMode") or "artifact").strip().lower()
    if mode not in {"artifact", "json_items", "table_rows"}:
        mode = "artifact"
    return mode


def _is_non_work_input_handle(handle: str) -> bool:
    name = str(handle or "").strip().lower()
    return name.startswith("param") or name.startswith("control") or name.startswith("ctl")


def _work_input_pairs(input_refs: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for port, aid in input_refs:
        if _is_non_work_input_handle(str(port or "")):
            continue
        out.append((str(port or "in"), str(aid)))
    return out


_INJECTABLE_NON_WORK_HANDLES = {"param_context", "param_filters", "control_in"}
_PLACEHOLDER_TOKEN_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
_EXACT_PLACEHOLDER_RE = re.compile(r"^\{([A-Za-z_][A-Za-z0-9_]*)\}$")


def _collect_placeholder_tokens(value: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for nested in value.values():
            found.update(_collect_placeholder_tokens(nested))
        return found
    if isinstance(value, list):
        for nested in value:
            found.update(_collect_placeholder_tokens(nested))
        return found
    if isinstance(value, str):
        for match in _PLACEHOLDER_TOKEN_RE.finditer(value):
            token = str(match.group(1) or "").strip()
            if token:
                found.add(token)
    return found


def _placeholder_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(value)


def _inject_placeholders(value: Any, injected: Dict[str, Any]) -> Any:
    if isinstance(value, dict):
        return {k: _inject_placeholders(v, injected) for k, v in value.items()}
    if isinstance(value, list):
        return [_inject_placeholders(v, injected) for v in value]
    if not isinstance(value, str):
        return value

    exact = _EXACT_PLACEHOLDER_RE.fullmatch(value.strip())
    if exact:
        key = str(exact.group(1) or "").strip()
        if key in injected:
            return copy.deepcopy(injected[key])

    def _replace(match: re.Match[str]) -> str:
        key = str(match.group(1) or "").strip()
        if key not in injected:
            return match.group(0)
        return _placeholder_text(injected.get(key))

    return _PLACEHOLDER_TOKEN_RE.sub(_replace, value)


def _artifact_payload_to_python(artifact: Artifact, payload_bytes: bytes) -> Any:
    payload_type = _infer_artifact_payload_type(artifact)
    raw = bytes(payload_bytes or b"")
    if payload_type == "json":
        try:
            return json.loads(raw.decode("utf-8", errors="replace"))
        except Exception:
            return {}
    if payload_type == "text":
        return raw.decode("utf-8", errors="replace")
    if payload_type == "table":
        try:
            df = load_table_from_artifact_bytes(str(getattr(artifact, "mime_type", "") or ""), raw)
            return df.to_dict(orient="records")
        except Exception:
            return raw.decode("utf-8", errors="replace")
    return raw.decode("utf-8", errors="replace")


async def _build_non_work_injection_values(
    context: GraphContext,
    input_refs: List[Tuple[str, str]],
    *,
    handles_to_inject: set[str],
) -> Dict[str, Any]:
    grouped: Dict[str, List[Any]] = {}
    for handle, artifact_id in input_refs:
        key = str(handle or "").strip()
        if key not in handles_to_inject:
            continue
        art = await context.artifact_store.get(str(artifact_id))
        payload = await context.artifact_store.read(str(artifact_id))
        grouped.setdefault(key, []).append(_artifact_payload_to_python(art, payload))

    merged: Dict[str, Any] = {}
    for handle, values in grouped.items():
        if not values:
            continue
        merged[handle] = copy.deepcopy(values[0]) if len(values) == 1 else copy.deepcopy(values)
    return merged


def _edge_mode(edge: Dict[str, Any]) -> str:
    data = edge.get("data") if isinstance(edge.get("data"), dict) else {}
    mode = str(data.get("mode") or "work").strip().lower() or "work"
    if mode not in {"work", "param", "control"}:
        return "work"
    return mode


def _build_fair_dequeue_plan(handle_requests: List[Tuple[str, int]]) -> List[str]:
    """Deterministic round-robin handle plan for mixed batch/single-item fan-in."""
    remaining: Dict[str, int] = {}
    order: List[str] = []
    for handle, count in handle_requests:
        key = str(handle or "in").strip() or "in"
        if key not in remaining:
            order.append(key)
            remaining[key] = 0
        remaining[key] = int(remaining.get(key, 0)) + max(0, int(count))
    out: List[str] = []
    while True:
        progressed = False
        for key in order:
            if int(remaining.get(key, 0)) <= 0:
                continue
            out.append(key)
            remaining[key] = int(remaining.get(key, 0)) - 1
            progressed = True
        if not progressed:
            break
    return out


def _edge_queue_policy(edge: Dict[str, Any]) -> str:
    data = edge.get("data") if isinstance(edge.get("data"), dict) else {}
    queue_cfg = data.get("queue") if isinstance(data.get("queue"), dict) else {}
    policy = str(queue_cfg.get("policy") or "fifo").strip().lower() or "fifo"
    if policy not in {"fifo", "round_robin"}:
        policy = "fifo"
    return policy


def _edge_work_max_items(edge: Dict[str, Any]) -> int:
    data = edge.get("data") if isinstance(edge.get("data"), dict) else {}
    work = data.get("work") if isinstance(data.get("work"), dict) else {}
    try:
        return max(1, int(work.get("max_items") or work.get("maxItems") or 256))
    except Exception:
        return 256


def _normalized_params_for_exec_key(
    *,
    kind: str,
    node: Dict[str, Any],
    params: Dict[str, Any],
) -> Dict[str, Any]:
    p = dict(params or {})
    if kind in {"llm", "model"}:
        from .schemas import normalize_llm_params_frontend

        return normalize_llm_params_frontend(p)
    if kind == "source":
        from .schemas import normalize_source_params_frontend

        source_kind = (node.get("data", {}).get("sourceKind") or p.get("source_type") or "file")
        p["source_type"] = source_kind
        p = normalize_source_params_frontend(p)
        p["source_type"] = source_kind
        for ui_key in (
            "recentSnapshotIds",
            "recent_snapshot_ids",
            "snapshotMetadata",
            "snapshot_metadata",
            "recentSnapshots",
            "snapshotHistory",
        ):
            p.pop(ui_key, None)
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
    node = {"data": {"kind": "tool", "schema": {}, "settings": {}}}
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
    node = {"data": {"kind": "transform", "schema": {}, "settings": {}}}
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
        "model": "LLM@1",
        "llm": "LLM@1",
        "tool": "TOOL@1",
        "component": "COMPONENT@1",
    }
    return mapping.get(str(kind or ""), "GENERIC@1")


def _tool_builtin_env_preflight_error(
    *,
    kind: str,
    params: Dict[str, Any],
) -> Optional["ContractMismatchError"]:
    if str(kind or "") != "tool":
        return None
    provider = str((params or {}).get("provider") or "").strip().lower()
    builtin_cfg = (params or {}).get("builtin")
    if not isinstance(builtin_cfg, dict):
        if provider != "builtin":
            return None
        builtin_cfg = {}
    try:
        resolved_env = resolve_builtin_environment(builtin_cfg)
    except ValueError as ex:
        return ContractMismatchError(
            "Tool builtin environment profile is invalid",
            code="ENV_PROFILE_INVALID",
            details=_contract_details(
                expected={
                    "profileId": "one of declared builtin profiles",
                    "profiles": sorted(BUILTIN_PROFILE_PACKAGES.keys()),
                },
                actual={
                    "provider": provider,
                    "profileId": str((builtin_cfg or {}).get("profileId") or "core").strip() or "core",
                    "message": str(ex),
                },
            ),
        )
    packages = [
        str(pkg).strip()
        for pkg in (resolved_env.get("packages") if isinstance(resolved_env.get("packages"), list) else [])
        if str(pkg).strip()
    ]
    missing = missing_packages_for_packages(packages)
    if missing:
        return ContractMismatchError(
            "Tool builtin environment profile is not installed",
            code="ENV_PROFILE_MISSING",
            details=_contract_details(
                expected={
                    "installed": True,
                    "profileId": str(resolved_env.get("profileId") or "core"),
                    "installTarget": str(resolved_env.get("installTarget") or BUILTIN_PROFILE_INSTALL_TARGETS.get(str(resolved_env.get("profileId") or "core"), "cpu_dev")),
                    "packages": packages,
                },
                actual={
                    "installed": False,
                    "profileId": str(resolved_env.get("profileId") or "core"),
                    "installTarget": str(resolved_env.get("installTarget") or BUILTIN_PROFILE_INSTALL_TARGETS.get(str(resolved_env.get("profileId") or "core"), "cpu_dev")),
                    "source": str(resolved_env.get("source") or ""),
                    "missingPackages": missing,
                    "installHint": "POST /env/profiles/install",
                },
            ),
        )
    explicit_lock = str(resolved_env.get("locked") or "").strip()
    if explicit_lock:
        expected_locks = _expected_tool_profile_locks(resolved_env)
        if explicit_lock not in {expected_locks["canonical"], expected_locks["sha256"]}:
            return ContractMismatchError(
                "Tool builtin environment profile lock mismatch",
                code="ENV_PROFILE_LOCK_MISMATCH",
                details=_contract_details(
                    expected={
                        "profileId": str(resolved_env.get("profileId") or "core"),
                        "installTarget": str(
                            resolved_env.get("installTarget")
                            or BUILTIN_PROFILE_INSTALL_TARGETS.get(
                                str(resolved_env.get("profileId") or "core"),
                                "cpu_dev",
                            )
                        ),
                        "lock": expected_locks["canonical"],
                        "alternateLock": expected_locks["sha256"],
                    },
                    actual={
                        "profileId": str(resolved_env.get("profileId") or "core"),
                        "lock": explicit_lock,
                    },
                ),
            )
    return None


def _tool_builtin_env_requirement(params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    provider = str((params or {}).get("provider") or "").strip().lower()
    builtin_cfg = (params or {}).get("builtin")
    if not isinstance(builtin_cfg, dict):
        if provider != "builtin":
            return None
        builtin_cfg = {}
    try:
        resolved = resolve_builtin_environment(builtin_cfg)
    except ValueError as ex:
        return {
            "invalid": True,
            "profileId": str((builtin_cfg or {}).get("profileId") or "core").strip() or "core",
            "provider": provider,
            "message": str(ex),
        }
    packages = [
        str(pkg).strip()
        for pkg in (resolved.get("packages") if isinstance(resolved.get("packages"), list) else [])
        if str(pkg).strip()
    ]
    return {
        "invalid": False,
        "profileId": str(resolved.get("profileId") or "core").strip() or "core",
        "source": str(resolved.get("source") or "builtin_profile").strip() or "builtin_profile",
        "installTarget": str(
            resolved.get("installTarget")
            or BUILTIN_PROFILE_INSTALL_TARGETS.get(str(resolved.get("profileId") or "core"), "cpu_dev")
        ),
        "packages": packages,
    }


def _collect_component_builtin_profile_requirements(
    *,
    nodes_by_id: Dict[str, Dict[str, Any]],
    internal_node_ids: list[str],
) -> Dict[str, Any]:
    profile_map: Dict[str, Dict[str, Any]] = {}
    invalid_nodes: list[Dict[str, Any]] = []
    for nid in sorted(set(str(v) for v in internal_node_ids if str(v).strip())):
        node = nodes_by_id.get(nid)
        if not isinstance(node, dict):
            continue
        data = node.get("data") if isinstance(node.get("data"), dict) else {}
        if str((data or {}).get("kind") or "").strip().lower() != "tool":
            continue
        params = data.get("params") if isinstance(data.get("params"), dict) else {}
        requirement = _tool_builtin_env_requirement(params if isinstance(params, dict) else {})
        if not isinstance(requirement, dict):
            continue
        if bool(requirement.get("invalid")):
            invalid_nodes.append(
                {
                    "nodeId": nid,
                    "profileId": str(requirement.get("profileId") or "core"),
                    "message": str(requirement.get("message") or ""),
                }
            )
            continue
        profile_id = str(requirement.get("profileId") or "").strip()
        if not profile_id:
            continue
        entry = profile_map.setdefault(
            profile_id,
            {
                "profileId": profile_id,
                "source": str(requirement.get("source") or "builtin_profile"),
                "installTarget": str(
                    requirement.get("installTarget")
                    or BUILTIN_PROFILE_INSTALL_TARGETS.get(profile_id, "cpu_dev")
                ),
                "packages": [],
                "_pkg_set": set(),
                "_node_set": set(),
            },
        )
        for pkg in requirement.get("packages") if isinstance(requirement.get("packages"), list) else []:
            pkg_name = str(pkg).strip()
            if not pkg_name or pkg_name in entry["_pkg_set"]:
                continue
            entry["_pkg_set"].add(pkg_name)
            entry["packages"].append(pkg_name)
        entry["_node_set"].add(nid)

    required_profiles: list[Dict[str, Any]] = []
    missing_profiles: list[Dict[str, Any]] = []
    for profile_id in sorted(profile_map.keys()):
        entry = profile_map[profile_id]
        packages = [
            str(pkg).strip()
            for pkg in (entry.get("packages") if isinstance(entry.get("packages"), list) else [])
            if str(pkg).strip()
        ]
        missing = missing_packages_for_packages(packages)
        item = {
            "profileId": profile_id,
            "source": str(entry.get("source") or "builtin_profile"),
            "installTarget": str(entry.get("installTarget") or BUILTIN_PROFILE_INSTALL_TARGETS.get(profile_id, "cpu_dev")),
            "packages": packages,
            "missingPackages": missing,
            "internalNodeIds": sorted(str(v) for v in (entry.get("_node_set") or set())),
        }
        required_profiles.append(item)
        if missing:
            missing_profiles.append(item)
    return {
        "requiredProfiles": required_profiles,
        "missingProfiles": missing_profiles,
        "invalidProfiles": invalid_nodes,
    }


def _env_profile_log_guidance(
    *,
    error_code: Optional[str],
    error_details: Optional[Dict[str, Any]],
) -> Optional[str]:
    code = str(error_code or "").strip().upper()
    if not code.startswith("ENV_PROFILE_"):
        return None
    details = error_details if isinstance(error_details, dict) else {}
    actual = details.get("actual") if isinstance(details.get("actual"), dict) else {}
    expected = details.get("expected") if isinstance(details.get("expected"), dict) else {}
    profile_id = str(actual.get("profileId") or expected.get("profileId") or "core").strip() or "core"
    install_target = str(
        actual.get("installTarget")
        or expected.get("installTarget")
        or BUILTIN_PROFILE_INSTALL_TARGETS.get(profile_id, "cpu_dev")
    ).strip() or "cpu_dev"
    install_hint = str(actual.get("installHint") or "POST /env/profiles/install").strip()
    missing = actual.get("missingPackages") if isinstance(actual.get("missingPackages"), list) else []
    missing_text = ", ".join([str(pkg).strip() for pkg in missing if str(pkg).strip()])
    if code == "ENV_PROFILE_MISSING":
        if missing_text:
            return (
                f"Environment profile '{profile_id}' missing packages: {missing_text}. "
                f"Install profile: {install_hint} (profileId='{profile_id}', target='{install_target}')."
            )
        return (
            f"Environment profile '{profile_id}' is not installed. "
            f"Install profile: {install_hint} (profileId='{profile_id}', target='{install_target}')."
        )
    if code == "ENV_PROFILE_PACKAGE_BLOCKED":
        blocked = actual.get("blockedPackages") if isinstance(actual.get("blockedPackages"), list) else []
        blocked_text = ", ".join([str(pkg).strip() for pkg in blocked if str(pkg).strip()])
        if blocked_text:
            return f"Environment profile '{profile_id}' includes blocked packages: {blocked_text}."
        return f"Environment profile '{profile_id}' includes blocked packages."
    if code == "ENV_PROFILE_INVALID":
        return f"Environment profile '{profile_id}' is invalid; update profile selection in the tool editor."
    if code == "ENV_PROFILE_LOCK_MISMATCH":
        expected_lock = str(expected.get("lock") or "").strip()
        actual_lock = str(actual.get("lock") or "").strip()
        if expected_lock and actual_lock:
            return (
                f"Environment profile '{profile_id}' lock mismatch. "
                f"Expected '{expected_lock}', got '{actual_lock}'."
            )
        return f"Environment profile '{profile_id}' lock mismatch."
    if code == "ENV_PROFILE_INSTALL_FAILED":
        return (
            f"Environment profile '{profile_id}' install failed. "
            f"Retry install via {install_hint} (profileId='{profile_id}', target='{install_target}')."
        )
    return f"Environment profile error for '{profile_id}'."


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


def _coercion_policy_for_node(node: Dict[str, Any]) -> str:
    data = node.get("data", {}) if isinstance(node, dict) else {}
    params = data.get("params", {}) if isinstance(data, dict) and isinstance(data.get("params"), dict) else {}
    policy = (
        params.get("coercion_policy")
        or params.get("coercionPolicy")
        or (params.get("coercion") or {}).get("policy")
        if isinstance(params.get("coercion"), dict)
        else None
    )
    value = str(policy or "safe_only").strip().lower()
    if value not in {"forbid", "safe_only", "allow_lossy"}:
        return "safe_only"
    return value


def _artifact_typed_schema(artifact: Artifact) -> Dict[str, Any]:
    ps = artifact.payload_schema if isinstance(artifact.payload_schema, dict) else {}
    ptype = str(_infer_artifact_payload_type(artifact) or "unknown").strip().lower() or "unknown"
    if ptype == "table":
        cols = _extract_table_columns_from_payload_schema(ps)
        fields = [
            {
                "name": str(c.get("name") or "").strip(),
                "type": str(c.get("type") or "unknown").strip() or "unknown",
                "nullable": True,
            }
            for c in cols
            if isinstance(c, dict) and str(c.get("name") or "").strip()
        ]
        return {"type": "table", "fields": fields}
    if ptype in {"json", "text", "binary", "embeddings"}:
        return {"type": ptype, "fields": []}
    return {"type": "unknown", "fields": []}


def _declared_component_input_schema(node: Dict[str, Any], input_name: str) -> Optional[Dict[str, Any]]:
    data = node.get("data", {}) if isinstance(node, dict) else {}
    if str(data.get("kind") or "") != "component":
        return None
    params = data.get("params", {}) if isinstance(data.get("params"), dict) else {}
    api = params.get("api") if isinstance(params.get("api"), dict) else {}
    inputs = api.get("inputs") if isinstance(api.get("inputs"), list) else []
    target = str(input_name or "in").strip()
    for entry in inputs:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("name") or "").strip() != target:
            continue
        ts = entry.get("typedSchema")
        return ts if isinstance(ts, dict) else None
    return None


def _typed_schema_compatibility(
    *,
    expected: Optional[Dict[str, Any]],
    actual: Optional[Dict[str, Any]],
    policy: str,
) -> tuple[bool, Dict[str, Any]]:
    exp = expected if isinstance(expected, dict) else None
    act = actual if isinstance(actual, dict) else None
    if exp is None or act is None:
        return True, {
            "coercionApplied": False,
            "missingColumns": [],
            "mismatchedColumns": [],
        }
    exp_t = str(exp.get("type") or "unknown").strip().lower() or "unknown"
    act_t = str(act.get("type") or "unknown").strip().lower() or "unknown"
    coercion_applied = False
    if exp_t != "unknown" and act_t != "unknown" and exp_t != act_t:
        if policy == "allow_lossy":
            coercion_applied = True
        else:
            return False, {
                "coercionApplied": False,
                "reason": "type_mismatch",
                "expectedType": exp_t,
                "actualType": act_t,
                "missingColumns": [],
                "mismatchedColumns": [],
            }
    if exp_t == "table" and act_t == "table":
        exp_fields = exp.get("fields") if isinstance(exp.get("fields"), list) else []
        act_fields = act.get("fields") if isinstance(act.get("fields"), list) else []
        exp_names = [str(f.get("name") or "").strip() for f in exp_fields if isinstance(f, dict)]
        act_names = {str(f.get("name") or "").strip() for f in act_fields if isinstance(f, dict)}
        missing = sorted([n for n in exp_names if n and n not in act_names])
        exp_types = {
            str(f.get("name") or "").strip(): str(f.get("type") or "unknown").strip().lower() or "unknown"
            for f in exp_fields
            if isinstance(f, dict) and str(f.get("name") or "").strip()
        }
        act_types = {
            str(f.get("name") or "").strip(): str(f.get("type") or "unknown").strip().lower() or "unknown"
            for f in act_fields
            if isinstance(f, dict) and str(f.get("name") or "").strip()
        }
        mismatched = sorted(
            [
                name
                for name in exp_types.keys()
                if name in act_types
                and exp_types.get(name) not in {"", "unknown"}
                and act_types.get(name) not in {"", "unknown"}
                and exp_types.get(name) != act_types.get(name)
            ]
        )
        if mismatched:
            if policy == "allow_lossy":
                coercion_applied = True
            else:
                return False, {
                    "coercionApplied": False,
                    "reason": "column_type_mismatch",
                    "missingColumns": [],
                    "mismatchedColumns": mismatched,
                }
        if missing:
            if policy == "allow_lossy":
                coercion_applied = True
            else:
                return False, {
                    "coercionApplied": False,
                    "reason": "missing_columns",
                    "missingColumns": missing,
                    "mismatchedColumns": [],
                }
    return True, {
        "coercionApplied": coercion_applied,
        "missingColumns": [],
        "mismatchedColumns": [],
    }


def _component_output_port_compatible(
    *,
    declared_payload_type: str,
    actual_payload_type: str,
    artifact: Artifact,
) -> bool:
    if declared_payload_type == actual_payload_type:
        return True
    # Source text-mode can materialize as a 1-row table with "text" column.
    if declared_payload_type == "text" and actual_payload_type == "table":
        ps = artifact.payload_schema if isinstance(artifact.payload_schema, dict) else {}
        coercion = ps.get("coercion") if isinstance(ps.get("coercion"), dict) else {}
        mode = str(coercion.get("mode") or "").strip().lower()
        if mode == "text_1row":
            return True
    return False


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
    if op_l == "tokenize_chunk":
        spec = norm.get("tokenize_chunk") or {}
        out_col = str(spec.get("outColumn") or "chunk")
        out = [
            {"name": out_col, "type": "string"},
            {"name": "source_row", "type": "int"},
            {"name": "source_column", "type": "string"},
            {"name": "chunk_index", "type": "int"},
            {"name": "token_count", "type": "int"},
        ]
        return canonical_table_columns(out)
    if op_l == "dataset_split":
        out = list(primary)
        if "split" not in out:
            out.append("split")
        return canonical_table_columns([{"name": c, "type": "unknown"} for c in out])
    if op_l == "embedding":
        spec = norm.get("embedding") or {}
        out_col = str(spec.get("outputColumn") or "embedding")
        out = [c for c in primary if c != out_col] + [out_col]
        return canonical_table_columns([{"name": c, "type": "unknown"} for c in out])
    if op_l == "feature_selection":
        spec = norm.get("feature_selection") or {}
        method = str(spec.get("method") or "variance").strip().lower()
        if method == "manual":
            selected = [str(c) for c in (spec.get("selectedColumns") or []) if str(c).strip() in set(primary)]
            if selected:
                return canonical_table_columns([{"name": c, "type": "unknown"} for c in selected])
    if op_l in {"sort", "limit", "dedupe", "null_policy", "outlier_policy", "text_clean", "nlp_normalize", "filter", "quality_gate", "ml_contract", "sql", "json_to_table", "text_to_table", "class_imbalance", "categorical_encode", "numeric_scale", "leakage_detect", "quality_profile", "drift_compare", "determinism_profile", "fit_state_registry", "pii_guard", "inference_parity"}:
        # sql may differ but keep deterministic fallback if no parser.
        return canonical_table_columns([{"name": c, "type": "unknown"} for c in primary])
    return canonical_table_columns([{"name": c, "type": "unknown"} for c in primary])


def _source_payload_schema(
    out_contract: Optional[str],
    data_value: Any,
    source_metadata: Optional[Any] = None,
) -> Optional[Dict[str, Any]]:
    source_data_schema = (
        getattr(source_metadata, "data_schema", {}) if isinstance(getattr(source_metadata, "data_schema", None), dict) else {}
    )
    source_priming_artifact = (
        getattr(source_metadata, "priming_artifact", {})
        if isinstance(getattr(source_metadata, "priming_artifact", None), dict)
        else {}
    )
    def _copy_format_specific(target: Dict[str, Any]) -> None:
        for key in (
            "parquet_logical_types",
            "parquet_stats",
            "csv_dialect",
            "image_metadata",
            "audio_metadata",
            "video_metadata",
            "pdf_metadata",
            "json_streaming",
            "json_mode_resolved",
            "json_flatten",
            "excel_provenance",
            "excel_policy",
            "txt_recordization",
        ):
            value = source_data_schema.get(key)
            if value is not None:
                target[key] = value

    if out_contract == "table" and isinstance(data_value, list):
        payload = _table_payload_schema_from_rows(data_value)
        table_columns = source_data_schema.get("table_columns")
        if isinstance(table_columns, list):
            payload["columns"] = canonical_table_columns(table_columns)
        coercion = source_data_schema.get("table_coercion")
        if isinstance(coercion, dict):
            payload["coercion"] = coercion
        source_observability = source_data_schema.get("source_observability")
        if isinstance(source_observability, dict):
            payload["source_observability"] = source_observability
        _copy_format_specific(payload)
        if isinstance(source_priming_artifact, dict) and source_priming_artifact:
            payload["priming_artifact"] = source_priming_artifact
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
        source_observability = source_data_schema.get("source_observability")
        if isinstance(source_observability, dict):
            out["source_observability"] = source_observability
        _copy_format_specific(out)
        if isinstance(source_priming_artifact, dict) and source_priming_artifact:
            out["priming_artifact"] = source_priming_artifact
        return out
    if out_contract == "text":
        out = {"schema_version": 1, "type": "text", "encoding": "utf-8"}
        source_observability = source_data_schema.get("source_observability")
        if isinstance(source_observability, dict):
            out["source_observability"] = source_observability
        _copy_format_specific(out)
        if isinstance(source_priming_artifact, dict) and source_priming_artifact:
            out["priming_artifact"] = source_priming_artifact
        return out
    if out_contract in {"binary", "image", "audio", "video"}:
        out = {"schema_version": 1, "type": str(out_contract)}
        source_observability = source_data_schema.get("source_observability")
        if isinstance(source_observability, dict):
            out["source_observability"] = source_observability
        _copy_format_specific(out)
        if isinstance(source_priming_artifact, dict) and source_priming_artifact:
            out["priming_artifact"] = source_priming_artifact
        return out
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


def _json_payload_value_schema(value: Any, *, depth: int = 0, max_depth: int = 24) -> Dict[str, Any]:
    # Memoize top-level JSON-shape inference by canonical payload digest.
    # Deeper recursive calls are handled by schema_infer internals.
    if depth <= 0:
        return infer_json_schema_cached(value, max_depth=max_depth)
    return infer_json_schema_cached(value, max_depth=max_depth)


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
    if kind not in {"source", "model", "llm", "tool"}:
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


def _tool_payload_schema(envelope_kind: str, payload: Any, envelope_meta: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    builtin_environment = None
    if isinstance(envelope_meta, dict) and isinstance(envelope_meta.get("builtin_environment"), dict):
        raw_env = envelope_meta.get("builtin_environment") or {}
        profile_id = str(raw_env.get("profileId") or "").strip()
        source = str(raw_env.get("source") or "").strip()
        install_target = str(raw_env.get("installTarget") or "").strip()
        locked = str(raw_env.get("locked") or "").strip()
        packages_raw = raw_env.get("packages")
        packages: list[str] = []
        if isinstance(packages_raw, list):
            for pkg in packages_raw:
                if isinstance(pkg, str) and pkg.strip():
                    packages.append(pkg.strip())
        if profile_id or source or install_target or packages or locked:
            builtin_environment = {
                "profileId": profile_id,
                "source": source,
                "installTarget": install_target,
                "packages": packages,
            }
            if locked:
                builtin_environment["locked"] = locked

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
        if isinstance(builtin_environment, dict):
            out["builtin_environment"] = builtin_environment
        return out
    if envelope_kind == "text":
        out = {"schema_version": 1, "type": "text", "encoding": "utf-8"}
        if isinstance(builtin_environment, dict):
            out["builtin_environment"] = builtin_environment
        return out
    if envelope_kind == "binary":
        out = {"schema_version": 1, "type": "binary"}
        if isinstance(builtin_environment, dict):
            out["builtin_environment"] = builtin_environment
        return out
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
    payload_type: Optional[str],
    schema: Optional[Dict[str, Any]],
    created_at_iso: str,
    run_id: Optional[str],
    graph_id: Optional[str],
    determinism_fingerprint: Optional[str] = None,
    code_hash: Optional[str] = None,
    profile_lock: Optional[str] = None,
    component_context: Optional[Dict[str, Any]] = None,
    lineage_v1: Optional[Dict[str, Any]] = None,
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
        "payloadType": str(payload_type or ""),
        "createdAt": created_at_iso,
        "runId": run_id,
        "graphId": graph_id,
    }
    if isinstance(determinism_fingerprint, str) and determinism_fingerprint.strip():
        out["determinismFingerprint"] = determinism_fingerprint.strip()
    if isinstance(code_hash, str) and code_hash.strip():
        out["codeHash"] = code_hash.strip()
    if isinstance(profile_lock, str) and profile_lock.strip():
        out["profileLock"] = profile_lock.strip()
    if isinstance(schema, dict):
        out["schema"] = schema
    if isinstance(component_context, dict) and component_context:
        out["component"] = {
            "componentId": str(component_context.get("componentId") or ""),
            "componentRevisionId": str(component_context.get("componentRevisionId") or ""),
            "instanceNodeId": str(component_context.get("instanceNodeId") or ""),
            "internalNodeId": str(component_context.get("internalNodeId") or ""),
        }
    if isinstance(lineage_v1, dict) and lineage_v1:
        out["lineageV1"] = lineage_v1
    return out


async def _artifact_lineage_v1(
    *,
    artifact_id: str,
    upstream_artifact_ids: list[str],
    node_params: Dict[str, Any],
    node_id: str,
    run_id: Optional[str],
    graph_id: Optional[str],
    exec_key: str,
    artifact_store: Any,
) -> Dict[str, Any]:
    parent_ids = sorted({str(a).strip() for a in (upstream_artifact_ids or []) if str(a).strip()})
    producer_ref = {
        "role": "producer",
        "artifactId": str(artifact_id or ""),
        "runId": str(run_id or ""),
        "graphId": str(graph_id or ""),
        "nodeId": str(node_id or ""),
        "execKey": str(exec_key or ""),
    }
    run_refs: list[Dict[str, str]] = [producer_ref]

    snapshot_refs: list[Dict[str, str]] = []
    snapshot_id = None
    if isinstance(node_params, dict):
        snapshot_id = node_params.get("snapshot_id") or node_params.get("snapshotId")
    if isinstance(snapshot_id, str) and snapshot_id.strip():
        snapshot_refs.append({"snapshotId": str(snapshot_id).strip().lower(), "role": "source_input"})

    if parent_ids and artifact_store is not None and hasattr(artifact_store, "get"):
        for parent_artifact_id in parent_ids:
            try:
                parent_art = await artifact_store.get(parent_artifact_id)
            except Exception:
                continue
            run_refs.append(
                {
                    "role": "parent_producer",
                    "artifactId": str(parent_artifact_id),
                    "runId": str(getattr(parent_art, "run_id", "") or ""),
                    "graphId": str(getattr(parent_art, "graph_id", "") or ""),
                    "nodeId": str(getattr(parent_art, "node_id", "") or ""),
                    "execKey": str(getattr(parent_art, "exec_key", "") or ""),
                }
            )
            parent_ps = parent_art.payload_schema if isinstance(parent_art.payload_schema, dict) else {}
            parent_meta = (
                parent_ps.get("artifactMetadataV1")
                if isinstance(parent_ps.get("artifactMetadataV1"), dict)
                else {}
            )
            parent_lineage = (
                parent_meta.get("lineageV1")
                if isinstance(parent_meta.get("lineageV1"), dict)
                else {}
            )
            parent_snapshots = (
                parent_lineage.get("snapshotRefs")
                if isinstance(parent_lineage.get("snapshotRefs"), list)
                else []
            )
            for snap in parent_snapshots:
                if isinstance(snap, dict) and str(snap.get("snapshotId") or "").strip():
                    snapshot_refs.append(
                        {
                            "snapshotId": str(snap.get("snapshotId") or "").strip().lower(),
                            "role": str(snap.get("role") or "ancestor"),
                        }
                    )

    # Deterministic de-duplication.
    unique_snapshot_keys: set[tuple[str, str]] = set()
    deduped_snapshots: list[Dict[str, str]] = []
    for snap in snapshot_refs:
        sid = str(snap.get("snapshotId") or "").strip().lower()
        role = str(snap.get("role") or "").strip() or "ancestor"
        if not sid:
            continue
        key = (sid, role)
        if key in unique_snapshot_keys:
            continue
        unique_snapshot_keys.add(key)
        deduped_snapshots.append({"snapshotId": sid, "role": role})

    unique_run_keys: set[tuple[str, str]] = set()
    deduped_run_refs: list[Dict[str, str]] = []
    for ref in run_refs:
        role = str(ref.get("role") or "").strip() or "producer"
        aid = str(ref.get("artifactId") or "").strip()
        if not aid:
            continue
        key = (role, aid)
        if key in unique_run_keys:
            continue
        unique_run_keys.add(key)
        deduped_run_refs.append(
            {
                "role": role,
                "artifactId": aid,
                "runId": str(ref.get("runId") or ""),
                "graphId": str(ref.get("graphId") or ""),
                "nodeId": str(ref.get("nodeId") or ""),
                "execKey": str(ref.get("execKey") or ""),
            }
        )

    return {
        "schemaVersion": 1,
        "datasetVersionId": str(artifact_id or ""),
        "artifactId": str(artifact_id or ""),
        "parentArtifactIds": parent_ids,
        "snapshotRefs": deduped_snapshots,
        "runRefs": deduped_run_refs,
    }


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


def _available_columns_for_input_handle(
    *,
    input_handle: str,
    input_schema_cols_by_handle: Dict[str, list[Dict[str, Any]]],
    input_columns: Dict[str, list[str]],
) -> tuple[list[str], str]:
    schema_cols = input_schema_cols_by_handle.get(input_handle) or []
    schema_names = [
        str(c.get("name"))
        for c in schema_cols
        if isinstance(c, dict) and str(c.get("name") or "").strip()
    ]
    if schema_names:
        return _stable_unique_strings(schema_names), "schema"
    inferred = _stable_unique_strings(input_columns.get(input_handle) or [])
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


def _infer_artifact_payload_type(artifact: Artifact) -> str:
    if artifact.payload_type:
        return str(artifact.payload_type)
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


def _source_observability_from_artifact(artifact: Artifact) -> Optional[Dict[str, Any]]:
    def _apply_format_specific_fields(out: Dict[str, Any], source: Dict[str, Any]) -> None:
        passthrough_keys = {
            "parquet_logical_types",
            "parquet_stats",
            "csv_dialect",
            "image_metadata",
            "audio_metadata",
            "video_metadata",
            "pdf_metadata",
            "json_streaming",
            "json_flatten",
            "excel_provenance",
            "excel_policy",
            "txt_recordization",
        }
        for key in passthrough_keys:
            if key in source:
                out[key] = source.get(key)

    ps = artifact.payload_schema if isinstance(artifact.payload_schema, dict) else {}
    direct = ps.get("source_observability")
    if isinstance(direct, dict):
        out = dict(direct)
        table_columns = ps.get("table_columns")
        if not isinstance(table_columns, list):
            table_columns = ps.get("columns")
        if isinstance(table_columns, list):
            out["table_columns"] = canonical_table_columns(table_columns)
        if "header_detected" in ps:
            out["header_detected"] = ps.get("header_detected")
        _apply_format_specific_fields(out, ps)
        return out
    schema_env = ps.get("schema")
    if isinstance(schema_env, dict):
        nested = schema_env.get("source_observability")
        if isinstance(nested, dict):
            out = dict(nested)
            table_columns = schema_env.get("table_columns")
            if not isinstance(table_columns, list):
                table_columns = schema_env.get("columns")
            if isinstance(table_columns, list):
                out["table_columns"] = canonical_table_columns(table_columns)
            if "header_detected" in schema_env:
                out["header_detected"] = schema_env.get("header_detected")
            _apply_format_specific_fields(out, schema_env)
            return out
    return None


def _source_priming_artifact_from_artifact(artifact: Artifact) -> Optional[Dict[str, Any]]:
    ps = artifact.payload_schema if isinstance(artifact.payload_schema, dict) else {}
    direct = ps.get("priming_artifact")
    if isinstance(direct, dict):
        return direct
    nested = ps.get("primingArtifactV1")
    if isinstance(nested, dict):
        return nested
    schema_env = ps.get("schema")
    if isinstance(schema_env, dict):
        nested_schema = schema_env.get("priming_artifact")
        if isinstance(nested_schema, dict):
            return nested_schema
    return None


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


def _declared_expected_typed_schema_from_node(node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    data = node.get("data", {}) if isinstance(node, dict) else {}
    schema_env = data.get("schema") if isinstance(data, dict) and isinstance(data.get("schema"), dict) else {}
    expected = schema_env.get("expectedSchema") if isinstance(schema_env.get("expectedSchema"), dict) else {}
    typed = expected.get("typedSchema") if isinstance(expected.get("typedSchema"), dict) else None
    if not isinstance(typed, dict):
        return None
    typed_type = str(typed.get("type") or "").strip().lower()
    if typed_type == "string":
        typed_type = "text"
    if typed_type not in {"table", "json", "text", "binary", "embeddings", "unknown"}:
        return None
    fields_raw = typed.get("fields") if isinstance(typed.get("fields"), list) else []
    fields = []
    for f in fields_raw:
        if not isinstance(f, dict):
            continue
        name = str(f.get("name") or "").strip()
        if not name:
            continue
        f_type = str(f.get("type") or "unknown").strip().lower() or "unknown"
        if f_type == "string":
            f_type = "text"
        fields.append(
            {
                "name": name,
                "type": f_type,
                "nullable": bool(f.get("nullable", True)),
            }
        )
    if typed_type != "table":
        fields = []
    return {"type": typed_type, "fields": fields}


def _node_typed_schema_type_from_node(
    node: Dict[str, Any],
    channels: tuple[str, ...] = ("expectedSchema", "inferredSchema", "observedSchema"),
) -> Optional[str]:
    data = node.get("data", {}) if isinstance(node, dict) else {}
    schema_env = data.get("schema") if isinstance(data, dict) and isinstance(data.get("schema"), dict) else {}
    if not isinstance(schema_env, dict):
        return None
    for key in channels:
        obs = schema_env.get(key)
        if not isinstance(obs, dict):
            continue
        typed = obs.get("typedSchema")
        if not isinstance(typed, dict):
            continue
        typed_type = str(typed.get("type") or "").strip().lower()
        if typed_type == "string":
            typed_type = "text"
        if typed_type in {"table", "json", "text", "binary", "embeddings"}:
            return typed_type
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
    elif source_kind == "object_store":
        bucket = str(params.get("bucket") or "").strip()
        key = str(params.get("key") or "").strip()
        provider = str(params.get("provider") or "").strip()
        if provider:
            out["provider"] = provider
        if bucket:
            out["bucket"] = bucket
        if key:
            out["key"] = key
    elif source_kind == "warehouse":
        provider = str(params.get("provider") or "").strip()
        query = str(params.get("query") or "").strip()
        if provider:
            out["provider"] = provider
        if query:
            out["query"] = query
    return out


def _expected_schema_contract_for_node(node: Dict[str, Any]) -> Dict[str, Any]:
    declared_typed = _declared_expected_typed_schema_from_node(node)
    if declared_typed is not None:
        canonical: Dict[str, Any] = {
            "schema_version": 1,
            "typed_schema": {
                "type": str(declared_typed.get("type") or "unknown"),
                "fields": declared_typed.get("fields") if isinstance(declared_typed.get("fields"), list) else [],
            },
        }
        if str(declared_typed.get("type") or "") == "table":
            table_cols = canonical_table_columns(
                [
                    {
                        "name": str(f.get("name") or "").strip(),
                        "type": str(f.get("type") or "unknown").strip() or "unknown",
                    }
                    for f in (declared_typed.get("fields") or [])
                    if isinstance(f, dict) and str(f.get("name") or "").strip()
                ]
            )
            canonical["table"] = {"columns": table_cols}
        return {
            "schemaObject": canonical,
            "schemaFingerprint": contract_schema_fingerprint(canonical),
            "schemaSource": "declared",
            "typedSchema": declared_typed,
        }
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


def _expected_output_schema_error(
    *,
    node: Dict[str, Any],
    artifact: Artifact,
    expected_schema: Optional[Dict[str, Any]],
    strict_coercion_policy: bool,
) -> Optional[ContractMismatchError]:
    expected = expected_schema if isinstance(expected_schema, dict) else {}
    expected_typed = expected.get("typedSchema") if isinstance(expected.get("typedSchema"), dict) else None
    if not isinstance(expected_typed, dict):
        return None
    policy = _coercion_policy_for_node(node) if strict_coercion_policy else "allow_lossy"
    actual_typed = _artifact_typed_schema(artifact)
    ok, ts_info = _typed_schema_compatibility(
        expected=expected_typed,
        actual=actual_typed,
        policy=policy,
    )
    if ok:
        return None
    reason = str(ts_info.get("reason") or "").strip().lower() if isinstance(ts_info, dict) else ""
    code = "SCHEMA_CONTRACT_MISMATCH"
    if reason == "missing_columns":
        code = "SCHEMA_MISSING_FIELD"
    elif reason in {"type_mismatch", "column_type_mismatch"}:
        code = "SCHEMA_TYPE_MISMATCH"
    return ContractMismatchError(
        "Expected output schema mismatch",
        code=code,
        details=_contract_details(
            missing_columns=ts_info.get("missingColumns") if isinstance(ts_info, dict) else [],
            expected={
                "typedSchema": expected_typed,
                "coercionPolicy": policy,
            },
            actual={
                "typedSchema": actual_typed,
                "mismatchedColumns": ts_info.get("mismatchedColumns") if isinstance(ts_info, dict) else [],
            },
        ),
    )


def _expected_mime_for_payload_type(payload_type: str) -> str:
    p = str(payload_type or "").strip().lower()
    if p == "json":
        return "application/json"
    if p == "table":
        return "text/csv; charset=utf-8"
    if p == "text":
        return "text/plain; charset=utf-8"
    if p == "embeddings":
        return "application/json"
    if p == "image":
        return "image/*"
    if p == "audio":
        return "audio/*"
    if p == "video":
        return "video/*"
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


def _schema_fp_for_artifact(
    *,
    payload_schema: Optional[Dict[str, Any]],
    observed_typed_schema: Optional[Dict[str, Any]] = None,
    expected_typed_schema: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Compute a stable schema fingerprint for artifact metadata.
    Includes payload schema and (when available) typed schema envelopes.
    """
    schema_payload = dict(payload_schema) if isinstance(payload_schema, dict) else {}
    if isinstance(schema_payload, dict):
        schema_payload.pop("artifactMetadataV1", None)
    if isinstance(observed_typed_schema, dict):
        schema_payload["typedSchemaObserved"] = canonical_json(observed_typed_schema)
    if isinstance(expected_typed_schema, dict):
        schema_payload["typedSchemaExpected"] = canonical_json(expected_typed_schema)
    return contract_schema_fingerprint(schema_payload)


def _normalize_mime_strict(mime_type: str) -> str:
    return str(mime_type or "").strip().lower()


def _transform_op_for_node(node: Dict[str, Any]) -> str:
    data = (node.get("data", {}) or {}) if isinstance(node, dict) else {}
    params = (data.get("params", {}) or {}) if isinstance(data, dict) else {}
    op = str(params.get("op") or "").strip().lower()
    if op:
        return op
    return str(data.get("transformKind") or "").strip().lower()


def _declared_out_port(kind: str, node: Dict[str, Any]) -> Optional[str]:
    params = (node.get("data", {}).get("params", {}) or {})
    if kind in {"llm", "model"}:
        typed_type = _node_typed_schema_type_from_node(node)
        if typed_type in {"json", "embeddings", "text"}:
            return typed_type
        return "text"
    if kind == "source":
        declared_typed = _declared_expected_typed_schema_from_node(node)
        if isinstance(declared_typed, dict):
            declared_type = str(declared_typed.get("type") or "").strip().lower()
            if declared_type in {"table", "text", "json", "binary", "image", "audio", "video"}:
                return declared_type
        source_kind = str(params.get("sourceKind") or params.get("source_type") or "file").strip().lower()
        if source_kind in {"file", "object_store"}:
            file_format = str(params.get("file_format") or "").strip().lower()
            if file_format in {"csv", "tsv", "parquet", "arrow", "feather", "xlsx", "xls"}:
                return "table"
            if file_format in {"json", "jsonl"}:
                return "json"
            if file_format in {"txt", "pdf"}:
                return "text"
            if file_format in {
                "jpg",
                "jpeg",
                "png",
                "webp",
                "gif",
                "svg",
                "tif",
                "tiff",
            }:
                return "image"
            if file_format in {
                "mp3",
                "wav",
                "flac",
                "ogg",
                "m4a",
                "aac",
            }:
                return "audio"
            if file_format in {
                "mp4",
                "mov",
                "webm",
            }:
                return "video"
            return "text"
        if source_kind == "api":
            return "json"
        if source_kind in {"database", "warehouse"}:
            return "table"
        if source_kind == "json":
            return "json"
        if source_kind == "table":
            return "table"
        return "text"
    if kind == "transform":
        transform_op = _transform_op_for_node(node)
        if transform_op in {"table_to_json", "json_filter"}:
            return "json"
        return "table"
    if kind == "tool":
        typed_type = _node_typed_schema_type_from_node(node)
        if typed_type in {"json", "text", "binary"}:
            return typed_type
        return None
    if kind == "component":
        api = params.get("api") if isinstance(params.get("api"), dict) else {}
        outputs = api.get("outputs") if isinstance(api.get("outputs"), list) else []
        if len(outputs) == 1 and isinstance(outputs[0], dict):
            typed = outputs[0].get("typedSchema") if isinstance(outputs[0].get("typedSchema"), dict) else {}
            out_pt = str(typed.get("type") or "").strip().lower()
            if out_pt == "string":
                out_pt = "text"
            return out_pt or None
    return None


def _declared_in_port(kind: str, node: Dict[str, Any], input_port: Optional[str] = None) -> Optional[str]:
    params = (node.get("data", {}).get("params", {}) or {})
    schema = (node.get("data", {}).get("schema", {}) or {})
    input_handle = str(input_port or "in").strip() or "in"
    expected_input_schemas = schema.get("expectedInputSchemas") if isinstance(schema.get("expectedInputSchemas"), dict) else {}
    expected_input_envelope = (
        expected_input_schemas.get(input_handle)
        if isinstance(expected_input_schemas.get(input_handle), dict)
        else expected_input_schemas.get("in")
        if isinstance(expected_input_schemas.get("in"), dict)
        else None
    )
    if isinstance(expected_input_envelope, dict):
        typed = expected_input_envelope.get("typedSchema") if isinstance(expected_input_envelope.get("typedSchema"), dict) else {}
        typed_type = str(typed.get("type") or "").strip().lower()
        if typed_type == "string":
            typed_type = "text"
        if typed_type in {"table", "json", "text", "binary", "embeddings", "image", "audio", "video"}:
            return typed_type
    if kind == "source":
        return None
    if kind == "transform":
        transform_op = _transform_op_for_node(node)
        if transform_op == "json_to_table":
            return "json"
        if transform_op == "text_to_table":
            return "text"
        if transform_op == "table_to_json":
            return "table"
        if transform_op == "json_filter":
            return "json"
        return "table"
    if kind == "llm":
        return "text"
    if kind == "model":
        model_kind = str(node.get("data", {}).get("modelKind") or "").strip().lower()
        if model_kind == "vision":
            return "image"
        if model_kind == "audio":
            return "audio"
        return "text"
    if kind == "tool":
        return None
    if kind == "component":
        api = params.get("api") if isinstance(params.get("api"), dict) else {}
        inputs = api.get("inputs") if isinstance(api.get("inputs"), list) else []
        if len(inputs) == 1 and isinstance(inputs[0], dict):
            typed = inputs[0].get("typedSchema") if isinstance(inputs[0].get("typedSchema"), dict) else {}
            in_pt = str(typed.get("type") or "").strip().lower()
            if in_pt == "string":
                in_pt = "text"
            return in_pt or None
    return None


def _resolve_join_placeholder_node_ids(
    clauses: List[Dict[str, Any]],
    connected_nodes: List[str],
) -> tuple[List[Dict[str, Any]], Dict[str, str]]:
    if not clauses:
        return clauses, {}
    ordered = [str(n).strip() for n in connected_nodes if str(n).strip()]
    if len(ordered) < 2:
        return clauses, {}
    left_default = ordered[0]
    right_default = ordered[1]
    placeholder_map: Dict[str, str] = {}
    resolved: List[Dict[str, Any]] = []
    left_tokens = {"upstream_left", "left"}
    right_tokens = {"upstream_right", "right"}
    for clause in clauses:
        if not isinstance(clause, dict):
            resolved.append(clause)
            continue
        next_clause = dict(clause)
        left_node = str(next_clause.get("leftNodeId") or "").strip()
        right_node = str(next_clause.get("rightNodeId") or "").strip()
        if left_node in left_tokens:
            next_clause["leftNodeId"] = left_default
            placeholder_map[left_node] = left_default
        if right_node in right_tokens or right_node in left_tokens:
            next_clause["rightNodeId"] = right_default
            placeholder_map[right_node] = right_default
        left_resolved = str(next_clause.get("leftNodeId") or "").strip()
        right_resolved = str(next_clause.get("rightNodeId") or "").strip()
        if left_resolved and right_resolved and left_resolved == right_resolved:
            for candidate in ordered:
                if candidate != left_resolved:
                    next_clause["rightNodeId"] = candidate
                    break
        resolved.append(next_clause)
    return resolved, placeholder_map


def _cached_artifact_contract_mismatch(
    kind: str,
    node: Dict[str, Any],
    artifact: Artifact,
    expected_schema: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    expected_schema = expected_schema or {}
    declared = _declared_out_port(kind, node) or "unknown"
    expected_schema_fingerprint = str(expected_schema.get("schemaFingerprint") or "")
    expected_schema_source = str(expected_schema.get("schemaSource") or "unknown")
    if not expected_schema_fingerprint:
        return None
    actual_schema_fingerprint = _artifact_schema_fingerprint(artifact)
    expected_mime = _normalize_mime_strict(_expected_mime_for_payload_type(declared))
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


def _env_int_allow_zero(name: str, default: int) -> int:
    raw = str(__import__("os").environ.get(name, "")).strip()
    if not raw:
        return max(0, int(default))
    try:
        val = int(raw)
    except Exception:
        return max(0, int(default))
    return max(0, val)


def _env_bool(name: str, default: bool) -> bool:
    raw = str(__import__("os").environ.get(name, "")).strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _sha256_text(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _determinism_fingerprint(determinism_env: Optional[Dict[str, Any]]) -> str:
    normalized = _sanitize_for_fingerprint(dict(determinism_env or {}))
    return sha256_hex(canonical_json(normalized).encode("utf-8"))


def _determinism_code_hash_from_env(determinism_env: Optional[Dict[str, Any]]) -> str:
    env = determinism_env if isinstance(determinism_env, dict) else {}
    return str(env.get("executor_code_hash") or "").strip()


def _determinism_profile_lock_from_env(determinism_env: Optional[Dict[str, Any]]) -> Optional[str]:
    env = determinism_env if isinstance(determinism_env, dict) else {}
    raw = env.get("tool_profile_lock")
    lock = str(raw or "").strip()
    return lock or None


def _short_fingerprint(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return raw[:12]


def _upstream_signature(upstream_ids: list[str]) -> Dict[str, Any]:
    ids = sorted(str(aid) for aid in (upstream_ids or []) if str(aid).strip())
    digest = _sha256_text(canonical_json(ids))
    return {"count": len(ids), "digest": digest[:12]}


def _cache_key_debug_payload(
    *,
    node_id: str,
    reason: str,
    meta: Dict[str, Any],
    node_state_hash: str,
    upstream_ids: list[str],
    determinism_env: Dict[str, Any],
    node_impl_version: str,
) -> Dict[str, Any]:
    prev_upstream = (
        [str(aid) for aid in (meta.get("upstreamArtifactIds") or []) if isinstance(aid, str) and aid.strip()]
        if isinstance(meta.get("upstreamArtifactIds"), list)
        else []
    )
    prev_det_fp = str(meta.get("determinismFingerprint") or "").strip()
    cur_det_fp = _determinism_fingerprint(determinism_env)
    prev_code_hash = str(meta.get("codeHash") or "").strip()
    cur_code_hash = _determinism_code_hash_from_env(determinism_env)
    prev_profile_lock = str(meta.get("profileLock") or "").strip()
    cur_profile_lock = str(_determinism_profile_lock_from_env(determinism_env) or "").strip()
    return {
        "nodeId": str(node_id or ""),
        "reason": str(reason or ""),
        "previous": {
            "nodeImplVersion": str(meta.get("nodeImplVersion") or ""),
            "paramsFingerprint": _short_fingerprint(meta.get("paramsFingerprint")),
            "upstream": _upstream_signature(prev_upstream),
            "determinismFingerprint": _short_fingerprint(prev_det_fp),
            "codeHash": _short_fingerprint(prev_code_hash),
            "profileLock": _short_fingerprint(prev_profile_lock),
        },
        "current": {
            "nodeImplVersion": str(node_impl_version or ""),
            "paramsFingerprint": _short_fingerprint(node_state_hash),
            "upstream": _upstream_signature(upstream_ids),
            "determinismFingerprint": _short_fingerprint(cur_det_fp),
            "codeHash": _short_fingerprint(cur_code_hash),
            "profileLock": _short_fingerprint(cur_profile_lock),
        },
    }


def _maybe_log_cache_key_diff(
    *,
    node_id: str,
    reason: str,
    meta: Dict[str, Any],
    node_state_hash: str,
    upstream_ids: list[str],
    determinism_env: Dict[str, Any],
    node_impl_version: str,
) -> None:
    if not _env_bool("CACHE_KEY_DEBUG", False):
        return
    try:
        payload = _cache_key_debug_payload(
            node_id=node_id,
            reason=reason,
            meta=meta,
            node_state_hash=node_state_hash,
            upstream_ids=upstream_ids,
            determinism_env=determinism_env,
            node_impl_version=node_impl_version,
        )
        logger.info("[cache-key-diff] %s", json.dumps(payload, sort_keys=True, separators=(",", ":")))
    except Exception:
        logger.debug("[cache-key-diff] failed to emit debug payload", exc_info=True)


def _executor_code_hash_for_kind(kind: str) -> str:
    key = str(kind or "").strip().lower()
    fn: Any = None
    if key == "source":
        fn = exec_source
    elif key == "transform":
        fn = run_transform
    elif key in {"llm", "model"}:
        fn = exec_llm
    elif key == "tool":
        fn = exec_tool
    fn_id = id(fn) if fn is not None else 0
    cache_key = (key, fn_id)
    cached = _EXECUTOR_CODE_HASH_CACHE.get(cache_key)
    if cached:
        return cached

    digest_input = ""
    try:
        if fn is not None:
            digest_input = inspect.getsource(fn)
    except Exception:
        digest_input = ""
    if not digest_input:
        try:
            module = inspect.getmodule(fn) if fn is not None else None
            module_path = getattr(module, "__file__", None)
            if isinstance(module_path, str) and module_path:
                with open(module_path, "rb") as f:
                    return hashlib.sha256(f.read()).hexdigest()
        except Exception:
            pass
    if not digest_input:
        digest_input = f"{key}:{str(getattr(fn, '__qualname__', 'unknown'))}"
    digest = _sha256_text(digest_input)
    _EXECUTOR_CODE_HASH_CACHE[cache_key] = digest
    return digest


def _tool_profile_lock(params: Dict[str, Any]) -> Optional[str]:
    p = params if isinstance(params, dict) else {}
    provider = str(p.get("provider") or "").strip().lower()
    if provider != "builtin":
        return None
    builtin_cfg = p.get("builtin")
    if not isinstance(builtin_cfg, dict):
        builtin_cfg = {}
    try:
        resolved = resolve_builtin_environment(builtin_cfg)
    except Exception:
        profile_id = str((builtin_cfg or {}).get("profileId") or "core").strip() or "core"
        return f"invalid:{profile_id}"
    profile_id = str(resolved.get("profileId") or "core").strip() or "core"
    install_target = str(
        resolved.get("installTarget")
        or BUILTIN_PROFILE_INSTALL_TARGETS.get(profile_id, "cpu_dev")
    ).strip() or "cpu_dev"
    packages = [
        str(pkg).strip()
        for pkg in (resolved.get("packages") if isinstance(resolved.get("packages"), list) else [])
        if str(pkg).strip()
    ]
    packages_key = "|".join(sorted(packages))
    return f"profile:{profile_id}:{install_target}:{_sha256_text(packages_key)}"


def _expected_tool_profile_locks(resolved_env: Dict[str, Any]) -> Dict[str, str]:
    profile_id = str(resolved_env.get("profileId") or "core").strip() or "core"
    install_target = str(
        resolved_env.get("installTarget")
        or BUILTIN_PROFILE_INSTALL_TARGETS.get(profile_id, "cpu_dev")
    ).strip() or "cpu_dev"
    packages = [
        str(pkg).strip()
        for pkg in (resolved_env.get("packages") if isinstance(resolved_env.get("packages"), list) else [])
        if str(pkg).strip()
    ]
    lock_hash = _sha256_text("|".join(sorted(packages)))
    return {
        "canonical": f"profile:{profile_id}:{install_target}:{lock_hash}",
        "sha256": f"sha256:{lock_hash}",
    }


def _determinism_env_for_node(kind: str, params: Dict[str, Any]) -> Dict[str, Any]:
    env: Dict[str, Any]
    if kind in {"llm", "model"}:
        output_cfg = params.get("output") if isinstance(params.get("output"), dict) else {}
        output_schema = params.get("output_schema")
        if output_schema is None and isinstance(output_cfg, dict):
            output_schema = output_cfg.get("jsonSchema")
        embedding_contract = params.get("embedding_contract")
        if embedding_contract is None and isinstance(output_cfg, dict):
            embedding_contract = output_cfg.get("embedding")
        llm_output_type = "text"
        if isinstance(embedding_contract, dict) and embedding_contract:
            llm_output_type = "embeddings"
        elif isinstance(output_schema, dict) and output_schema:
            llm_output_type = "json"
        input_encoding = str((params.get("input_encoding") or params.get("inputEncoding") or "text"))
        env = {
            "llm_input_format": "artifact_input_v2",
            "llm_input_encoding": input_encoding,
            "llm_table_format": "csv_v1",
            "llm_table_max_rows": _env_int("LLM_TABLE_MAX_ROWS", 200),
            "llm_table_max_cols": _env_int("LLM_TABLE_MAX_COLS", 50),
            "llm_prompt_max_chars": _env_int("LLM_PROMPT_MAX_CHARS", 20000),
            "llm_table_sort_rows": _env_bool("LLM_TABLE_SORT_ROWS", True),
            "llm_output_type": llm_output_type,
        }
    elif kind == "transform":
        env = {"transform_engine": "duckdb"}
    elif kind == "tool":
        provider = str((params.get("provider") if isinstance(params, dict) else "") or "").strip().lower()
        env = {"tool_provider": provider}
        profile_lock = _tool_profile_lock(params if isinstance(params, dict) else {})
        if profile_lock:
            env["tool_profile_lock"] = profile_lock
    else:
        env = {}
    env["executor_code_hash"] = _executor_code_hash_for_kind(kind)
    return env


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


def _artifact_meta_v1_from_artifact(artifact: Optional[Artifact]) -> Dict[str, Any]:
    if artifact is None:
        return {}
    payload_schema = artifact.payload_schema if isinstance(artifact.payload_schema, dict) else {}
    meta = payload_schema.get("artifactMetadataV1") if isinstance(payload_schema.get("artifactMetadataV1"), dict) else {}
    return meta if isinstance(meta, dict) else {}


async def _classify_cache_miss_reason(
    *,
    context: GraphContext,
    node_id: str,
    exec_key: str,
    node_state_hash: str,
    upstream_ids: list[str],
    determinism_env: Dict[str, Any],
    node_impl_version: str,
) -> str:
    latest_fn = getattr(context.artifact_store, "get_latest_node_artifact", None)
    if not callable(latest_fn):
        return "CACHE_ENTRY_MISSING"
    latest_artifact_id = await latest_fn(
        graph_id=context.graph_id,
        node_id=node_id,
        exclude_artifact_id=exec_key,
    )
    if not isinstance(latest_artifact_id, str) or not latest_artifact_id.strip():
        return "CACHE_ENTRY_MISSING"
    try:
        previous_artifact = await context.artifact_store.get(str(latest_artifact_id))
    except Exception:
        return "CACHE_ENTRY_MISSING"
    meta = _artifact_meta_v1_from_artifact(previous_artifact)
    if not meta:
        return "CACHE_ENTRY_MISSING"

    prev_code_hash = str(meta.get("codeHash") or "").strip()
    cur_code_hash = _determinism_code_hash_from_env(determinism_env)
    if prev_code_hash and cur_code_hash and prev_code_hash != cur_code_hash:
        _maybe_log_cache_key_diff(
            node_id=node_id,
            reason="BUILD_CHANGED",
            meta=meta,
            node_state_hash=node_state_hash,
            upstream_ids=upstream_ids,
            determinism_env=determinism_env,
            node_impl_version=node_impl_version,
        )
        return "BUILD_CHANGED"

    if str(meta.get("nodeImplVersion") or "") != str(node_impl_version or ""):
        _maybe_log_cache_key_diff(
            node_id=node_id,
            reason="BUILD_CHANGED",
            meta=meta,
            node_state_hash=node_state_hash,
            upstream_ids=upstream_ids,
            determinism_env=determinism_env,
            node_impl_version=node_impl_version,
        )
        return "BUILD_CHANGED"

    params_fingerprint = str(meta.get("paramsFingerprint") or "").strip()
    if params_fingerprint and params_fingerprint != str(node_state_hash or ""):
        _maybe_log_cache_key_diff(
            node_id=node_id,
            reason="PARAMS_CHANGED",
            meta=meta,
            node_state_hash=node_state_hash,
            upstream_ids=upstream_ids,
            determinism_env=determinism_env,
            node_impl_version=node_impl_version,
        )
        return "PARAMS_CHANGED"

    upstream_present = "upstreamArtifactIds" in meta and isinstance(meta.get("upstreamArtifactIds"), list)
    if upstream_present:
        prev_upstream = [
            str(aid)
            for aid in (meta.get("upstreamArtifactIds") if isinstance(meta.get("upstreamArtifactIds"), list) else [])
            if isinstance(aid, str) and aid.strip()
        ]
        if sorted(prev_upstream) != sorted(str(aid) for aid in (upstream_ids or []) if str(aid).strip()):
            _maybe_log_cache_key_diff(
                node_id=node_id,
                reason="INPUT_CHANGED",
                meta=meta,
                node_state_hash=node_state_hash,
                upstream_ids=upstream_ids,
                determinism_env=determinism_env,
                node_impl_version=node_impl_version,
            )
            return "INPUT_CHANGED"

    prev_det_fp = str(meta.get("determinismFingerprint") or "").strip()
    cur_det_fp = _determinism_fingerprint(determinism_env)
    if prev_det_fp and prev_det_fp != cur_det_fp:
        _maybe_log_cache_key_diff(
            node_id=node_id,
            reason="ENV_CHANGED",
            meta=meta,
            node_state_hash=node_state_hash,
            upstream_ids=upstream_ids,
            determinism_env=determinism_env,
            node_impl_version=node_impl_version,
        )
        return "ENV_CHANGED"

    prev_profile_lock = str(meta.get("profileLock") or "").strip()
    cur_profile_lock = str(_determinism_profile_lock_from_env(determinism_env) or "").strip()
    if prev_profile_lock and cur_profile_lock and prev_profile_lock != cur_profile_lock:
        _maybe_log_cache_key_diff(
            node_id=node_id,
            reason="ENV_CHANGED",
            meta=meta,
            node_state_hash=node_state_hash,
            upstream_ids=upstream_ids,
            determinism_env=determinism_env,
            node_impl_version=node_impl_version,
        )
        return "ENV_CHANGED"

    if not params_fingerprint and not upstream_present and not prev_det_fp and not prev_profile_lock:
        _maybe_log_cache_key_diff(
            node_id=node_id,
            reason="CACHE_ENTRY_MISSING",
            meta=meta,
            node_state_hash=node_state_hash,
            upstream_ids=upstream_ids,
            determinism_env=determinism_env,
            node_impl_version=node_impl_version,
        )
        return "CACHE_ENTRY_MISSING"

    _maybe_log_cache_key_diff(
        node_id=node_id,
        reason="CACHE_ENTRY_MISSING",
        meta=meta,
        node_state_hash=node_state_hash,
        upstream_ids=upstream_ids,
        determinism_env=determinism_env,
        node_impl_version=node_impl_version,
    )
    return "CACHE_ENTRY_MISSING"



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
    
    logger.debug("run_graph_context_initialized")
    context.bus.graph_id = graph_id
    component_parent_for_internal: Dict[str, str] = {}
    component_meta_by_parent: Dict[str, Dict[str, Any]] = {}
    execution_nodes_by_id: Dict[str, Dict[str, Any]] = {}

    def _component_path_for_log_node(node_id: Optional[str]) -> Optional[list[str]]:
        raw = str(node_id or "").strip()
        if not raw:
            return None
        component_instance_ids: list[str] = []
        if raw.startswith("cmp:"):
            cursor = raw
            guard = 0
            while cursor.startswith("cmp:") and guard < 32:
                guard += 1
                rest = cursor[4:]
                sep = rest.find(":")
                if sep <= 0:
                    break
                instance_id = rest[:sep].strip()
                if not instance_id:
                    break
                component_instance_ids.append(instance_id)
                cursor = rest[sep + 1:]
        else:
            parent_id = component_parent_for_internal.get(raw)
            if parent_id:
                component_instance_ids.append(parent_id)
        if not component_instance_ids:
            return None
        names: list[str] = []
        for instance_id in component_instance_ids:
            meta = component_meta_by_parent.get(instance_id, {})
            component_id = str(meta.get("componentId") or "").strip()
            if not component_id:
                node = execution_nodes_by_id.get(instance_id, {})
                data = node.get("data") if isinstance(node, dict) else {}
                params = data.get("params") if isinstance(data, dict) else {}
                component_ref = params.get("componentRef") if isinstance(params, dict) else {}
                component_id = str(component_ref.get("componentId") or "").strip() if isinstance(component_ref, dict) else ""
            names.append(component_id or instance_id)
        return names or None

    async def _emit(evt: Dict[str, Any]) -> None:
        payload = evt
        if isinstance(payload, dict) and str(payload.get("type") or "") == "log":
            component_path = _component_path_for_log_node(payload.get("nodeId"))
            if component_path and "componentPath" not in payload:
                payload = {**payload, "componentPath": component_path}
        await context.bus.emit(payload)

    cache = cache or ExecutionCache()
    cache_stats = {"hit": 0, "miss": 0, "hit_contract_mismatch": 0}
    cache_summary_emitted = False
    run_telemetry_emitted = False
    feature_flags = get_feature_flags()
    strict_schema_edge_checks = bool(feature_flags.get("STRICT_SCHEMA_EDGE_CHECKS", True))
    strict_coercion_policy = bool(feature_flags.get("STRICT_COERCION_POLICY", True))
    schema_stats_start = get_schema_infer_stats()
    run_started_t = asyncio.get_running_loop().time()
    max_nodes_per_run = _env_int("RUNNER_MAX_NODES", 2000, minimum=1)
    max_edges_per_run = _env_int_allow_zero("RUNNER_MAX_EDGES", 5000)
    max_runtime_ms = _env_int_allow_zero("RUNNER_MAX_RUNTIME_MS", 0)
    max_inflight = _env_int("RUNNER_MAX_CONCURRENCY", 4, minimum=1)
    max_source = _env_int("RUNNER_MAX_SOURCE", 2, minimum=1)
    max_transform = _env_int("RUNNER_MAX_TRANSFORM", 2, minimum=1)
    # Default model concurrency is intentionally conservative (1) for low-VRAM hosts.
    # Override with RUNNER_MAX_MODEL (or legacy RUNNER_MAX_LLM) to increase later.
    max_model = _env_int("RUNNER_MAX_MODEL", _env_int("RUNNER_MAX_LLM", 1, minimum=1), minimum=1)
    max_llm = max_model
    max_tool = _env_int("RUNNER_MAX_TOOL", 2, minimum=1)
    node_retry_max_attempts = _env_int_allow_zero("RUNNER_NODE_MAX_RETRIES", 0)
    node_retry_backoff_ms = _env_int_allow_zero("RUNNER_NODE_RETRY_BACKOFF_MS", 0)
    reproducibility_metadata = {
        "schemaVersion": 1,
        "executionVersion": str(context.execution_version or ""),
        "featureFlags": {
            "strictSchemaEdgeChecks": strict_schema_edge_checks,
            "strictCoercionPolicy": strict_coercion_policy,
        },
        "guardrails": {
            "maxNodes": int(max_nodes_per_run),
            "maxEdges": int(max_edges_per_run),
            "maxRuntimeMs": int(max_runtime_ms),
            "concurrencyCaps": {
                "global": int(max_inflight),
                "source": int(max_source),
                "transform": int(max_transform),
                "model": int(max_model),
                "llm": int(max_llm),
                "tool": int(max_tool),
            },
            "retryPolicy": {
                "maxAttempts": int(node_retry_max_attempts),
                "backoffMs": int(node_retry_backoff_ms),
            },
        },
    }
    peak_concurrency = 0
    total_cached = 0
    total_succeeded = 0
    total_failed = 0

    async def _emit_run_telemetry_once() -> None:
        nonlocal run_telemetry_emitted
        if run_telemetry_emitted:
            return
        run_telemetry_emitted = True
        schema_stats_end = get_schema_infer_stats()
        schema_delta = {
            "hit": int(schema_stats_end.get("hit", 0) - schema_stats_start.get("hit", 0)),
            "miss": int(schema_stats_end.get("miss", 0) - schema_stats_start.get("miss", 0)),
            "bypass": int(schema_stats_end.get("bypass", 0) - schema_stats_start.get("bypass", 0)),
        }
        await _emit({
            "type": "run_telemetry",
            "schema_version": 1,
            "runId": run_id,
            "at": iso_now(),
            "runtime_ms": max(0, int((asyncio.get_running_loop().time() - run_started_t) * 1000.0)),
            "peak_concurrency": int(peak_concurrency),
            "executed": int(total_succeeded + total_failed),
            "cached": int(total_cached),
            "failed": int(total_failed),
            "planned": int(len(getattr(getattr(context, "planner_ref", None), "subgraph", []) or [])),
            "cache_hit": int(cache_stats["hit"]),
            "cache_miss": int(cache_stats["miss"]),
            "cache_hit_contract_mismatch": int(cache_stats["hit_contract_mismatch"]),
            "schema_infer": schema_delta,
            "strict_schema_edge_checks": strict_schema_edge_checks,
            "strict_coercion_policy": strict_coercion_policy,
            "reproducibility": reproducibility_metadata,
        })

    async def _emit_cache_decision(
        *,
        node_id: str,
        node_kind: str,
        decision: str,
        exec_key: str,
        artifact_id: Optional[str] = None,
        expected_payload_type: Optional[str] = None,
        actual_payload_type: Optional[str] = None,
        producer_exec_key: Optional[str] = None,
        expected_schema_source: Optional[str] = None,
        expected_contract_fingerprint: Optional[str] = None,
        actual_contract_fingerprint: Optional[str] = None,
        mismatch_kind: Optional[str] = None,
        reason: Optional[str] = None,
        detail: Optional[Dict[str, Any]] = None,
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
        if expected_payload_type:
            evt["expectedType"] = expected_payload_type
        if actual_payload_type:
            evt["actualType"] = actual_payload_type
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
        if isinstance(detail, dict) and detail:
            evt["detail"] = detail
        await _emit(evt)
        await _emit(
            {
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "info",
                "message": (
                    f"[cache] node={node_id} kind={node_kind} decision={d} "
                    f"reason={resolved_reason} exec_key={str(exec_key)[:12]}"
                ),
                "nodeId": node_id,
            }
        )

    async def _emit_cache_summary_once() -> None:
        nonlocal cache_summary_emitted
        if cache_summary_emitted:
            return
        cache_summary_emitted = True
        await _emit_run_telemetry_once()
        await _emit({
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
            await _emit({
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "error",
                "message": f"[{error.code}] {error.message}",
                "nodeId": error.node_id
            })
        await _emit({
            "type": "run_finished",
            "runId": run_id,
            "at": iso_now(),
            "status": "failed"
        })
        await _emit_cache_summary_once()
        return

    for warning in validation.warnings:
        if str(getattr(warning, "code", "")).strip().upper() == "EDGE_CONTRACT_DRIFT":
            details = warning.details if isinstance(warning.details, dict) else {}
            await _emit({
                "type": "contract_drift",
                "runId": run_id,
                "at": iso_now(),
                "edgeId": str(getattr(warning, "edge_id", "") or details.get("edgeId") or ""),
                "sourceNodeId": str(details.get("sourceNodeId") or ""),
                "targetNodeId": str(details.get("targetNodeId") or ""),
                "sourceHandle": str(details.get("sourceHandle") or "out"),
                "targetHandle": str(details.get("targetHandle") or "in"),
                "snapshotSourceSchemaFingerprint": str(details.get("snapshotSourceSchemaFingerprint") or ""),
                "snapshotTargetSchemaFingerprint": str(details.get("snapshotTargetSchemaFingerprint") or ""),
                "currentSourceSchemaFingerprint": str(details.get("currentSourceSchemaFingerprint") or ""),
                "currentTargetSchemaFingerprint": str(details.get("currentTargetSchemaFingerprint") or ""),
                "suggestions": list(getattr(warning, "suggestions", None) or []),
            })
        await _emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "warn",
            "message": f"[{warning.code}] {warning.message}",
            "nodeId": warning.node_id
        })

    # ===== PHASE 1.5: COMPONENT EXPANSION =====
    component_expansion = None
    # Snapshot the graph at run start so rewires/edits apply on the next run only.
    execution_graph = copy.deepcopy(graph)
    component_store = getattr(runtime_ref, "component_revisions", None) if runtime_ref is not None else None
    try:
        component_expansion = expand_graph_components(
            copy.deepcopy(graph),
            component_store=component_store,
            max_depth=5,
        )
        execution_graph = component_expansion.graph
    except ComponentExpansionError as ex:
        await _emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "error",
            "message": f"[{ex.code}] {str(ex)}",
            "code": ex.code,
            "details": ex.details,
        })
        await _emit({
            "type": "run_finished",
            "runId": run_id,
            "at": iso_now(),
            "status": "failed",
            "error": str(ex),
            "errorCode": ex.code,
            "errorDetails": ex.details,
        })
        await _emit_cache_summary_once()
        return

    node_count = len(execution_graph.get("nodes", []) if isinstance(execution_graph, dict) else [])
    edge_count = len(execution_graph.get("edges", []) if isinstance(execution_graph, dict) else [])
    if node_count > max_nodes_per_run:
        msg = (
            f"RESOURCE_LIMIT_NODES: graph has {node_count} nodes, "
            f"limit is {max_nodes_per_run}"
        )
        await _emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "error",
            "message": msg,
        })
        await _emit({
            "type": "run_finished",
            "runId": run_id,
            "at": iso_now(),
            "status": "failed",
            "error": msg,
            "errorCode": "RESOURCE_LIMIT_NODES",
        })
        await _emit_cache_summary_once()
        return
    if max_edges_per_run > 0 and edge_count > max_edges_per_run:
        msg = (
            f"RESOURCE_LIMIT_EDGES: graph has {edge_count} edges, "
            f"limit is {max_edges_per_run}"
        )
        await _emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "error",
            "message": msg,
        })
        await _emit({
            "type": "run_finished",
            "runId": run_id,
            "at": iso_now(),
            "status": "failed",
            "error": msg,
            "errorCode": "RESOURCE_LIMIT_EDGES",
        })
        await _emit_cache_summary_once()
        return

    # ===== PHASE 2: EXECUTION =====
    try:
        raw_hints = graph.get("__executionHints") if isinstance(graph, dict) else None
        dirty_hint_ids = set()
        if isinstance(raw_hints, dict):
            raw_dirty = raw_hints.get("dirtyNodeIds")
            if isinstance(raw_dirty, list):
                dirty_hint_ids = {str(nid) for nid in raw_dirty if isinstance(nid, str) and str(nid).strip()}
        plan = compile_plan(
            execution_graph,
            run_from,
            run_mode=run_mode,
            dirty_node_ids=dirty_hint_ids,
        )
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
        await _emit({
            "type": "run_started",
            "runId": run_id,
            "at": iso_now(),
            "runFrom": run_from,
            "runMode": effective_run_mode,
            "plannedNodeIds": planned_node_ids,
            "reproducibility": reproducibility_metadata,
        })
        nodes = node_map(execution_graph)
        execution_nodes_by_id = nodes
        edges = edge_map(execution_graph)
        node_param_modes: Dict[str, str] = {nid: _node_runtime_param_mode(node) for nid, node in nodes.items()}
        node_param_snapshots: Dict[str, Dict[str, Any]] = {
            nid: copy.deepcopy((node.get("data", {}) or {}).get("params", {}) or {})
            for nid, node in nodes.items()
        }
        _work_input_overrides: contextvars.ContextVar[Dict[str, str]] = contextvars.ContextVar(
            "work_input_overrides", default={}
        )
        _active_work_batch: contextvars.ContextVar[List[Dict[str, Any]]] = contextvars.ContextVar(
            "active_work_batch", default=[]
        )

        def get_current_artifact(node_id_ref: str, source_handle: str = "out") -> Optional[str]:
            overrides = _work_input_overrides.get({})
            if isinstance(overrides, dict):
                aid = overrides.get(str(node_id_ref))
                if isinstance(aid, str) and aid.strip():
                    return aid.strip()
            return context.bindings.get_current_artifact(node_id_ref, handle=source_handle)
        component_runtime_state: Dict[str, Dict[str, Any]] = {}
        component_parent_for_internal = {}
        component_meta_by_parent = {}

        if component_expansion:
            component_parent_for_internal = dict(component_expansion.internal_to_parent)
            component_meta_by_parent = dict(component_expansion.parent_component_meta)
            for parent_node_id, internal_nodes in component_expansion.parent_to_internal.items():
                planned_internal = [nid for nid in internal_nodes if nid in plan.subgraph]
                if not planned_internal:
                    continue
                internal_set = set(planned_internal)
                internal_out_degree: Dict[str, int] = {nid: 0 for nid in planned_internal}
                for e in execution_graph.get("edges", []) if isinstance(execution_graph, dict) else []:
                    if not isinstance(e, dict):
                        continue
                    src = str(e.get("source") or "")
                    tgt = str(e.get("target") or "")
                    if src in internal_set and tgt in internal_set:
                        internal_out_degree[src] = int(internal_out_degree.get(src, 0)) + 1
                output_binding_edges = []
                for e in execution_graph.get("edges", []) if isinstance(execution_graph, dict) else []:
                    if not isinstance(e, dict):
                        continue
                    src = str(e.get("source") or "")
                    tgt = str(e.get("target") or "")
                    if tgt != parent_node_id:
                        continue
                    if src not in internal_set:
                        continue
                    output_binding_edges.append(e)
                component_runtime_state[parent_node_id] = {
                    "remaining": len(planned_internal),
                    "started": False,
                    "failed": False,
                    "started_at": None,
                    "output_binding_edges": output_binding_edges,
                    "builtin_profile_requirements": _collect_component_builtin_profile_requirements(
                        nodes_by_id=nodes,
                        internal_node_ids=planned_internal,
                    ),
                }

        async def _component_mark_node_start(node_id: str) -> None:
            current_parent_id = component_parent_for_internal.get(node_id)
            while current_parent_id:
                state = component_runtime_state.get(current_parent_id)
                if state and not state.get("started"):
                    state["started"] = True
                    state["started_at"] = asyncio.get_running_loop().time()
                    meta = component_meta_by_parent.get(current_parent_id, {})
                    await _emit({
                        "type": "component_started",
                        "runId": run_id,
                        "at": iso_now(),
                        "nodeId": current_parent_id,
                        "componentId": str(meta.get("componentId") or ""),
                        "componentRevisionId": str(meta.get("componentRevisionId") or ""),
                        "builtinEnvironment": state.get("builtin_profile_requirements"),
                    })
                    builtin_req = (
                        state.get("builtin_profile_requirements")
                        if isinstance(state.get("builtin_profile_requirements"), dict)
                        else {}
                    )
                    required_profiles = (
                        builtin_req.get("requiredProfiles")
                        if isinstance(builtin_req.get("requiredProfiles"), list)
                        else []
                    )
                    missing_profiles = (
                        builtin_req.get("missingProfiles")
                        if isinstance(builtin_req.get("missingProfiles"), list)
                        else []
                    )
                    invalid_profiles = (
                        builtin_req.get("invalidProfiles")
                        if isinstance(builtin_req.get("invalidProfiles"), list)
                        else []
                    )
                    if required_profiles:
                        profile_ids = [
                            (
                                f"{str(p.get('profileId') or '').strip()}@"
                                f"{str(p.get('installTarget') or BUILTIN_PROFILE_INSTALL_TARGETS.get(str(p.get('profileId') or '').strip(), 'cpu_dev')).strip()}"
                            )
                            for p in required_profiles
                            if isinstance(p, dict) and str(p.get("profileId") or "").strip()
                        ]
                        missing_parts: list[str] = []
                        for item in missing_profiles:
                            if not isinstance(item, dict):
                                continue
                            mid = str(item.get("profileId") or "").strip()
                            missing_pkgs = item.get("missingPackages") if isinstance(item.get("missingPackages"), list) else []
                            missing_text = ", ".join(str(pkg).strip() for pkg in missing_pkgs if str(pkg).strip())
                            if mid and missing_text:
                                missing_parts.append(f"{mid}({missing_text})")
                            elif mid:
                                missing_parts.append(mid)
                        invalid_parts = [
                            f"{str(item.get('nodeId') or '')}:{str(item.get('profileId') or 'core')}"
                            for item in invalid_profiles
                            if isinstance(item, dict)
                        ]
                        details: list[str] = []
                        if missing_parts:
                            details.append(
                                f"missing: {'; '.join(missing_parts)}. Install profile: POST /env/profiles/install."
                            )
                        if invalid_parts:
                            details.append(f"invalid tool profile config at {', '.join(invalid_parts)}.")
                        suffix = f" {' '.join(details)}" if details else ""
                        await _emit({
                            "type": "log",
                            "runId": run_id,
                            "at": iso_now(),
                            "level": "warn" if (missing_parts or invalid_parts) else "info",
                            "message": (
                                f"COMPONENT_ENV_PROFILE_REQUIREMENTS: Component requires builtin profiles: "
                                f"{', '.join(profile_ids)}.{suffix}"
                            ),
                            "nodeId": current_parent_id,
                            "requiredProfiles": required_profiles,
                            "missingProfiles": missing_profiles,
                            "invalidProfiles": invalid_profiles,
                        })
                current_parent_id = component_parent_for_internal.get(current_parent_id)

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
                current_parent_id = parent_node_id
                while current_parent_id:
                    current_state = component_runtime_state.get(current_parent_id)
                    if current_state and not current_state.get("failed"):
                        current_state["failed"] = True
                        meta = component_meta_by_parent.get(current_parent_id, {})
                        await _emit({
                            "type": "component_failed",
                            "runId": run_id,
                            "at": iso_now(),
                            "nodeId": current_parent_id,
                            "componentId": str(meta.get("componentId") or ""),
                            "componentRevisionId": str(meta.get("componentRevisionId") or ""),
                            "error": str(error or "Component internal node failed"),
                        })
                    current_parent_id = component_parent_for_internal.get(current_parent_id)
                return

            if state["remaining"] == 0 and not state.get("failed"):
                meta = component_meta_by_parent.get(parent_node_id, {})
                await _emit({
                    "type": "component_finished",
                    "runId": run_id,
                    "at": iso_now(),
                    "nodeId": parent_node_id,
                    "componentId": str(meta.get("componentId") or ""),
                    "componentRevisionId": str(meta.get("componentRevisionId") or ""),
                    "status": "succeeded",
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

        def _assert_binding_ready_for_commit(
            *,
            node_id: str,
            snapshot: tuple[Optional[str], Optional[str]],
            commit_artifact_id: str,
            phase: str,
        ) -> None:
            current = _binding_snapshot(node_id)
            # Normal case: unchanged since execution began.
            if current == snapshot:
                return
            # Idempotent re-commit case: already bound to this computed artifact.
            if current == (commit_artifact_id, "computed"):
                return
            raise RuntimeError(
                "Binding changed during execution before commit "
                f"(node_id={node_id}, phase={phase}, expected={snapshot}, actual={current})"
            )

        async def _resolve_node_execution(node_id: str, *, work_batch: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
            raw_node = nodes[node_id]
            kind = raw_node["data"]["kind"]
            if node_param_modes.get(node_id, "read_once") == "dynamic":
                params = raw_node["data"].get("params", {}) or {}
                node_param_snapshots[node_id] = copy.deepcopy(params)
            else:
                params = copy.deepcopy(node_param_snapshots.get(node_id, raw_node["data"].get("params", {}) or {}))
            n = copy.deepcopy(raw_node)
            n.setdefault("data", {})
            if isinstance(work_batch, list) and work_batch:
                params["_work_batch"] = copy.deepcopy(work_batch)
                params["_work_item"] = copy.deepcopy(work_batch[0])
            n["data"]["params"] = copy.deepcopy(params)
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
            processing_policy = _node_processing_policy(raw_node)
            if tool_uncacheable:
                cache_bypass_reason = "UNCACHEABLE_EFFECTFUL_TOOL"
            elif global_cache_mode == "force_off":
                cache_bypass_reason = "GLOBAL_FORCE_OFF"
            elif node_force_off:
                cache_bypass_reason = "NODE_POLICY_FORCE_OFF"
            elif global_cache_mode == "default_on" and node_prefer_off:
                cache_bypass_reason = "NODE_POLICY_PREFER_OFF"
            elif processing_policy.get("consume_mode") in {"single_item", "batch"}:
                cache_bypass_reason = "STREAMING_WORK_ITEM"
            use_cache_for_node = cache_bypass_reason is None

            up_nodes = sorted(upstream_node_ids(edges, node_id))
            upstream_ids = [aid for aid in (get_current_artifact(nid) for nid in up_nodes) if aid]
            resolve_input_refs_error: Optional[ContractMismatchError] = None
            try:
                input_refs = await resolve_input_refs(
                    edges,
                    node_id,
                    get_current_artifact,
                    lambda nid: nodes.get(nid),
                    context.artifact_store,
                )
            except ContractMismatchError as ex:
                resolve_input_refs_error = ex
                input_refs = []

            # Runtime placeholder injection for non-work handles, available to any node kind.
            # Supported tokens: {param_context}, {param_filters}, {control_in}
            placeholder_tokens = _collect_placeholder_tokens(params)
            injectable_handles = {
                token
                for token in placeholder_tokens
                if token in _INJECTABLE_NON_WORK_HANDLES
            }
            if injectable_handles and input_refs:
                injected_values = await _build_non_work_injection_values(
                    context,
                    input_refs,
                    handles_to_inject=injectable_handles,
                )
                if injected_values:
                    params = _inject_placeholders(params, injected_values)
                    n["data"]["params"] = copy.deepcopy(params)

            try:
                normalized_params_for_hash = _normalized_params_for_exec_key(
                    kind=kind,
                    node=n,
                    params=params,
                )
            except Exception as ex:
                # Do not fail node execution on hash-normalization errors; fall back to sanitized raw params.
                normalized_params_for_hash = _sanitize_for_fingerprint(dict(params or {}))
                if kind == "source":
                    normalized_params_for_hash.setdefault(
                        "source_type",
                        str(n.get("data", {}).get("sourceKind") or params.get("source_type") or "file"),
                    )
                await _emit({
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "warn",
                    "message": f"PARAM_NORMALIZE_FALLBACK: {str(ex)}",
                    "nodeId": node_id,
                })
            # Streaming work nodes must produce distinct execution keys per item/batch.
            # `_work_item`/`_work_batch` are runtime keys and intentionally excluded from the
            # general fingerprint sanitizer, so we add a stable runtime work fingerprint here.
            if isinstance(work_batch, list) and work_batch:
                runtime_work_fingerprint: List[Dict[str, Any]] = []
                for item in work_batch:
                    if not isinstance(item, dict):
                        continue
                    runtime_work_fingerprint.append(
                        {
                            "edgeId": str(item.get("edgeId") or ""),
                            "sourceNodeId": str(item.get("sourceNodeId") or ""),
                            "targetHandle": str(item.get("targetHandle") or "in"),
                            "artifactId": str(item.get("artifactId") or ""),
                            "itemMode": str(item.get("itemMode") or "artifact"),
                            "itemIndex": int(item.get("itemIndex") or 0),
                        }
                    )
                if runtime_work_fingerprint:
                    normalized_params_for_hash["runtime_work_fingerprint"] = runtime_work_fingerprint
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
            node_impl_version = _node_impl_version(kind)
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
                node_impl_version=node_impl_version,
            )
            expected_schema = _expected_schema_contract_for_node(n)
            cached_artifact_id = exec_key if (use_cache_for_node and await context.artifact_store.exists(exec_key)) else None
            cache_resolution = "CACHE_HIT" if cached_artifact_id else "CACHE_MISS"
            cache_miss_reason = "CACHE_ENTRY_MISSING"
            if use_cache_for_node and cache_resolution == "CACHE_MISS":
                cache_miss_reason = await _classify_cache_miss_reason(
                    context=context,
                    node_id=node_id,
                    exec_key=exec_key,
                    node_state_hash=node_state_hash,
                    upstream_ids=upstream_ids,
                    determinism_env=determinism_env,
                    node_impl_version=node_impl_version,
                )
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
                "tool_provider": tool_provider,
                "determinism_env": determinism_env,
                "tool_mode": tool_mode,
                "cache_bypass_reason": cache_bypass_reason,
                "use_cache_for_node": use_cache_for_node,
                "upstream_ids": upstream_ids,
                "input_refs": input_refs,
                "node_state_hash": node_state_hash,
                "node_impl_version": node_impl_version,
                "exec_key": exec_key,
                "artifactId": exec_key,
                "expected_schema": expected_schema,
                "cache_resolution": cache_resolution,
                "cachedArtifactId": cached_artifact_id,
                "cache_miss_reason": cache_miss_reason,
                "resolve_input_refs_error": resolve_input_refs_error,
            }

        async def _execute_node(
            node_id: str,
            *,
            cache_only: bool = False,
            work_batch: Optional[List[Dict[str, Any]]] = None,
        ) -> Dict[str, Any]:
            node_started_t = asyncio.get_running_loop().time()
            decision_value = "accept"
            decision_reason_code = ""
            binding_snapshot = _binding_snapshot(node_id)
            await _emit({
                "type": "node_started",
                "runId": run_id,
                "at": iso_now(),
                "nodeId": node_id
            })

            # Activate incoming edges
            for edge_id in plan.incoming_edges.get(node_id, []):
                await _emit({
                    "type": "edge_exec",
                    "runId": run_id,
                    "at": iso_now(),
                    "edgeId": edge_id,
                    "exec": "active"
                })

            work_batch_list = work_batch if isinstance(work_batch, list) else []
            override_map: Dict[str, str] = {}
            for item in work_batch_list:
                if not isinstance(item, dict):
                    continue
                src_node_id = str(item.get("sourceNodeId") or "").strip()
                artifact_id = str(item.get("artifactId") or "").strip()
                if src_node_id and artifact_id:
                    override_map[src_node_id] = artifact_id
            token_overrides = _work_input_overrides.set(override_map)
            token_batch = _active_work_batch.set(work_batch_list)
            try:
                resolved = await _resolve_node_execution(node_id, work_batch=work_batch_list)
            finally:
                _active_work_batch.reset(token_batch)
                _work_input_overrides.reset(token_overrides)
            n = resolved["node"]
            kind = resolved["kind"]
            params = resolved["params"]
            tool_provider = resolved["tool_provider"]
            determinism_env = resolved["determinism_env"]
            tool_mode = resolved["tool_mode"]
            cache_bypass_reason = resolved["cache_bypass_reason"]
            use_cache_for_node = resolved["use_cache_for_node"]
            upstream_ids = resolved["upstream_ids"]
            input_refs = resolved["input_refs"]
            node_state_hash = resolved["node_state_hash"]
            node_impl_version = resolved["node_impl_version"]
            exec_key = resolved["exec_key"]
            artifact_id = resolved["artifactId"]
            expected_schema = resolved["expected_schema"]
            cache_resolution = resolved["cache_resolution"]
            cached_artifact_id = resolved["cachedArtifactId"]
            cache_miss_reason = str(resolved.get("cache_miss_reason") or "CACHE_ENTRY_MISSING")
            resolve_input_refs_error = resolved.get("resolve_input_refs_error")
            expected_schema_source = str((expected_schema or {}).get("schemaSource") or "")

            if expected_schema_source.startswith("default:"):
                await _emit({
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

            declared_in = _declared_in_port(kind, n)
            preflight_error: Optional[ContractMismatchError] = resolve_input_refs_error
            if preflight_error is None:
                preflight_error = _tool_builtin_env_preflight_error(
                    kind=kind,
                    params=params,
                )
            # TKT-006/TKT-007: enforce edge-level input compatibility with explicit coercion policy.
            if (
                preflight_error is None
                and strict_schema_edge_checks
                and kind not in {"source", "component"}
                and input_refs
            ):
                coercion_policy = (
                    _coercion_policy_for_node(n) if strict_coercion_policy else "allow_lossy"
                )
                for input_port, upstream_id in input_refs:
                    input_port_name = str(input_port or "").strip().lower()
                    if input_port_name.startswith("param") or input_port_name.startswith("control") or input_port_name.startswith("ctl"):
                        # Param/control links are validated through affinity + param-shape contracts.
                        # Skip work-payload preflight checks here.
                        continue
                    upstream_art = await context.artifact_store.get(upstream_id)
                    upstream_pt = _infer_artifact_payload_type(upstream_art)
                    expected_in = str(_declared_in_port(kind, n, input_port=input_port) or declared_in or "").strip()
                    actual_ts = _artifact_typed_schema(upstream_art)
                    if expected_in and upstream_pt != expected_in:
                        if coercion_policy == "allow_lossy":
                            await _emit({
                                "type": "log",
                                "runId": run_id,
                                "at": iso_now(),
                                "level": "warn",
                                "message": (
                                    f"[COERCION_APPLIED] edge {upstream_id}->{node_id}:{input_port} "
                                    f"payload type {upstream_pt} coerced to {expected_in} (policy=allow_lossy)"
                                ),
                                "nodeId": node_id,
                            })
                        else:
                            preflight_error = ContractMismatchError(
                                "Work payload mismatch: upstream artifact payload type does not match expected input type",
                                code="CONTRACT_EDGE_PAYLOAD_TYPE_MISMATCH",
                                details=_contract_details(
                                    expected={"inputType": expected_in, "coercionPolicy": coercion_policy},
                                    actual={"artifactId": upstream_id, "actualType": upstream_pt, "inputPort": input_port},
                                ),
                            )
                            break
                    if expected_in == "table" and upstream_pt == "table":
                        ts_fields = actual_ts.get("fields") if isinstance(actual_ts.get("fields"), list) else []
                        has_typed_columns = any(
                            isinstance(f, dict) and str(f.get("name") or "").strip()
                            for f in ts_fields
                        )
                        if not has_typed_columns:
                            preflight_error = ContractMismatchError(
                                "Work payload mismatch: table input is missing typed schema columns",
                                code="CONTRACT_EDGE_TYPED_SCHEMA_MISSING",
                                details=_contract_details(
                                    expected={
                                        "inputType": "table",
                                        "typedSchema": {"type": "table", "fields": "non-empty"},
                                        "coercionPolicy": coercion_policy,
                                        "inputPort": input_port,
                                    },
                                    actual={
                                        "artifactId": upstream_id,
                                        "actualType": upstream_pt,
                                        "typedSchema": actual_ts or {},
                                    },
                                ),
                            )
                            break
                    expected_ts = _declared_component_input_schema(n, str(input_port or "in"))
                    ok_ts, ts_info = _typed_schema_compatibility(
                        expected=expected_ts,
                        actual=actual_ts,
                        policy=coercion_policy,
                    )
                    if not ok_ts:
                        preflight_error = ContractMismatchError(
                            "Work payload mismatch: typed schema incompatibility",
                            code="CONTRACT_EDGE_TYPED_SCHEMA_MISMATCH",
                            details=_contract_details(
                                missing_columns=ts_info.get("missingColumns") if isinstance(ts_info, dict) else [],
                                expected={
                                    "typedSchema": expected_ts or {},
                                    "coercionPolicy": coercion_policy,
                                    "inputPort": input_port,
                                },
                                actual={
                                    "artifactId": upstream_id,
                                    "typedSchema": actual_ts or {},
                                    "mismatchedColumns": (
                                        ts_info.get("mismatchedColumns")
                                        if isinstance(ts_info, dict)
                                        else []
                                    ),
                                },
                            ),
                        )
                        break
                    if bool(ts_info.get("coercionApplied")):
                        await _emit({
                            "type": "log",
                            "runId": run_id,
                            "at": iso_now(),
                            "level": "warn",
                            "message": (
                                f"[COERCION_APPLIED] edge {upstream_id}->{node_id}:{input_port} "
                                f"typed schema adapted (policy={coercion_policy})"
                            ),
                            "nodeId": node_id,
                        })

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
                    await _emit({
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
                    await _emit({
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
                        await _emit({
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
                    await _emit({
                        "type": "node_output",
                        "runId": run_id,
                        "nodeId": node_id,
                        "at": iso_now(),
                        "artifactId": cached_artifact_id,
                        "mimeType": cached_art.mime_type,
                        "payloadType": _infer_artifact_payload_type(cached_art),
                        "sourceObservability": _source_observability_from_artifact(cached_art),
                        "primingArtifact": _source_priming_artifact_from_artifact(cached_art),
                        "cached": True,
                    })

                    await _emit({
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
                        await _emit({
                            "type": "edge_exec",
                            "runId": run_id,
                            "at": iso_now(),
                            "edgeId": edge_id,
                            "exec": "done"
                        })
                    await asyncio.sleep(0.05)
                    return {
                        "ok": True,
                        "cached": True,
                        "decision": decision_value,
                        "reasonCode": decision_reason_code,
                    }
            if use_cache_for_node and cache_resolution == "CACHE_MISS":
                cache_stats["miss"] += 1
                await _emit_cache_decision(
                    node_id=node_id,
                    node_kind=kind,
                    decision="cache_miss",
                    exec_key=exec_key,
                    reason=cache_miss_reason,
                    detail={
                        "paramsFingerprint": node_state_hash,
                        "upstreamArtifactIds": sorted(upstream_ids),
                        "determinismFingerprint": _determinism_fingerprint(determinism_env),
                        "codeHash": _determinism_code_hash_from_env(determinism_env),
                        "profileLock": _determinism_profile_lock_from_env(determinism_env),
                    },
                )
                if cache_only:
                    msg = (
                        "Selected-only run requires cached ancestors, "
                        f"but cache entry was missing for node '{node_id}'."
                    )
                    await _emit({
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
                    transform_kind = str((n.get("data", {}) or {}).get("transformKind") or "").lower()
                    op_hint = str((params or {}).get("op") or "").lower()
                    transform_op = op_hint or transform_kind
                    allowed_in_contracts = (
                        {"json"} if transform_op == "json_to_table"
                        else {"json"} if transform_op == "json_filter"
                        else {"text"} if transform_op == "text_to_table"
                        else {"table"} if transform_op == "table_to_json"
                        else {"table", "json", "text"}
                    )
                    expected_out_contract = "json" if transform_op in {"table_to_json", "json_filter"} else "table"
                    # Transform contract is derived by operation.
                    in_contract = sorted(allowed_in_contracts)[0] if len(allowed_in_contracts) == 1 else "table"
                    out_contract = expected_out_contract

                    await _emit({
                        "type": "log",
                        "runId": run_id,
                        "at": iso_now(),
                        "level": "info",
                        "message": "transform: start",
                        "nodeId": node_id,
                    })
                    is_join_transform = transform_kind == "join" or op_hint == "join"
                    if is_join_transform:
                        join_edges = [e for e in edges.values() if e.get("target") == node_id]
                        if not join_edges:
                            msg = "join: ack input node=<none> sourceNode=<none> inputHandle=<none> artifact=<none> cols=[]"
                            await _emit({
                                "type": "log",
                                "runId": run_id,
                                "at": iso_now(),
                                "level": "info",
                                "message": msg,
                                "nodeId": node_id,
                            })
                            print(
                                f"[join-ack] runId={run_id} nodeId={node_id} "
                                "node=<none> sourceNode=<none> inputHandle=<none> artifact=<none> cols=[]"
                            )
                        else:
                            for e in join_edges:
                                source_node = str(e.get("source") or "<unknown>")
                                source_handle = str(e.get("sourceHandle") or "out")
                                input_handle = str(e.get("targetHandle") or "in")
                                source_artifact = get_current_artifact(source_node, source_handle)
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
                                    f"inputHandle={input_handle} artifact={artifact_label} cols={cols_label}"
                                )
                                await _emit({
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
                                    f"inputHandle={input_handle} artifact={artifact_label} cols={cols_label}"
                                )

                    if not params.get("enabled", True):
                        await _emit({
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
                        # 1) collect upstream artifacts (inputHandle -> artifactId)
                        input_refs = await resolve_input_refs(
                            edges,
                            node_id,
                            get_current_artifact,
                            lambda nid: nodes.get(nid),
                            context.artifact_store,
                        )  # [(inputHandle, artifactId), ...]
                        input_tables = {}  # inputHandle -> DataFrame
                        param_inputs: Dict[str, Any] = {}
                        input_columns: dict[str, list[str]] = {}
                        input_schema_cols_by_handle: dict[str, list[Dict[str, Any]]] = {}
                        input_provenance_by_handle: dict[str, Dict[str, Any]] = {}
                        required_cols_by_artifact: dict[str, list[str]] = {}
                        upstream_source_by_artifact: dict[str, str] = {}
                        upstream_source_handle_by_artifact: dict[str, str] = {}
                        norm = _normalized_params_for_exec_key(kind=kind, node=n, params=params)
                        op = str(norm.get("op") or "").lower()
                        expected_payload_type = (
                            "json" if op == "json_to_table"
                            else "json" if op == "json_filter"
                            else "text" if op == "text_to_table"
                            else str(in_contract or "table")
                        )

                        for e in edges.values():
                            if e.get("target") != node_id:
                                continue
                            src = e.get("source")
                            if not src:
                                continue
                            source_handle = str(e.get("sourceHandle") or "out").strip() or "out"
                            src_artifact_id = get_current_artifact(src, source_handle)
                            if not src_artifact_id:
                                continue
                            upstream_source_by_artifact[src_artifact_id] = str(src)
                            upstream_source_handle_by_artifact[src_artifact_id] = source_handle
                            contract = (e.get("data", {}) or {}).get("contract", {}) or {}
                            payload = contract.get("payload", {}) if isinstance(contract, dict) else {}
                            target_hint = payload.get("target", {}) if isinstance(payload, dict) else {}
                            req_cols = target_hint.get("required_columns") if isinstance(target_hint, dict) else None
                            if isinstance(req_cols, list) and req_cols:
                                required_cols_by_artifact[src_artifact_id] = req_cols

                        for input_handle, upstream_artifact_id in input_refs:
                            art = await context.artifact_store.get(upstream_artifact_id)
                            b = await context.artifact_store.read(upstream_artifact_id)
                            if str(input_handle).startswith("param_"):
                                parsed_param: Any = {}
                                try:
                                    parsed_param = json.loads(bytes(b).decode("utf-8"))
                                except Exception:
                                    parsed_param = {}
                                if isinstance(parsed_param, dict) and isinstance(parsed_param.get("payload"), dict):
                                    parsed_param = parsed_param.get("payload")
                                param_inputs[str(input_handle)] = parsed_param
                                continue
                            ps = getattr(art, "payload_schema", None) or {}
                            ps_type_raw = ps.get("type") if isinstance(ps, dict) else None
                            ps_type = str(ps_type_raw or "").lower()
                            if ps_type == "string":
                                ps_type = "text"
                            if ps_type and ps_type != expected_payload_type:
                                raise ContractMismatchError(
                                    (
                                        "Transform payload schema mismatch: "
                                        f"expected {expected_payload_type} input but got '{ps_type}'"
                                    ),
                                    code="PAYLOAD_SCHEMA_MISMATCH",
                                    details=_contract_details(
                                        expected={"payloadType": expected_payload_type},
                                        actual={
                                            "payloadType": ps_type,
                                            "artifactId": upstream_artifact_id,
                                        },
                                    ),
                                )
                            req_cols = required_cols_by_artifact.get(upstream_artifact_id)
                            if req_cols and expected_payload_type == "table":
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
                            if op == "json_filter":
                                parsed_json: Any = {}
                                try:
                                    parsed_json = json.loads(bytes(b).decode("utf-8"))
                                except Exception:
                                    parsed_json = {}
                                if isinstance(parsed_json, dict) and "payload" in parsed_json:
                                    payload_value = parsed_json.get("payload")
                                    if isinstance(payload_value, (dict, list)):
                                        parsed_json = payload_value
                                if input_handle == "in":
                                    param_inputs["_json_filter_in"] = parsed_json
                                input_tables[input_handle] = load_table_from_json_bytes(
                                    b"[]",
                                    orient="records",
                                    rows_key="rows",
                                )
                                input_columns[input_handle] = []
                                input_schema_cols_by_handle[input_handle] = []
                            elif op == "json_to_table":
                                json_spec = norm.get("json_to_table") if isinstance(norm.get("json_to_table"), dict) else {}
                                json_orient = str(json_spec.get("orient") or "records").strip().lower() or "records"
                                json_rows_key = str(json_spec.get("rowsKey") or "rows").strip() or "rows"
                                df = load_table_from_json_bytes(
                                    b,
                                    orient=json_orient,
                                    rows_key=json_rows_key,
                                )
                            elif op == "text_to_table":
                                text_spec = norm.get("text_to_table") if isinstance(norm.get("text_to_table"), dict) else {}
                                text_mode = str(text_spec.get("mode") or "lines").strip().lower() or "lines"
                                text_column = str(text_spec.get("column") or "text").strip() or "text"
                                text_delimiter = str(text_spec.get("delimiter") or ",")
                                text_has_header = bool(text_spec.get("hasHeader", True))
                                df = load_table_from_text_bytes(
                                    b,
                                    mode=text_mode,
                                    column=text_column,
                                    delimiter=text_delimiter,
                                    has_header=text_has_header,
                                )
                            else:
                                df = load_table_from_artifact_bytes(art.mime_type or "application/octet-stream", b)
                            if op != "json_filter":
                                input_tables[input_handle] = df
                                input_columns[input_handle] = [str(c) for c in list(getattr(df, "columns", []))]
                                schema_cols = _extract_table_columns_from_payload_schema(getattr(art, "payload_schema", None))
                                input_schema_cols_by_handle[input_handle] = (
                                    schema_cols
                                    if schema_cols
                                    else canonical_table_columns(
                                        [{"name": c, "type": "unknown"} for c in input_columns[input_handle]]
                                    )
                                )
                            input_provenance_by_handle[input_handle] = {
                                "sourceKind": "upstream",
                                "upstream": {
                                    "nodeId": upstream_source_by_artifact.get(upstream_artifact_id),
                                    "sourceHandle": upstream_source_handle_by_artifact.get(upstream_artifact_id, input_handle),
                                },
                            }

                        op_preview = op
                        input_schema_summary = [
                            {
                                "inputHandle": input_handle,
                                "artifact": aid,
                                "columns": _compact_typed_columns(input_schema_cols_by_handle.get(input_handle) or []),
                            }
                            for input_handle, aid in input_refs
                        ]
                        input_schema_msg = (
                            f"transform: input-schema op={op_preview or '<none>'} "
                            f"inputs={json.dumps(input_schema_summary, ensure_ascii=False, separators=(',', ':'))}"
                        )
                        await _emit({
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

                        # 3) execute (cache resolve already happened before node execution)
                        op = str(norm.get("op") or "")
                        primary_input_handle = "in" if "in" in input_tables else (next(iter(input_tables.keys())) if input_tables else "in")
                        primary_cols = input_columns.get(primary_input_handle, [])
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
                                available_cols, available_source = _available_columns_for_input_handle(
                                    input_handle=primary_input_handle,
                                    input_schema_cols_by_handle=input_schema_cols_by_handle,
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
                            available_cols, available_source = _available_columns_for_input_handle(
                                input_handle=primary_input_handle,
                                input_schema_cols_by_handle=input_schema_cols_by_handle,
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
                            available_cols, available_source = _available_columns_for_input_handle(
                                input_handle=primary_input_handle,
                                input_schema_cols_by_handle=input_schema_cols_by_handle,
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
                            derive_spec = norm.get("derive") or {}
                            derive_mode = str(derive_spec.get("mode") or "").strip().lower()
                            derive_cols = derive_spec.get("columns") if isinstance(derive_spec.get("columns"), list) else []
                            derive_rules = derive_spec.get("rules") if isinstance(derive_spec.get("rules"), list) else []
                            expected_cols: list[str] = []
                            if derive_mode == "rules":
                                for rule in derive_rules:
                                    if not isinstance(rule, dict):
                                        continue
                                    formula = rule.get("formula")
                                    args = formula.get("args") if isinstance(formula, dict) and isinstance(formula.get("args"), list) else []
                                    for arg in args:
                                        if isinstance(arg, dict) and str(arg.get("column") or "").strip():
                                            expected_cols.append(str(arg.get("column")).strip())
                            else:
                                for d in derive_cols:
                                    if not isinstance(d, dict):
                                        continue
                                    expected_cols.extend(_extract_quoted_identifiers(str(d.get("expr") or "")))
                            expected_cols = sorted(set(expected_cols))
                            if expected_cols:
                                available_cols, available_source = _available_columns_for_input_handle(
                                    input_handle=primary_input_handle,
                                    input_schema_cols_by_handle=input_schema_cols_by_handle,
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
                            available_cols, available_source = _available_columns_for_input_handle(
                                input_handle=primary_input_handle,
                                input_schema_cols_by_handle=input_schema_cols_by_handle,
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
                            clauses_resolved, placeholder_map = _resolve_join_placeholder_node_ids(
                                [c for c in clauses if isinstance(c, dict)],
                                sorted(connected_nodes),
                            )
                            if placeholder_map:
                                norm["join"] = {"clauses": clauses_resolved}
                                clauses = clauses_resolved
                                await _emit({
                                    "type": "log",
                                    "runId": run_id,
                                    "at": iso_now(),
                                    "level": "info",
                                    "message": (
                                        "join: resolved placeholder node ids "
                                        f"{json.dumps(placeholder_map, sort_keys=True)}"
                                    ),
                                    "nodeId": node_id,
                                })
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
                        elif op == "null_policy":
                            null_spec = norm.get("null_policy") or {}
                            cols = [
                                str(c).strip()
                                for c in (null_spec.get("columns") or [])
                                if str(c).strip()
                            ]
                            rules = null_spec.get("rules") if isinstance(null_spec.get("rules"), list) else []
                            for rule in rules:
                                if isinstance(rule, dict):
                                    col = str(rule.get("column") or "").strip()
                                    if col:
                                        cols.append(col)
                            cols = _stable_unique_strings(cols)
                            if cols:
                                available_cols, available_source = _available_columns_for_input_handle(
                                    input_handle=primary_input_handle,
                                    input_schema_cols_by_handle=input_schema_cols_by_handle,
                                    input_columns=input_columns,
                                )
                                available_set = set(available_cols)
                                missing_cols = [c for c in cols if c not in available_set]
                                if missing_cols:
                                    raise ContractMismatchError(
                                        f"Transform payload schema mismatch: null_policy references missing columns {missing_cols}",
                                        code="MISSING_COLUMN",
                                        details=_missing_column_details(
                                            op="null_policy",
                                            param_path="params.null_policy",
                                            missing_columns=missing_cols,
                                            available_columns=available_cols,
                                            available_source=available_source,
                                        ),
                                    )
                        elif op == "outlier_policy":
                            outlier_spec = norm.get("outlier_policy") or {}
                            cols = [
                                str(c).strip()
                                for c in (outlier_spec.get("columns") or [])
                                if str(c).strip()
                            ]
                            if cols:
                                available_cols, available_source = _available_columns_for_input_handle(
                                    input_handle=primary_input_handle,
                                    input_schema_cols_by_handle=input_schema_cols_by_handle,
                                    input_columns=input_columns,
                                )
                                available_set = set(available_cols)
                                missing_cols = [c for c in cols if c not in available_set]
                                if missing_cols:
                                    raise ContractMismatchError(
                                        f"Transform payload schema mismatch: outlier_policy references missing columns {missing_cols}",
                                        code="MISSING_COLUMN",
                                        details=_missing_column_details(
                                            op="outlier_policy",
                                            param_path="params.outlier_policy.columns",
                                            missing_columns=missing_cols,
                                            available_columns=available_cols,
                                            available_source=available_source,
                                        ),
                                    )
                        elif op == "text_clean":
                            clean_spec = norm.get("text_clean") or {}
                            cols = [
                                str(c).strip()
                                for c in (clean_spec.get("columns") or [])
                                if str(c).strip()
                            ]
                            if cols:
                                available_cols, available_source = _available_columns_for_input_handle(
                                    input_handle=primary_input_handle,
                                    input_schema_cols_by_handle=input_schema_cols_by_handle,
                                    input_columns=input_columns,
                                )
                                available_set = set(available_cols)
                                missing_cols = [c for c in cols if c not in available_set]
                                if missing_cols:
                                    raise ContractMismatchError(
                                        f"Transform payload schema mismatch: text_clean references missing columns {missing_cols}",
                                        code="MISSING_COLUMN",
                                        details=_missing_column_details(
                                            op="text_clean",
                                            param_path="params.text_clean.columns",
                                            missing_columns=missing_cols,
                                            available_columns=available_cols,
                                            available_source=available_source,
                                        ),
                                    )
                        elif op == "nlp_normalize":
                            nlp_spec = norm.get("nlp_normalize") or {}
                            language = str(nlp_spec.get("language") or "en").strip().lower()
                            if language != "en":
                                raise ContractMismatchError(
                                    f"Transform config mismatch: unsupported nlp_normalize.language '{language}'",
                                    code="CONTRACT_MISMATCH",
                                    details={
                                        "errorCode": "CONTRACT_MISMATCH",
                                        "op": "nlp_normalize",
                                        "paramPath": "params.nlp_normalize.language",
                                        "expected": {"supportedLanguages": ["en"]},
                                        "actual": {"language": language},
                                    },
                                )
                            cols = [
                                str(c).strip()
                                for c in (nlp_spec.get("columns") or [])
                                if str(c).strip()
                            ]
                            if cols:
                                available_cols, available_source = _available_columns_for_input_handle(
                                    input_handle=primary_input_handle,
                                    input_schema_cols_by_handle=input_schema_cols_by_handle,
                                    input_columns=input_columns,
                                )
                                available_set = set(available_cols)
                                missing_cols = [c for c in cols if c not in available_set]
                                if missing_cols:
                                    raise ContractMismatchError(
                                        f"Transform payload schema mismatch: nlp_normalize references missing columns {missing_cols}",
                                        code="MISSING_COLUMN",
                                        details=_missing_column_details(
                                            op="nlp_normalize",
                                            param_path="params.nlp_normalize.columns",
                                            missing_columns=missing_cols,
                                            available_columns=available_cols,
                                            available_source=available_source,
                                        ),
                                    )
                        elif op == "tokenize_chunk":
                            chunk_spec = norm.get("tokenize_chunk") or {}
                            cols = [
                                str(c).strip()
                                for c in (chunk_spec.get("columns") or [])
                                if str(c).strip()
                            ]
                            if cols:
                                available_cols, available_source = _available_columns_for_input_handle(
                                    input_handle=primary_input_handle,
                                    input_schema_cols_by_handle=input_schema_cols_by_handle,
                                    input_columns=input_columns,
                                )
                                available_set = set(available_cols)
                                missing_cols = [c for c in cols if c not in available_set]
                                if missing_cols:
                                    raise ContractMismatchError(
                                        f"Transform payload schema mismatch: tokenize_chunk references missing columns {missing_cols}",
                                        code="MISSING_COLUMN",
                                        details=_missing_column_details(
                                            op="tokenize_chunk",
                                            param_path="params.tokenize_chunk.columns",
                                            missing_columns=missing_cols,
                                            available_columns=available_cols,
                                            available_source=available_source,
                                        ),
                                    )
                            max_tokens = int(chunk_spec.get("maxTokens") or 256)
                            overlap = int(chunk_spec.get("overlap") or 0)
                            if overlap >= max_tokens:
                                raise ContractMismatchError(
                                    "Transform config mismatch: tokenize_chunk overlap must be less than maxTokens",
                                    code="CONTRACT_MISMATCH",
                                    details={
                                        "errorCode": "CONTRACT_MISMATCH",
                                        "op": "tokenize_chunk",
                                        "paramPath": "params.tokenize_chunk.overlap",
                                        "expected": {"overlapLtMaxTokens": True},
                                        "actual": {"overlap": overlap, "maxTokens": max_tokens},
                                    },
                                )
                        elif op == "dataset_split":
                            split_spec = norm.get("dataset_split") or {}
                            strategy = str(split_spec.get("strategy") or "random").strip().lower()
                            required_col = ""
                            required_path = ""
                            if strategy == "stratified":
                                required_col = str(split_spec.get("stratifyColumn") or "").strip()
                                required_path = "params.dataset_split.stratifyColumn"
                            elif strategy == "group":
                                required_col = str(split_spec.get("groupColumn") or "").strip()
                                required_path = "params.dataset_split.groupColumn"
                            elif strategy == "time":
                                required_col = str(split_spec.get("timeColumn") or "").strip()
                                required_path = "params.dataset_split.timeColumn"
                            if required_col:
                                available_cols, available_source = _available_columns_for_input_handle(
                                    input_handle=primary_input_handle,
                                    input_schema_cols_by_handle=input_schema_cols_by_handle,
                                    input_columns=input_columns,
                                )
                                if required_col not in set(available_cols):
                                    raise ContractMismatchError(
                                        f"Transform payload schema mismatch: dataset_split references missing column '{required_col}'",
                                        code="MISSING_COLUMN",
                                        details=_missing_column_details(
                                            op="dataset_split",
                                            param_path=required_path,
                                            missing_columns=[required_col],
                                            available_columns=available_cols,
                                            available_source=available_source,
                                        ),
                                    )
                        elif op == "class_imbalance":
                            imb_spec = norm.get("class_imbalance") or {}
                            label_col = str(imb_spec.get("labelColumn") or "label").strip()
                            available_cols, available_source = _available_columns_for_input_handle(
                                input_handle=primary_input_handle,
                                input_schema_cols_by_handle=input_schema_cols_by_handle,
                                input_columns=input_columns,
                            )
                            if label_col and label_col not in set(available_cols):
                                raise ContractMismatchError(
                                    f"Transform payload schema mismatch: class_imbalance references missing label column '{label_col}'",
                                    code="MISSING_COLUMN",
                                    details=_missing_column_details(
                                        op="class_imbalance",
                                        param_path="params.class_imbalance.labelColumn",
                                        missing_columns=[label_col],
                                        available_columns=available_cols,
                                        available_source=available_source,
                                    ),
                                )
                        elif op == "categorical_encode":
                            cat_spec = norm.get("categorical_encode") or {}
                            cols = [
                                str(c).strip()
                                for c in (cat_spec.get("columns") or [])
                                if str(c).strip()
                            ]
                            if cols:
                                available_cols, available_source = _available_columns_for_input_handle(
                                    input_handle=primary_input_handle,
                                    input_schema_cols_by_handle=input_schema_cols_by_handle,
                                    input_columns=input_columns,
                                )
                                missing_cols = [c for c in cols if c not in set(available_cols)]
                                if missing_cols:
                                    raise ContractMismatchError(
                                        f"Transform payload schema mismatch: categorical_encode references missing columns {missing_cols}",
                                        code="MISSING_COLUMN",
                                        details=_missing_column_details(
                                            op="categorical_encode",
                                            param_path="params.categorical_encode.columns",
                                            missing_columns=missing_cols,
                                            available_columns=available_cols,
                                            available_source=available_source,
                                        ),
                                    )
                        elif op == "numeric_scale":
                            scale_spec = norm.get("numeric_scale") or {}
                            cols = [
                                str(c).strip()
                                for c in (scale_spec.get("columns") or [])
                                if str(c).strip()
                            ]
                            if cols:
                                available_cols, available_source = _available_columns_for_input_handle(
                                    input_handle=primary_input_handle,
                                    input_schema_cols_by_handle=input_schema_cols_by_handle,
                                    input_columns=input_columns,
                                )
                                missing_cols = [c for c in cols if c not in set(available_cols)]
                                if missing_cols:
                                    raise ContractMismatchError(
                                        f"Transform payload schema mismatch: numeric_scale references missing columns {missing_cols}",
                                        code="MISSING_COLUMN",
                                        details=_missing_column_details(
                                            op="numeric_scale",
                                            param_path="params.numeric_scale.columns",
                                            missing_columns=missing_cols,
                                            available_columns=available_cols,
                                            available_source=available_source,
                                        ),
                                    )
                        elif op == "embedding":
                            emb_spec = norm.get("embedding") or {}
                            cols = [
                                str(c).strip()
                                for c in (emb_spec.get("columns") or [])
                                if str(c).strip()
                            ]
                            if cols:
                                available_cols, available_source = _available_columns_for_input_handle(
                                    input_handle=primary_input_handle,
                                    input_schema_cols_by_handle=input_schema_cols_by_handle,
                                    input_columns=input_columns,
                                )
                                missing_cols = [c for c in cols if c not in set(available_cols)]
                                if missing_cols:
                                    raise ContractMismatchError(
                                        f"Transform payload schema mismatch: embedding references missing columns {missing_cols}",
                                        code="MISSING_COLUMN",
                                        details=_missing_column_details(
                                            op="embedding",
                                            param_path="params.embedding.columns",
                                            missing_columns=missing_cols,
                                            available_columns=available_cols,
                                            available_source=available_source,
                                        ),
                                    )
                        elif op == "feature_selection":
                            fs_spec = norm.get("feature_selection") or {}
                            cols = [
                                str(c).strip()
                                for c in (fs_spec.get("columns") or [])
                                if str(c).strip()
                            ]
                            manual_selected = [
                                str(c).strip()
                                for c in (fs_spec.get("selectedColumns") or [])
                                if str(c).strip()
                            ]
                            referenced = cols + manual_selected
                            if referenced:
                                available_cols, available_source = _available_columns_for_input_handle(
                                    input_handle=primary_input_handle,
                                    input_schema_cols_by_handle=input_schema_cols_by_handle,
                                    input_columns=input_columns,
                                )
                                missing_cols = [c for c in referenced if c not in set(available_cols)]
                                if missing_cols:
                                    raise ContractMismatchError(
                                        f"Transform payload schema mismatch: feature_selection references missing columns {missing_cols}",
                                        code="MISSING_COLUMN",
                                        details=_missing_column_details(
                                            op="feature_selection",
                                            param_path="params.feature_selection",
                                            missing_columns=missing_cols,
                                            available_columns=available_cols,
                                            available_source=available_source,
                                        ),
                                    )
                        elif op == "leakage_detect":
                            leak_spec = norm.get("leakage_detect") or {}
                            split_col = str(leak_spec.get("splitColumn") or "split").strip()
                            referenced = [split_col] + [
                                str(c).strip()
                                for c in (leak_spec.get("keyColumns") or [])
                                if str(c).strip()
                            ]
                            label_col = str(leak_spec.get("labelColumn") or "").strip()
                            if label_col:
                                referenced.append(label_col)
                            available_cols, available_source = _available_columns_for_input_handle(
                                input_handle=primary_input_handle,
                                input_schema_cols_by_handle=input_schema_cols_by_handle,
                                input_columns=input_columns,
                            )
                            missing_cols = [c for c in referenced if c not in set(available_cols)]
                            if missing_cols:
                                raise ContractMismatchError(
                                    f"Transform payload schema mismatch: leakage_detect references missing columns {missing_cols}",
                                    code="MISSING_COLUMN",
                                    details=_missing_column_details(
                                        op="leakage_detect",
                                        param_path="params.leakage_detect",
                                        missing_columns=missing_cols,
                                        available_columns=available_cols,
                                        available_source=available_source,
                                    ),
                                )
                        elif op == "quality_profile":
                            qp_spec = norm.get("quality_profile") or {}
                            cols = [
                                str(c).strip()
                                for c in (qp_spec.get("columns") or [])
                                if str(c).strip()
                            ]
                            if cols:
                                available_cols, available_source = _available_columns_for_input_handle(
                                    input_handle=primary_input_handle,
                                    input_schema_cols_by_handle=input_schema_cols_by_handle,
                                    input_columns=input_columns,
                                )
                                missing_cols = [c for c in cols if c not in set(available_cols)]
                                if missing_cols:
                                    raise ContractMismatchError(
                                        f"Transform payload schema mismatch: quality_profile references missing columns {missing_cols}",
                                        code="MISSING_COLUMN",
                                        details=_missing_column_details(
                                            op="quality_profile",
                                            param_path="params.quality_profile.columns",
                                            missing_columns=missing_cols,
                                            available_columns=available_cols,
                                            available_source=available_source,
                                        ),
                                    )
                        elif op == "drift_compare":
                            dc_spec = norm.get("drift_compare") or {}
                            cols = [
                                str(c).strip()
                                for c in (dc_spec.get("compareColumns") or [])
                                if str(c).strip()
                            ]
                            if cols:
                                available_cols, available_source = _available_columns_for_input_handle(
                                    input_handle=primary_input_handle,
                                    input_schema_cols_by_handle=input_schema_cols_by_handle,
                                    input_columns=input_columns,
                                )
                                missing_cols = [c for c in cols if c not in set(available_cols)]
                                if missing_cols:
                                    raise ContractMismatchError(
                                        f"Transform payload schema mismatch: drift_compare references missing columns {missing_cols}",
                                        code="MISSING_COLUMN",
                                        details=_missing_column_details(
                                            op="drift_compare",
                                            param_path="params.drift_compare.compareColumns",
                                            missing_columns=missing_cols,
                                            available_columns=available_cols,
                                            available_source=available_source,
                                        ),
                                    )
                        elif op == "fit_state_registry":
                            fsr_spec = norm.get("fit_state_registry") or {}
                            cols = [
                                str(c).strip()
                                for c in (fsr_spec.get("includeColumns") or [])
                                if str(c).strip()
                            ]
                            if cols:
                                available_cols, available_source = _available_columns_for_input_handle(
                                    input_handle=primary_input_handle,
                                    input_schema_cols_by_handle=input_schema_cols_by_handle,
                                    input_columns=input_columns,
                                )
                                missing_cols = [c for c in cols if c not in set(available_cols)]
                                if missing_cols:
                                    raise ContractMismatchError(
                                        f"Transform payload schema mismatch: fit_state_registry references missing columns {missing_cols}",
                                        code="MISSING_COLUMN",
                                        details=_missing_column_details(
                                            op="fit_state_registry",
                                            param_path="params.fit_state_registry.includeColumns",
                                            missing_columns=missing_cols,
                                            available_columns=available_cols,
                                            available_source=available_source,
                                        ),
                                    )
                        elif op == "pii_guard":
                            pii_spec = norm.get("pii_guard") or {}
                            cols = [
                                str(c).strip()
                                for c in (pii_spec.get("columns") or [])
                                if str(c).strip()
                            ]
                            if cols:
                                available_cols, available_source = _available_columns_for_input_handle(
                                    input_handle=primary_input_handle,
                                    input_schema_cols_by_handle=input_schema_cols_by_handle,
                                    input_columns=input_columns,
                                )
                                missing_cols = [c for c in cols if c not in set(available_cols)]
                                if missing_cols:
                                    raise ContractMismatchError(
                                        f"Transform payload schema mismatch: pii_guard references missing columns {missing_cols}",
                                        code="MISSING_COLUMN",
                                        details=_missing_column_details(
                                            op="pii_guard",
                                            param_path="params.pii_guard.columns",
                                            missing_columns=missing_cols,
                                            available_columns=available_cols,
                                            available_source=available_source,
                                        ),
                                    )
                        elif op == "ml_contract":
                            contract_spec = norm.get("ml_contract") or {}
                            label_col = str(contract_spec.get("labelColumn") or "").strip()
                            feature_cols = _stable_unique_strings(
                                [
                                    str(c).strip()
                                    for c in (contract_spec.get("featureColumns") or [])
                                    if str(c).strip()
                                ]
                            )
                            optional_cols = []
                            id_col = str(contract_spec.get("idColumn") or "").strip()
                            ts_col = str(contract_spec.get("timestampColumn") or "").strip()
                            if id_col:
                                optional_cols.append(id_col)
                            if ts_col:
                                optional_cols.append(ts_col)
                            required_cols = _stable_unique_strings(
                                [label_col, *feature_cols, *optional_cols]
                            )
                            available_cols, available_source = _available_columns_for_input_handle(
                                input_handle=primary_input_handle,
                                input_schema_cols_by_handle=input_schema_cols_by_handle,
                                input_columns=input_columns,
                            )
                            available_set = set(available_cols)
                            missing_cols = [c for c in required_cols if c and c not in available_set]
                            if missing_cols:
                                raise ContractMismatchError(
                                    f"Transform payload schema mismatch: ml_contract references missing columns {missing_cols}",
                                    code="MISSING_COLUMN",
                                    details=_missing_column_details(
                                        op="ml_contract",
                                        param_path="params.ml_contract",
                                        missing_columns=missing_cols,
                                        available_columns=available_cols,
                                        available_source=available_source,
                                    ),
                                )
                            require_non_null_label = bool(contract_spec.get("requireNonNullLabel", True))
                            if require_non_null_label and label_col:
                                primary_df = input_tables.get(primary_input_handle)
                                if primary_df is None and input_tables:
                                    primary_df = next(iter(input_tables.values()))
                                if primary_df is not None and label_col in list(getattr(primary_df, "columns", [])):
                                    series = primary_df[label_col]
                                    null_like = int(series.isna().sum())
                                    if null_like > 0:
                                        raise ContractMismatchError(
                                            f"Transform payload schema mismatch: ml_contract label column '{label_col}' has null values",
                                            code="CONTRACT_MISMATCH",
                                            details={
                                                "errorCode": "CONTRACT_MISMATCH",
                                                "op": "ml_contract",
                                                "paramPath": "params.ml_contract.requireNonNullLabel",
                                                "expected": {"requireNonNullLabel": True, "column": label_col},
                                                "actual": {"nullCount": int(null_like), "column": label_col},
                                            },
                                        )
                        elif op == "dedupe":
                            dedupe_spec = norm.get("dedupe") or {}
                            by_cols = [str(c) for c in (dedupe_spec.get("by") or []) if str(c).strip()]
                            all_columns = bool(dedupe_spec.get("allColumns", len(by_cols) == 0))
                            available_cols, available_source = _available_columns_for_input_handle(
                                input_handle=primary_input_handle,
                                input_schema_cols_by_handle=input_schema_cols_by_handle,
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
                            available_cols, available_source = _available_columns_for_input_handle(
                                input_handle=primary_input_handle,
                                input_schema_cols_by_handle=input_schema_cols_by_handle,
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
                        elif op == "quality_gate":
                            gate_spec = norm.get("quality_gate") or {}
                            checks = gate_spec.get("checks") if isinstance(gate_spec.get("checks"), list) else []
                            available_cols, available_source = _available_columns_for_input_handle(
                                input_handle=primary_input_handle,
                                input_schema_cols_by_handle=input_schema_cols_by_handle,
                                input_columns=input_columns,
                            )
                            available_set = set(available_cols)
                            for idx, check in enumerate(checks):
                                if not isinstance(check, dict):
                                    continue
                                kind = str(check.get("kind") or "").strip().lower()
                                referenced: list[str] = []
                                if kind in {"null_pct", "range", "uniqueness", "class_balance"}:
                                    col = str(check.get("column") or "").strip()
                                    if col:
                                        referenced.append(col)
                                elif kind == "leakage":
                                    feature_col = str(check.get("featureColumn") or "").strip()
                                    target_col = str(check.get("targetColumn") or "").strip()
                                    if feature_col:
                                        referenced.append(feature_col)
                                    if target_col:
                                        referenced.append(target_col)
                                missing_cols = [c for c in referenced if c not in available_set]
                                if missing_cols:
                                    raise ContractMismatchError(
                                        f"Transform payload schema mismatch: quality_gate references missing columns {missing_cols}",
                                        code="MISSING_COLUMN",
                                        details=_missing_column_details(
                                            op="quality_gate",
                                            param_path=f"params.quality_gate.checks.{idx}",
                                            missing_columns=missing_cols,
                                            available_columns=available_cols,
                                            available_source=available_source,
                                        ),
                                    )

                        try:
                            res = run_transform(
                                params=norm,
                                input_tables=input_tables,
                                join_lookup=join_lookup,
                                param_inputs=param_inputs,
                            )
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

                        await _emit({
                            "type": "log",
                            "runId": run_id,
                            "at": iso_now(),
                            "level": "info",
                            "message": f"transform: produced {len(res.payload_bytes)} bytes, content_hash={res.meta.get('content_hash')}",
                            "nodeId": node_id,
                        })
                        filter_compile_meta = (
                            res.meta.get("filter_compile")
                            if isinstance(res.meta, dict) and isinstance(res.meta.get("filter_compile"), dict)
                            else None
                        )
                        if isinstance(filter_compile_meta, dict):
                            await _emit(
                                {
                                    "type": "log",
                                    "runId": run_id,
                                    "at": iso_now(),
                                    "level": "info",
                                    "message": (
                                        "transform: filter-compile "
                                        + json.dumps(filter_compile_meta, ensure_ascii=False, separators=(",", ":"))
                                    ),
                                    "nodeId": node_id,
                                }
                            )
                        json_filter_compile_meta = (
                            res.meta.get("json_filter_compile")
                            if isinstance(res.meta, dict) and isinstance(res.meta.get("json_filter_compile"), dict)
                            else None
                        )
                        if isinstance(json_filter_compile_meta, dict):
                            await _emit(
                                {
                                    "type": "log",
                                    "runId": run_id,
                                    "at": iso_now(),
                                    "level": "info",
                                    "message": (
                                        "transform: json-filter-compile "
                                        + json.dumps(json_filter_compile_meta, ensure_ascii=False, separators=(",", ":"))
                                    ),
                                    "nodeId": node_id,
                                }
                            )
                        derive_compile_meta = (
                            res.meta.get("derive_compile")
                            if isinstance(res.meta, dict) and isinstance(res.meta.get("derive_compile"), dict)
                            else None
                        )
                        if isinstance(derive_compile_meta, dict):
                            await _emit(
                                {
                                    "type": "log",
                                    "runId": run_id,
                                    "at": iso_now(),
                                    "level": "info",
                                    "message": (
                                        "transform: derive-compile "
                                        + json.dumps(derive_compile_meta, ensure_ascii=False, separators=(",", ":"))
                                    ),
                                    "nodeId": node_id,
                                }
                            )
                        quality_gate_meta = (res.meta.get("quality_gate") if isinstance(res.meta, dict) else None)
                        warn_violations = (
                            quality_gate_meta.get("warnViolations")
                            if isinstance(quality_gate_meta, dict) and isinstance(quality_gate_meta.get("warnViolations"), list)
                            else []
                        )
                        if warn_violations:
                            await _emit({
                                "type": "log",
                                "runId": run_id,
                                "at": iso_now(),
                                "level": "warn",
                                "message": json.dumps(
                                    {
                                        "op": "quality_gate",
                                        "warnViolations": warn_violations,
                                        "checksEvaluated": quality_gate_meta.get("checksEvaluated"),
                                    },
                                    ensure_ascii=False,
                                    separators=(",", ":"),
                                ),
                                "nodeId": node_id,
                            })

                        # 5) store artifact bytes + cache
                        artifact_id = exec_key  # keep your convention

                        created_at_dt = datetime.now(timezone.utc)
                        transform_payload_type = str(
                            (
                                (res.meta.get("payloadType") if isinstance(res.meta, dict) else None)
                                or "table"
                            )
                        ).strip().lower() or "table"
                        table_schema_env: Optional[Dict[str, Any]] = None
                        schema_for_metadata: Optional[Dict[str, Any]] = None

                        if transform_payload_type == "table":
                            primary_cols_for_schema = input_columns.get(primary_input_handle, [])
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
                            output_cols = runtime_cols if runtime_cols else output_cols_core

                            primary_type_by_name: dict[str, str] = {
                                str(c.get("name") or ""): str(c.get("type") or "unknown")
                                for c in (input_schema_cols_by_handle.get(primary_input_handle) or [])
                                if isinstance(c, dict) and str(c.get("name") or "").strip()
                            }
                            all_name_counts: dict[str, int] = {}
                            all_name_type: dict[str, str] = {}
                            for cols in input_schema_cols_by_handle.values():
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
                            passthrough_ops = {
                                "filter",
                                "sort",
                                "limit",
                                "dedupe",
                                "null_policy",
                                "outlier_policy",
                                "text_clean",
                                "nlp_normalize",
                                "dataset_split",
                                "class_imbalance",
                                "categorical_encode",
                                "numeric_scale",
                                "embedding",
                                "feature_selection",
                                "leakage_detect",
                                "quality_profile",
                                "drift_compare",
                                "determinism_profile",
                                "fit_state_registry",
                                "pii_guard",
                                "inference_parity",
                                "quality_gate",
                                "ml_contract",
                                "sql",
                                "select",
                            }
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
                                    if next_type == "unknown" and all_name_counts.get(col_name, 0) == 1:
                                        next_type = all_name_type.get(col_name, "unknown")
                                enriched_cols.append({"name": col_name, "type": next_type})
                            output_cols = canonical_table_columns(enriched_cols if enriched_cols else output_cols)
                            upstream_refs = [
                                {
                                    "nodeId": input_provenance_by_handle.get(input_handle, {})
                                    .get("upstream", {})
                                    .get("nodeId"),
                                    "sourceHandle": input_provenance_by_handle.get(input_handle, {})
                                    .get("upstream", {})
                                    .get("sourceHandle", input_handle),
                                }
                                for input_handle, _ in input_refs
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
                            await _emit({
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
                            schema_for_metadata = table_schema_env
                        else:
                            decoded = res.payload_bytes.decode("utf-8", errors="replace")
                            try:
                                parsed = json.loads(decoded)
                            except Exception:
                                parsed = decoded
                            json_schema = _json_payload_value_schema(parsed)
                            base_payload_schema = {
                                "schema_version": 1,
                                "type": "json",
                                "schema": json_schema,
                            }
                            # Keep metadata schema acyclic. Pointing this at base_payload_schema would
                            # later create payload_schema -> artifactMetadataV1 -> schema -> payload_schema.
                            schema_for_metadata = json_schema if isinstance(json_schema, dict) else None
                            await _emit({
                                "type": "log",
                                "runId": run_id,
                                "at": iso_now(),
                                "level": "info",
                                "message": f"transform: output-schema op={op} type={transform_payload_type}",
                                "nodeId": node_id,
                            })
                        schema_fp = str((expected_schema or {}).get("schemaFingerprint") or "")
                        if not schema_fp:
                            schema_fp = contract_schema_fingerprint(
                                canonical_schema_for_contract(default_contract_for_node(n))
                            )
                        contract_fingerprint = schema_fp
                        lineage_v1 = await _artifact_lineage_v1(
                            artifact_id=artifact_id,
                            upstream_artifact_ids=sorted([aid for _, aid in input_refs]),
                            node_params=params if isinstance(params, dict) else {},
                            node_id=node_id,
                            run_id=run_id,
                            graph_id=context.graph_id,
                            exec_key=exec_key,
                            artifact_store=context.artifact_store,
                        )
                        base_payload_schema["artifactMetadataV1"] = _artifact_metadata_v1(
                            exec_key=exec_key,
                            node_id=node_id,
                            node_type=kind,
                            node_impl_version=node_impl_version,
                            params_fingerprint=node_state_hash,
                            upstream_artifact_ids=sorted([aid for _, aid in input_refs]),
                            contract_fingerprint=contract_fingerprint,
                            schema_fingerprint=schema_fp,
                            mime_type=res.mime_type,
                            payload_type=transform_payload_type,
                            schema=schema_for_metadata,
                            created_at_iso=created_at_dt.isoformat(),
                            run_id=run_id,
                            graph_id=context.graph_id,
                            determinism_fingerprint=_determinism_fingerprint(determinism_env),
                            code_hash=_determinism_code_hash_from_env(determinism_env),
                            profile_lock=_determinism_profile_lock_from_env(determinism_env),
                            component_context=(
                                (n.get("data", {}) or {}).get("_componentContext")
                                if isinstance((n.get("data", {}) or {}).get("_componentContext"), dict)
                                else None
                            ),
                            lineage_v1=lineage_v1,
                        )
                        artifact = Artifact(
                            artifact_id=artifact_id,
                            node_kind=kind,
                            params_hash=node_state_hash,
                            upstream_ids=sorted([aid for _, aid in input_refs]),
                            created_at=created_at_dt,
                            execution_version=context.execution_version,
                            mime_type=res.mime_type,
                            payload_type=transform_payload_type,
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
                        _assert_binding_ready_for_commit(
                            node_id=node_id,
                            snapshot=binding_snapshot,
                            commit_artifact_id=committed_artifact_id,
                            phase="post_write_bind_transform",
                        )
                        context.bindings.bind(node_id=node_id, artifact_id=committed_artifact_id, status="computed")

                        additional_outputs = (
                            res.additional_outputs if isinstance(getattr(res, "additional_outputs", None), dict) else {}
                        )
                        for output_handle, output_payload in additional_outputs.items():
                            handle = str(output_handle or "").strip()
                            if not handle or handle == "out":
                                continue
                            output_bytes = bytes(getattr(output_payload, "payload_bytes", b"") or b"")
                            output_mime = str(getattr(output_payload, "mime_type", "") or "").strip() or "application/octet-stream"
                            output_meta = (
                                getattr(output_payload, "meta", {})
                                if isinstance(getattr(output_payload, "meta", {}), dict)
                                else {}
                            )
                            output_payload_type = str(output_meta.get("payloadType") or "").strip().lower()
                            if output_payload_type not in {"table", "json", "text", "binary", "embeddings", "image", "audio", "video"}:
                                output_payload_type = "json" if "json" in output_mime.lower() else "binary"

                            output_artifact_id = f"{exec_key}::{handle}"
                            output_schema_for_metadata: Optional[Dict[str, Any]] = None
                            if output_payload_type == "table":
                                output_columns = canonical_table_columns(
                                    [{"name": str(c), "type": "unknown"} for c in (output_meta.get("columns") or [])]
                                )
                                output_table_schema = _table_schema_envelope(
                                    columns=output_columns,
                                    row_count=(output_meta.get("row_count") if isinstance(output_meta, dict) else None),
                                    provenance={
                                        "sourceKind": "transform",
                                        "upstream": [
                                            {
                                                "nodeId": input_provenance_by_handle.get(input_handle, {})
                                                .get("upstream", {})
                                                .get("nodeId"),
                                                "sourceHandle": input_provenance_by_handle.get(input_handle, {})
                                                .get("upstream", {})
                                                .get("sourceHandle", input_handle),
                                            }
                                            for input_handle, _ in input_refs
                                        ],
                                    },
                                )
                                output_base_payload_schema = {
                                    "schema_version": 1,
                                    "type": "table",
                                    "columns": output_columns,
                                    "schema": output_table_schema,
                                }
                                output_schema_for_metadata = output_table_schema
                            elif output_payload_type == "json":
                                output_text = output_bytes.decode("utf-8", errors="replace")
                                try:
                                    output_value = json.loads(output_text)
                                except Exception:
                                    output_value = output_text
                                output_json_schema = _json_payload_value_schema(output_value)
                                output_base_payload_schema = {
                                    "schema_version": 1,
                                    "type": "json",
                                    "schema": output_json_schema,
                                }
                                output_schema_for_metadata = (
                                    output_json_schema if isinstance(output_json_schema, dict) else None
                                )
                            else:
                                output_base_payload_schema = {"schema_version": 1, "type": output_payload_type}
                                output_schema_for_metadata = (
                                    output_base_payload_schema
                                    if isinstance(output_base_payload_schema, dict)
                                    else None
                                )

                            output_schema_fp = _schema_fp_for_artifact(
                                payload_schema=output_base_payload_schema,
                                observed_typed_schema=None,
                                expected_typed_schema=None,
                            )
                            output_lineage_v1 = await _artifact_lineage_v1(
                                artifact_id=output_artifact_id,
                                upstream_artifact_ids=sorted([aid for _, aid in input_refs]),
                                node_params=norm if isinstance(norm, dict) else {},
                                node_id=node_id,
                                run_id=run_id,
                                graph_id=context.graph_id,
                                exec_key=f"{exec_key}::{handle}",
                                artifact_store=context.artifact_store,
                            )
                            output_base_payload_schema["artifactMetadataV1"] = _artifact_metadata_v1(
                                exec_key=f"{exec_key}::{handle}",
                                node_id=node_id,
                                node_type=kind,
                                node_impl_version=node_impl_version,
                                params_fingerprint=node_state_hash,
                                upstream_artifact_ids=sorted([aid for _, aid in input_refs]),
                                contract_fingerprint=contract_fingerprint,
                                schema_fingerprint=output_schema_fp,
                                mime_type=output_mime,
                                payload_type=output_payload_type,
                                schema=output_schema_for_metadata,
                                created_at_iso=created_at_dt.isoformat(),
                                run_id=run_id,
                                graph_id=context.graph_id,
                                determinism_fingerprint=_determinism_fingerprint(determinism_env),
                                code_hash=_determinism_code_hash_from_env(determinism_env),
                                profile_lock=_determinism_profile_lock_from_env(determinism_env),
                                component_context=(
                                    (n.get("data", {}) or {}).get("_componentContext")
                                    if isinstance((n.get("data", {}) or {}).get("_componentContext"), dict)
                                    else None
                                ),
                                lineage_v1=output_lineage_v1,
                            )
                            output_artifact = Artifact(
                                artifact_id=output_artifact_id,
                                node_kind=kind,
                                params_hash=node_state_hash,
                                upstream_ids=sorted([aid for _, aid in input_refs]),
                                created_at=created_at_dt,
                                execution_version=context.execution_version,
                                mime_type=output_mime,
                                payload_type=output_payload_type,
                                size_bytes=len(output_bytes),
                                storage_uri=f"artifact://{output_artifact_id}",
                                payload_schema=output_base_payload_schema,
                                run_id=run_id,
                                graph_id=context.graph_id,
                                node_id=node_id,
                                exec_key=f"{exec_key}::{handle}",
                            )
                            committed_output_artifact_id = await context.artifact_store.write(output_artifact, output_bytes)
                            await _record_consumers(
                                context=context,
                                input_artifact_ids=[aid for _, aid in input_refs],
                                consumer_run_id=run_id,
                                consumer_node_id=node_id,
                                consumer_exec_key=f"{exec_key}::{handle}",
                                output_artifact_id=committed_output_artifact_id,
                            )
                            context.bindings.bind(
                                node_id=node_id,
                                handle=handle,
                                artifact_id=committed_output_artifact_id,
                                status="computed",
                            )
                            await _emit(
                                {
                                    "type": "node_output",
                                    "runId": run_id,
                                    "nodeId": node_id,
                                    "at": iso_now(),
                                    "artifactId": committed_output_artifact_id,
                                    "mimeType": output_mime,
                                    "payloadType": output_payload_type,
                                    "handle": handle,
                                    "sourceObservability": _source_observability_from_artifact(output_artifact),
                                    "primingArtifact": _source_priming_artifact_from_artifact(output_artifact),
                                }
                            )

                        # cache index
                        await cache.store_artifact_id(exec_key, committed_artifact_id)

                        print(f"[artifact] transform node={node_id} bytes={len(res.payload_bytes)} id={artifact_id[:10]}...")

                        # emit node_output (UI fetches by artifactId)
                        await _emit({
                            "type": "node_output",
                            "runId": run_id,
                            "nodeId": node_id,
                            "at": iso_now(),
                            "artifactId": committed_artifact_id,
                            "mimeType": res.mime_type,
                            "payloadType": transform_payload_type,
                            "sourceObservability": _source_observability_from_artifact(artifact),
                            "primingArtifact": _source_priming_artifact_from_artifact(artifact),
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
                elif kind in {"llm", "model"}:
                    # Canonical upstream artifact list for model input serialization.
                    # Only work handles are serialized into {input}; param/control handles
                    # are for side-band configuration and must not be fed to json/table encoders.
                    llm_work_input_pairs = _work_input_pairs(input_refs) if input_refs else []
                    llm_upstream_ids = (
                        [aid for _, aid in llm_work_input_pairs]
                        if llm_work_input_pairs
                        else ([] if input_refs else upstream_ids)
                    )

                    llm_input_pairs = (
                        llm_work_input_pairs
                        if llm_work_input_pairs
                        else [("in", aid) for aid in llm_upstream_ids]
                    )
                    for input_port_name, upstream_id in llm_input_pairs:
                        if _is_non_work_input_handle(input_port_name):
                            # Param/control links are validated through affinity + param-shape contracts.
                            # Do not enforce work payload contracts on these handles.
                            continue
                        upstream_art = await context.artifact_store.get(upstream_id)
                        upstream_pt = _infer_artifact_payload_type(upstream_art)
                        llm_in_contract = str((_declared_in_port(kind, n, input_port=input_port_name) or "text"))

                        if upstream_pt != llm_in_contract:
                            if strict_schema_edge_checks:
                                raise ContractMismatchError(
                                    "LLM input contract mismatch: upstream artifact payload type does not match expected input type",
                                    code="LLM_INPUT_PORT_MISMATCH",
                                    details=_contract_details(
                                        expected={"inputType": llm_in_contract, "inputPort": input_port_name},
                                        actual={"artifactId": upstream_id, "actualType": upstream_pt, "inputPort": input_port_name},
                                    ),
                                )
                            await _emit({
                                "type": "log",
                                "runId": run_id,
                                "at": iso_now(),
                                "level": "warn",
                                "message": (
                                    f"[COERCION_APPLIED] edge {upstream_id}->{node_id}:{input_port_name} "
                                    f"payload type {upstream_pt} coerced to {llm_in_contract} "
                                    "(STRICT_SCHEMA_EDGE_CHECKS=off)"
                                ),
                                "nodeId": node_id,
                            })

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
                    tool_input_pairs = (
                        [(str(port or "in"), aid) for port, aid in input_refs]
                        if input_refs
                        else [("in", aid) for aid in tool_upstream_ids]
                    )
                    for input_port_name, upstream_id in tool_input_pairs:
                        tool_in_contract = _declared_in_port("tool", n, input_port=input_port_name)
                        if not tool_in_contract:
                            continue
                        if input_port_name.startswith("param") or input_port_name.startswith("control") or input_port_name.startswith("ctl"):
                            # Non-work handles are validated separately by affinity/shape contracts.
                            continue
                        upstream_art = await context.artifact_store.get(upstream_id)
                        upstream_pt = _infer_artifact_payload_type(upstream_art)
                        if upstream_pt != tool_in_contract:
                            if strict_schema_edge_checks:
                                raise ContractMismatchError(
                                    "Tool input contract mismatch: upstream artifact payload type does not match expected input type",
                                    code="TOOL_INPUT_PORT_MISMATCH",
                                    details=_contract_details(
                                        expected={"inputType": tool_in_contract, "inputPort": input_port_name},
                                        actual={"artifactId": upstream_id, "actualType": upstream_pt, "inputPort": input_port_name},
                                    ),
                                )
                            await _emit({
                                "type": "log",
                                "runId": run_id,
                                "at": iso_now(),
                                "level": "warn",
                                "message": (
                                    f"[COERCION_APPLIED] edge {upstream_id}->{node_id}:{input_port_name} "
                                    f"payload type {upstream_pt} coerced to {tool_in_contract} "
                                    "(STRICT_SCHEMA_EDGE_CHECKS=off)"
                                ),
                                "nodeId": node_id,
                            })

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
                elif kind == "component":
                    component_ref = params.get("componentRef") if isinstance(params.get("componentRef"), dict) else {}
                    component_api = params.get("api") if isinstance(params.get("api"), dict) else {}
                    declared_outputs = (
                        component_api.get("outputs")
                        if isinstance(component_api.get("outputs"), list)
                        else []
                    )
                    bindings = params.get("bindings") if isinstance(params.get("bindings"), dict) else {}
                    output_bindings = (
                        bindings.get("outputs")
                        if isinstance(bindings.get("outputs"), dict)
                        else {}
                    )
                    refs_by_handle: Dict[str, list[str]] = {}
                    for input_handle, aid in input_refs:
                        handle = str(input_handle or "").strip()
                        if not handle:
                            continue
                        refs_by_handle.setdefault(handle, []).append(str(aid))

                    wrapper_outputs: Dict[str, Any] = {}
                    wrapper_upstream_ids: list[str] = []
                    for out_decl in declared_outputs:
                        if not isinstance(out_decl, dict):
                            continue
                        out_name = str(out_decl.get("name") or "").strip()
                        if not out_name:
                            continue
                        binding = output_bindings.get(out_name) if isinstance(output_bindings, dict) else None
                        if not isinstance(binding, dict):
                            raise ContractMismatchError(
                                f"Component output binding missing for '{out_name}'",
                                code="COMPONENT_OUTPUT_BINDING_MISSING",
                                details=_contract_details(
                                    expected={"output": out_name, "binding": "required"},
                                    actual={"binding": None},
                                ),
                            )
                        mode = str(binding.get("artifact") or "current").strip().lower() or "current"
                        if mode not in {"current", "last"}:
                            raise ContractMismatchError(
                                f"Component output binding for '{out_name}' has unsupported artifact mode '{mode}'",
                                code="COMPONENT_OUTPUT_BINDING_INVALID",
                                details=_contract_details(
                                    expected={"output": out_name, "artifactMode": ["current", "last"]},
                                    actual={"artifactMode": mode},
                                ),
                            )
                        candidates = refs_by_handle.get(out_name, [])
                        if not candidates:
                            raise ContractMismatchError(
                                f"Component output '{out_name}' not resolved. Ensure bindings.outputs.{out_name}.outputRef exists and produced an artifact.",
                                code="COMPONENT_OUTPUT_NOT_RESOLVED",
                                details=_contract_details(
                                    expected={"output": out_name, "resolvedArtifact": True},
                                    actual={"resolvedArtifact": False},
                                ),
                            )
                        current_artifact_id = str(candidates[0] or "").strip()
                        bound_artifact_id = current_artifact_id
                        if mode == "last":
                            output_edge_resolution = _resolve_component_output_artifact_from_output_edges(
                                edges=edges,
                                component_instance_node_id=str(node_id),
                                output_name=out_name,
                                get_current_artifact=get_current_artifact,
                            )
                            bound_runtime_node_id = str(output_edge_resolution.get("runtimeNodeId") or "").strip()
                            if not bound_runtime_node_id:
                                raise ContractMismatchError(
                                    f"Component output binding for '{out_name}' requires a resolvable outputRef when artifact='last'",
                                    code="COMPONENT_OUTPUT_BINDING_INVALID",
                                    details=_contract_details(
                                        expected={"output": out_name, "outputRef": "resolvable when artifact='last'"},
                                        actual={"runtimeNodeId": ""},
                                    ),
                                )
                            latest_lookup = getattr(context.artifact_store, "get_latest_node_artifact", None)
                            if not callable(latest_lookup):
                                raise ContractMismatchError(
                                    "Component output binding artifact='last' requires artifact store support",
                                    code="COMPONENT_OUTPUT_BINDING_UNSUPPORTED",
                                    details=_contract_details(
                                        expected={"artifactStoreCapability": "get_latest_node_artifact"},
                                        actual={"artifactStoreCapability": "missing"},
                                    ),
                                )
                            last_artifact_id = await latest_lookup(
                                graph_id=graph_id,
                                node_id=bound_runtime_node_id,
                                exclude_artifact_id=current_artifact_id or None,
                            )
                            bound_artifact_id = str(last_artifact_id or "").strip()
                            if not bound_artifact_id:
                                raise ContractMismatchError(
                                    f"Component output '{out_name}' requested artifact='last' but no previous artifact exists for the bound outputRef",
                                    code="COMPONENT_OUTPUT_LAST_NOT_FOUND",
                                    details=_contract_details(
                                        expected={"output": out_name, "artifact": "last"},
                                        actual={"runtimeNodeId": bound_runtime_node_id, "artifactFound": False},
                                    ),
                                )
                        if not bound_artifact_id:
                            raise ContractMismatchError(
                                f"Component output '{out_name}' resolved to empty artifact id",
                                code="COMPONENT_OUTPUT_INVALID_ARTIFACT_ID",
                                details=_contract_details(
                                    expected={"output": out_name, "artifactId": "non-empty"},
                                    actual={"artifactId": ""},
                                ),
                            )
                        bound_artifact = await context.artifact_store.get(bound_artifact_id)
                        actual_payload_type = str(_infer_artifact_payload_type(bound_artifact) or "json").strip().lower() or "json"
                        wrapper_typed = await _component_wrapper_output_typed_schema(
                            artifact_store=context.artifact_store,
                            artifact_id=bound_artifact_id,
                            output_name=out_name,
                        )
                        if wrapper_typed is not None:
                            wrapper_payload_type = _typed_schema_type_to_payload_type(wrapper_typed)
                            if wrapper_payload_type != "unknown":
                                actual_payload_type = wrapper_payload_type
                        coercion_policy = (
                            _coercion_policy_for_node(n) if strict_coercion_policy else "allow_lossy"
                        )
                        declared_typed = (
                            _normalize_typed_schema_for_runtime(out_decl.get("typedSchema"))
                            if isinstance(out_decl.get("typedSchema"), dict)
                            else None
                        )
                        actual_typed = wrapper_typed if wrapper_typed is not None else _artifact_typed_schema(bound_artifact)
                        ts_ok, ts_info = _typed_schema_compatibility(
                            expected=declared_typed,
                            actual=actual_typed,
                            policy=coercion_policy,
                        )
                        if strict_schema_edge_checks and (not ts_ok):
                            raise ContractMismatchError(
                                f"Component output '{out_name}' typed schema mismatch",
                                code="COMPONENT_OUTPUT_TYPED_SCHEMA_MISMATCH",
                                details=_contract_details(
                                    missing_columns=ts_info.get("missingColumns") if isinstance(ts_info, dict) else [],
                                    expected={"output": out_name, "typedSchema": declared_typed or {}, "coercionPolicy": coercion_policy},
                                    actual={
                                        "artifactId": bound_artifact_id,
                                        "typedSchema": actual_typed or {},
                                        "mismatchedColumns": (
                                            ts_info.get("mismatchedColumns")
                                            if isinstance(ts_info, dict)
                                            else []
                                        ),
                                    },
                                ),
                            )
                        wrapper_upstream_ids.append(bound_artifact_id)
                        wrapper_outputs[out_name] = {
                            "artifactId": bound_artifact_id,
                            "mimeType": str(getattr(bound_artifact, "mime_type", "") or "").strip() or "application/octet-stream",
                            "payloadType": actual_payload_type,
                            "typedSchema": declared_typed,
                            "typedSchemaExpected": declared_typed,
                            "typedSchemaObserved": actual_typed,
                            "required": bool(out_decl.get("required", True)),
                        }
                    for out_decl in declared_outputs:
                        if not isinstance(out_decl, dict):
                            continue
                        out_name = str(out_decl.get("name") or "").strip()
                        if not out_name:
                            continue
                        if bool(out_decl.get("required", True)) and out_name not in wrapper_outputs:
                            raise ContractMismatchError(
                                f"Required component output '{out_name}' was not produced",
                                code="COMPONENT_OUTPUT_REQUIRED_MISSING",
                                details=_contract_details(
                                    expected={"output": out_name, "required": True},
                                    actual={"produced": False},
                                ),
                            )

                    output = NodeOutput(
                        status="succeeded",
                        data={
                            "ok": True,
                            "component": {
                                "componentId": str(component_ref.get("componentId") or ""),
                                "revisionId": str(component_ref.get("revisionId") or ""),
                                "apiVersion": str(component_ref.get("apiVersion") or "v1"),
                                "instanceNodeId": node_id,
                            },
                            "outputs": wrapper_outputs,
                            "meta": {
                                "mimeType": "application/json",
                                "payloadType": "json",
                                "inputRefs": [{"inputHandle": h, "artifactId": a} for h, a in input_refs],
                                "upstreamArtifactIds": sorted(set(wrapper_upstream_ids)),
                            },
                        },
                        metadata=None,
                        execution_time_ms=0.0,
                    )
                else:
                    raise RuntimeError(f"Unknown node kind: {kind}")

                # Validate output
                if output.status == "failed":
                    meta_obj = getattr(output, "metadata", None)
                    data_obj = getattr(output, "data", None)
                    reject_flag = False
                    if isinstance(meta_obj, dict):
                        reject_flag = bool(meta_obj.get("reject")) or str(meta_obj.get("decision") or "").strip().lower() == "reject"
                    if isinstance(data_obj, dict):
                        payload_obj = data_obj.get("payload") if isinstance(data_obj.get("payload"), dict) else {}
                        meta_payload = data_obj.get("meta") if isinstance(data_obj.get("meta"), dict) else {}
                        reject_flag = reject_flag or bool(payload_obj.get("reject")) or bool(meta_payload.get("reject"))
                    if reject_flag:
                        decision_value = "reject"
                        decision_reason_code = "NODE_REJECTED_NON_ERROR"
                        await _emit(
                            {
                                "type": "log",
                                "runId": run_id,
                                "at": iso_now(),
                                "level": "info",
                                "message": "Node rejected input (non-error); continuing run",
                                "nodeId": node_id,
                            }
                        )
                        output = NodeOutput(
                            status="succeeded",
                            data={"kind": "json", "payload": {"rejected": True}, "meta": {"decision": "reject"}},
                            metadata=None,
                            execution_time_ms=max(0.0, float(getattr(output, "execution_time_ms", 0.0) or 0.0)),
                        )
                    else:
                        raise RuntimeError(output.error or "Node execution failed")

                # Store output for legacy flow / UI
                context.outputs[node_id] = output

                if kind == "transform":
                    await _emit({
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
                        out_contract = str(
                            (_declared_out_port("source", n))
                            or "text"
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
                        elif out_contract in {"binary", "image", "audio", "video"}:
                            if not isinstance(data_value, (bytes, bytearray)):
                                raise RuntimeError(
                                    f"Source output contract mismatch: out={out_contract} expects bytes, got {type(data_value)}"
                                )
                            payload_bytes = bytes(data_value)
                            source_meta = getattr(output, "metadata", None)
                            source_meta_mime = (
                                str(getattr(source_meta, "mime_type", "")).strip()
                                if source_meta is not None
                                else ""
                            )
                            if out_contract == "image":
                                mime_type = source_meta_mime or "image/png"
                            elif out_contract == "audio":
                                mime_type = source_meta_mime or "audio/wav"
                            elif out_contract == "video":
                                mime_type = source_meta_mime or "video/mp4"
                            else:
                                mime_type = source_meta_mime or "application/octet-stream"
                        else:
                            raise RuntimeError(
                                f"Source output contract mismatch: unsupported output type '{out_contract}'"
                            )

                    elif kind in {"llm", "model"}:
                        out_contract = str(_declared_out_port("llm", n) or "text")

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
                                f"LLM output contract mismatch: unsupported output type '{out_contract}'"
                            )

                    elif kind == "tool":
                        envelope = data_value if isinstance(data_value, dict) else {
                            "kind": "json",
                            "payload": data_value,
                            "meta": {"status": "ok"},
                        }
                        envelope_kind = str(envelope.get("kind") or "json")
                        out_contract = _declared_out_port("tool", n) or envelope_kind
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
                    artifact_payload_type = _declared_out_port(kind, n)
                    if kind == "source":
                        artifact_payload_type = str(
                            (_declared_out_port("source", n))
                            or artifact_payload_type
                            or "text"
                        )
                    if kind in {"llm", "model"}:
                        artifact_payload_type = str(_declared_out_port("llm", n) or "text")

                    base_payload_schema = (
                        _source_payload_schema(
                            str(artifact_payload_type or "table"),
                            data_value,
                            output.metadata if kind == "source" else None,
                        )
                        if kind == "source"
                        else _llm_payload_schema(mime_type, data_value)
                        if kind in {"llm", "model"}
                        else _tool_payload_schema(
                            str(data_value.get("kind") or "json") if isinstance(data_value, dict) else "json",
                            data_value.get("payload") if isinstance(data_value, dict) else data_value,
                            data_value.get("meta") if isinstance(data_value, dict) and isinstance(data_value.get("meta"), dict) else None,
                        )
                        if kind == "tool"
                        else None
                    ) or {}
                    if kind in {"source", "model", "llm", "tool"}:
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
                    if kind == "source" and str(artifact_payload_type or "").lower() == "table":
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
                    lineage_v1 = await _artifact_lineage_v1(
                        artifact_id=artifact_id,
                        upstream_artifact_ids=sorted(upstream_ids),
                        node_params=params if isinstance(params, dict) else {},
                        node_id=node_id,
                        run_id=run_id,
                        graph_id=context.graph_id,
                        exec_key=exec_key,
                        artifact_store=context.artifact_store,
                    )
                    base_payload_schema["artifactMetadataV1"] = _artifact_metadata_v1(
                        exec_key=exec_key,
                        node_id=node_id,
                        node_type=kind,
                        node_impl_version=node_impl_version,
                        params_fingerprint=artifact_params_hash,
                        upstream_artifact_ids=sorted(upstream_ids),
                        contract_fingerprint=contract_fingerprint,
                        schema_fingerprint=schema_fp,
                        mime_type=mime_type,
                        payload_type=artifact_payload_type,
                        schema=table_schema_env,
                        created_at_iso=created_at_dt.isoformat(),
                        run_id=run_id,
                        graph_id=context.graph_id,
                        determinism_fingerprint=_determinism_fingerprint(determinism_env),
                        code_hash=_determinism_code_hash_from_env(determinism_env),
                        profile_lock=_determinism_profile_lock_from_env(determinism_env),
                        component_context=(
                            (n.get("data", {}) or {}).get("_componentContext")
                            if isinstance((n.get("data", {}) or {}).get("_componentContext"), dict)
                            else None
                        ),
                        lineage_v1=lineage_v1,
                    )

                    artifact = Artifact(
                        artifact_id=artifact_id,
                        node_kind=kind,
                        params_hash=artifact_params_hash,
                        upstream_ids=sorted(upstream_ids),
                        created_at=created_at_dt,
                        execution_version=context.execution_version,
                        mime_type=mime_type,
                        payload_type=artifact_payload_type,
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
                    _assert_binding_ready_for_commit(
                        node_id=node_id,
                        snapshot=binding_snapshot,
                        commit_artifact_id=committed_artifact_id,
                        phase="post_write_bind",
                    )
                    context.bindings.bind(node_id=node_id, artifact_id=committed_artifact_id, status="computed")
                    expected_output_error = _expected_output_schema_error(
                        node=n,
                        artifact=artifact,
                        expected_schema=expected_schema,
                        strict_coercion_policy=bool(strict_coercion_policy),
                    )
                    if expected_output_error is not None:
                        raise expected_output_error
                    await _emit({
                        "type": "node_output",
                        "runId": run_id,
                        "nodeId": node_id,
                        "at": iso_now(),
                        "artifactId": committed_artifact_id,
                        "mimeType": artifact.mime_type,
                        "payloadType": _infer_artifact_payload_type(artifact),
                        "sourceObservability": _source_observability_from_artifact(artifact),
                        "primingArtifact": _source_priming_artifact_from_artifact(artifact),
                    })

                    # Update cache index
                    if use_cache_for_node:
                        await cache.store_artifact_id(exec_key, committed_artifact_id)

                    # Verification print
                    print(f"[artifact] node={node_id} kind={kind} bytes={len(payload_bytes)} \n\tid={artifact_id}...")

                    await _emit({
                        "type": "node_finished",
                        "runId": run_id,
                        "at": iso_now(),
                        "nodeId": node_id,
                        "status": output.status,
                        "execution_time_ms": max(0.0, float(getattr(output, "execution_time_ms", 0.0) or 0.0)),
                    })

            except asyncio.CancelledError:
                await _emit({
                    "type": "node_cancelled",
                    "runId": run_id,
                    "at": iso_now(),
                    "nodeId": node_id,
                    "status": "cancelled",
                })
                await _emit({
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
                    await _emit({
                        "type": "log",
                        "runId": run_id,
                        "at": iso_now(),
                        "level": "error",
                        "message": mismatch_msg,
                        "nodeId": node_id,
                    })
                    print(f"[schema-mismatch] runId={run_id} nodeId={node_id} {mismatch_msg}")

                if error_code:
                    env_guidance = _env_profile_log_guidance(
                        error_code=error_code,
                        error_details=error_details,
                    )
                    await _emit({
                        "type": "log",
                        "runId": run_id,
                        "at": iso_now(),
                        "level": "error",
                        "message": f"{error_code}: {error_message}",
                        "nodeId": node_id,
                    })
                    if env_guidance:
                        await _emit({
                            "type": "log",
                            "runId": run_id,
                            "at": iso_now(),
                            "level": "error",
                            "message": f"{error_code}: {env_guidance}",
                            "nodeId": node_id,
                        })

                await _emit({
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "error",
                    "message": error_message,
                    "nodeId": node_id
                })
                await _emit({
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
                    expected_payload = error_details.get("expected") if isinstance(error_details.get("expected"), dict) else {}
                    actual_payload = error_details.get("actual") if isinstance(error_details.get("actual"), dict) else {}
                    code_value = ex.code if isinstance(ex, ContractMismatchError) else "CONTRACT_MISMATCH"
                    await _emit({
                        "type": "log",
                        "runId": run_id,
                        "at": iso_now(),
                        "level": "error",
                        "message": json.dumps(
                            {
                                "code": code_value,
                                "errorCode": error_code,
                                "missingColumns": error_details.get("missingColumns", []),
                                "availableColumns": error_details.get("availableColumns", []),
                                "paramPath": error_details.get("paramPath"),
                                "expected": expected_payload,
                                "actual": actual_payload,
                                "profileId": (
                                    actual_payload.get("profileId")
                                    if isinstance(actual_payload, dict)
                                    else None
                                )
                                or (
                                    expected_payload.get("profileId")
                                    if isinstance(expected_payload, dict)
                                    else None
                                ),
                                "missingPackages": (
                                    actual_payload.get("missingPackages")
                                    if isinstance(actual_payload, dict)
                                    else None
                                ),
                                "installHint": (
                                    actual_payload.get("installHint")
                                    if isinstance(actual_payload, dict)
                                    else None
                                ),
                            },
                            ensure_ascii=False,
                        ),
                        "nodeId": node_id,
                    })
                return {"ok": False, "cached": False, "errorCode": error_code}

            # Mark incoming edges as done
            for edge_id in plan.incoming_edges.get(node_id, []):
                await _emit({
                    "type": "edge_exec",
                    "runId": run_id,
                    "at": iso_now(),
                    "edgeId": edge_id,
                    "exec": "done"
                })

            await asyncio.sleep(0.05)
            return {
                "ok": True,
                "cached": False,
                "decision": decision_value,
                "reasonCode": decision_reason_code,
            }

        run_t0 = asyncio.get_running_loop().time()
        global_sem = asyncio.Semaphore(max_inflight)
        kind_sems = {
            "source": asyncio.Semaphore(max_source),
            "transform": asyncio.Semaphore(max_transform),
            "model": asyncio.Semaphore(max_model),
            "llm": asyncio.Semaphore(max_llm),
            "tool": asyncio.Semaphore(max_tool),
        }

        inflight_current = 0
        peak_concurrency = 0
        total_cached = 0
        total_succeeded = 0
        total_failed = 0
        node_runtime_metrics: Dict[str, Dict[str, Any]] = {}
        queue_registry = QueueRegistry(
            limits=QueueLimits(
                per_edge_max=_env_int("RUNNER_QUEUE_PER_EDGE_MAX", 1000, minimum=1),
                global_max=_env_int("RUNNER_QUEUE_GLOBAL_MAX", 50000, minimum=1),
            )
        )
        node_processing_policy: Dict[str, Dict[str, Any]] = {
            nid: _node_processing_policy(nodes.get(nid, {})) for nid in nodes.keys()
        }
        node_accept_reject_counters: Dict[str, Dict[str, int]] = {
            nid: {"accepted": 0, "rejected": 0} for nid in nodes.keys()
        }
        runtime_item_metrics: Dict[str, Any] = {
            "itemsEnqueued": 0,
            "itemsDequeued": 0,
            "itemsRejected": 0,
            "itemsAccepted": 0,
            "nodeCounters": node_accept_reject_counters,
            "byPlane": {
                "work": {"itemsEnqueued": 0, "itemsDequeued": 0, "itemsAccepted": 0, "itemsRejected": 0},
                "param": {"itemsEnqueued": 0, "itemsDequeued": 0, "itemsAccepted": 0, "itemsRejected": 0},
                "control": {"itemsEnqueued": 0, "itemsDequeued": 0, "itemsAccepted": 0, "itemsRejected": 0},
            },
            "byHandle": {},
        }

        def _metric_plane(mode: str) -> str:
            value = str(mode or "work").strip().lower()
            return value if value in {"work", "param", "control"} else "work"

        def _ensure_handle_metrics(node_id: str, handle: str, plane: str) -> Dict[str, Any]:
            by_handle = runtime_item_metrics.setdefault("byHandle", {})
            key = f"{str(node_id or '').strip()}:{str(handle or 'in').strip() or 'in'}"
            existing = by_handle.get(key)
            if isinstance(existing, dict):
                if not existing.get("plane"):
                    existing["plane"] = plane
                return existing
            metrics = {
                "nodeId": str(node_id or "").strip(),
                "handle": str(handle or "in").strip() or "in",
                "plane": plane,
                "itemsEnqueued": 0,
                "itemsDequeued": 0,
                "itemsAccepted": 0,
                "itemsRejected": 0,
            }
            by_handle[key] = metrics
            return metrics

        def _inc_runtime_metric(*, plane: str, node_id: str, handle: str, field: str, amount: int = 1) -> None:
            inc = max(0, int(amount))
            if inc <= 0:
                return
            p = _metric_plane(plane)
            by_plane = runtime_item_metrics.setdefault("byPlane", {})
            plane_bucket = by_plane.setdefault(
                p,
                {"itemsEnqueued": 0, "itemsDequeued": 0, "itemsAccepted": 0, "itemsRejected": 0},
            )
            plane_bucket[field] = int(plane_bucket.get(field, 0)) + inc
            handle_bucket = _ensure_handle_metrics(node_id, handle, p)
            handle_bucket[field] = int(handle_bucket.get(field, 0)) + inc

        async def _expand_edge_work_items(
            *,
            source_node_id: str,
            source_handle: str,
            edge_obj: Dict[str, Any],
            edge_id: str,
            target_handle: str,
        ) -> List[Dict[str, Any]]:
            artifact_id = str(get_current_artifact(source_node_id, source_handle) or "").strip()
            if not artifact_id:
                return []
            item_mode = _edge_work_item_mode(edge_obj)
            max_items = _edge_work_max_items(edge_obj)
            items: List[Dict[str, Any]] = []
            if item_mode == "artifact":
                return [
                    {
                        "edgeId": edge_id,
                        "sourceNodeId": source_node_id,
                        "sourceHandle": source_handle,
                        "targetHandle": target_handle,
                        "artifactId": artifact_id,
                        "itemIndex": 0,
                        "itemMode": "artifact",
                    }
                ]
            try:
                upstream_art = await context.artifact_store.get(artifact_id)
                payload = await context.artifact_store.read(artifact_id)
            except Exception:
                return [
                    {
                        "edgeId": edge_id,
                        "sourceNodeId": source_node_id,
                        "sourceHandle": source_handle,
                        "targetHandle": target_handle,
                        "artifactId": artifact_id,
                        "itemIndex": 0,
                        "itemMode": "artifact",
                    }
                ]
            if item_mode == "json_items":
                explicit_json_items_source = False
                try:
                    parsed = json.loads(payload.decode("utf-8"))
                except Exception:
                    parsed = None
                iterable = parsed if isinstance(parsed, list) else []
                if isinstance(parsed, list):
                    explicit_json_items_source = True
                if not iterable and isinstance(parsed, dict):
                    payload_obj = parsed.get("payload")
                    if isinstance(payload_obj, list):
                        explicit_json_items_source = True
                        iterable = payload_obj
                for idx, entry in enumerate(iterable):
                    if idx >= max_items:
                        break
                    items.append(
                        {
                            "edgeId": edge_id,
                            "sourceNodeId": source_node_id,
                            "sourceHandle": source_handle,
                            "targetHandle": target_handle,
                            "artifactId": artifact_id,
                            "itemIndex": idx,
                            "itemMode": "json_items",
                            "itemPreview": entry if isinstance(entry, (dict, list, str, int, float, bool, type(None))) else str(entry),
                        }
                    )
                if explicit_json_items_source and not items:
                    return []
            elif item_mode == "table_rows":
                try:
                    df = load_table_from_artifact_bytes(str(getattr(upstream_art, "mime_type", "") or ""), payload)
                    if hasattr(df, "to_dict"):
                        rows = df.to_dict(orient="records")  # pandas DataFrame
                    elif hasattr(df, "to_dicts"):
                        rows = df.to_dicts()  # polars DataFrame (defensive)
                    else:
                        rows = []
                except Exception:
                    rows = []
                for idx, row in enumerate(rows):
                    if idx >= max_items:
                        break
                    items.append(
                        {
                            "edgeId": edge_id,
                            "sourceNodeId": source_node_id,
                            "sourceHandle": source_handle,
                            "targetHandle": target_handle,
                            "artifactId": artifact_id,
                            "itemIndex": idx,
                            "itemMode": "table_rows",
                            "itemPreview": row if isinstance(row, dict) else {},
                        }
                    )
            if not items:
                items.append(
                    {
                        "edgeId": edge_id,
                        "sourceNodeId": source_node_id,
                        "sourceHandle": source_handle,
                        "targetHandle": target_handle,
                        "artifactId": artifact_id,
                        "itemIndex": 0,
                        "itemMode": "artifact",
                    }
                )
            return items

        async def _run_with_limits(node_id: str, *, work_batch: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
            nonlocal inflight_current, peak_concurrency
            n = nodes[node_id]
            kind = n.get("data", {}).get("kind")
            kind_sem = kind_sems.get(kind)
            handle_ids: List[str] = []
            if isinstance(work_batch, list):
                seen_handles: Dict[str, bool] = {}
                for item in work_batch:
                    if not isinstance(item, dict):
                        continue
                    handle = str(item.get("targetHandle") or "in").strip() or "in"
                    if handle not in seen_handles:
                        seen_handles[handle] = True
                        handle_ids.append(handle)
            t0 = asyncio.get_running_loop().time()
            await _component_mark_node_start(node_id)
            wait_t0 = asyncio.get_running_loop().time()
            blocked = bool(global_sem.locked() or (kind_sem.locked() if kind_sem else False))
            if blocked:
                for handle in handle_ids:
                    await _emit(
                        {
                            "type": "control_signal",
                            "runId": run_id,
                            "at": iso_now(),
                            "signal": "blocked",
                            "nodeId": node_id,
                            "handle": handle,
                        }
                    )
                await _emit(
                    {
                        "type": "control_signal",
                        "runId": run_id,
                        "at": iso_now(),
                        "signal": "blocked",
                        "nodeId": node_id,
                    }
                )
            async with global_sem:
                acquired_t = asyncio.get_running_loop().time()
                inflight_current += 1
                if inflight_current > peak_concurrency:
                    peak_concurrency = inflight_current
                if blocked:
                    for handle in handle_ids:
                        await _emit(
                            {
                                "type": "control_signal",
                                "runId": run_id,
                                "at": iso_now(),
                                "signal": "resume",
                                "nodeId": node_id,
                                "handle": handle,
                            }
                        )
                    await _emit(
                        {
                            "type": "control_signal",
                            "runId": run_id,
                            "at": iso_now(),
                            "signal": "resume",
                            "nodeId": node_id,
                        }
                    )
                for handle in handle_ids:
                    await _emit(
                        {
                            "type": "control_signal",
                            "runId": run_id,
                            "at": iso_now(),
                            "signal": "busy",
                            "nodeId": node_id,
                            "handle": handle,
                        }
                    )
                await _emit(
                    {
                        "type": "control_signal",
                        "runId": run_id,
                        "at": iso_now(),
                        "signal": "busy",
                        "nodeId": node_id,
                    }
                )
                if kind_sem is None:
                    try:
                        result = await _execute_node(node_id, work_batch=work_batch)
                        await _component_mark_node_finish(
                            node_id,
                            ok=bool(result.get("ok")),
                            error=None if result.get("ok") else str(result.get("error") or ""),
                        )
                        return result
                    except asyncio.CancelledError:
                        await _emit({
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
                        await _emit({
                            "type": "log",
                            "runId": run_id,
                            "at": iso_now(),
                            "level": "error",
                            "message": f"NODE_EXECUTOR_ERROR: {str(ex)}",
                            "nodeId": node_id,
                        })
                        await _emit({
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
                        node_runtime_metrics[node_id] = {
                            "inputWaitMs": max(0.0, (acquired_t - wait_t0) * 1000.0),
                            "runTimeMs": max(0.0, (asyncio.get_running_loop().time() - t0) * 1000.0),
                            "retryCount": 0,
                            "backpressureStatus": "blocked" if blocked else "clear",
                        }
                        for handle in handle_ids:
                            await _emit(
                                {
                                    "type": "control_signal",
                                    "runId": run_id,
                                    "at": iso_now(),
                                    "signal": "ready",
                                    "nodeId": node_id,
                                    "handle": handle,
                                }
                            )
                        await _emit(
                            {
                                "type": "control_signal",
                                "runId": run_id,
                                "at": iso_now(),
                                "signal": "ready",
                                "nodeId": node_id,
                            }
                        )
                        inflight_current -= 1
                try:
                    async with kind_sem:
                        try:
                            result = await _execute_node(
                                node_id,
                                cache_only=(node_id in plan.cache_only_nodes),
                                work_batch=work_batch,
                            )
                            await _component_mark_node_finish(
                                node_id,
                                ok=bool(result.get("ok")),
                                error=None if result.get("ok") else str(result.get("error") or ""),
                            )
                            return result
                        except asyncio.CancelledError:
                            await _emit({
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
                            await _emit({
                                "type": "log",
                                "runId": run_id,
                                "at": iso_now(),
                                "level": "error",
                                "message": f"NODE_EXECUTOR_ERROR: {str(ex)}",
                                "nodeId": node_id,
                            })
                            await _emit({
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
                    node_runtime_metrics[node_id] = {
                        "inputWaitMs": max(0.0, (acquired_t - wait_t0) * 1000.0),
                        "runTimeMs": max(0.0, (asyncio.get_running_loop().time() - t0) * 1000.0),
                        "retryCount": 0,
                        "backpressureStatus": "blocked" if blocked else "clear",
                    }
                    for handle in handle_ids:
                        await _emit(
                            {
                                "type": "control_signal",
                                "runId": run_id,
                                "at": iso_now(),
                                "signal": "ready",
                                "nodeId": node_id,
                                "handle": handle,
                            }
                        )
                    await _emit(
                        {
                            "type": "control_signal",
                            "runId": run_id,
                            "at": iso_now(),
                            "signal": "ready",
                            "nodeId": node_id,
                        }
                    )
                    inflight_current -= 1

        # Queue-oriented scheduler (replaces levelized execution).
        sub = set(plan.subgraph)
        indeg: Dict[str, int] = {nid: 0 for nid in sub}
        adj: Dict[str, List[str]] = {nid: [] for nid in sub}
        for e in edges.values():
            s = e.get("source")
            t = e.get("target")
            if s in sub and t in sub:
                adj[s].append(t)
                indeg[t] += 1
        outbound_edges_by_source: Dict[str, List[Dict[str, Any]]] = {nid: [] for nid in sub}
        for edge_id, e in edges.items():
            s = e.get("source")
            t = e.get("target")
            if s in sub and t in sub:
                outbound_edges_by_source[s].append(
                    {
                        "edgeId": str(edge_id),
                        "target": str(t),
                        "targetHandle": str(e.get("targetHandle") or "in"),
                    }
                )
        order_index = {nid: i for i, nid in enumerate(plan.order)}
        for nid in adj:
            adj[nid].sort(key=lambda n: order_index.get(n, 10**9))
        for nid in outbound_edges_by_source:
            outbound_edges_by_source[nid].sort(key=lambda item: order_index.get(str(item.get("target") or ""), 10**9))
        edge_dependency_released: Dict[str, bool] = {
            str(edge_id): False
            for edge_id, edge in edges.items()
            if edge.get("source") in sub and edge.get("target") in sub
        }
        ready: List[str] = sorted(
            [nid for nid, d in indeg.items() if d == 0],
            key=lambda n: order_index.get(n, 10**9),
        )
        deps_released: Dict[str, bool] = {nid: indeg.get(nid, 0) == 0 for nid in sub}
        node_started_once: Dict[str, bool] = {nid: False for nid in sub}
        node_inflight_counts: Dict[str, int] = {nid: 0 for nid in sub}
        blocked_descendants: set[str] = set()
        inflight: Dict[asyncio.Task, Dict[str, Any]] = {}
        completed_count = 0
        run_failed = False
        total_soft_failed = 0
        connected_work_edges_by_handle: Dict[str, Dict[str, set[str]]] = {nid: {} for nid in sub}
        provided_work_edges_by_handle: Dict[str, Dict[str, set[str]]] = {nid: {} for nid in sub}
        handle_satisfaction_state: Dict[str, Dict[str, str]] = {nid: {} for nid in sub}
        connected_nonwork_edges_by_handle: Dict[str, Dict[str, Dict[str, set[str]]]] = {nid: {} for nid in sub}
        provided_nonwork_edges_by_handle: Dict[str, Dict[str, Dict[str, set[str]]]] = {nid: {} for nid in sub}
        warning_first_emitted_keys: set[str] = set()
        warning_counters: Dict[str, Dict[str, Any]] = {}

        for nid in sub:
            for incoming_edge_id in plan.incoming_edges.get(nid, []):
                incoming_edge = edges.get(incoming_edge_id) or {}
                handle = str(incoming_edge.get("targetHandle") or "in").strip() or "in"
                incoming_mode = _edge_mode(incoming_edge)
                if incoming_mode == "work":
                    connected_work_edges_by_handle.setdefault(nid, {}).setdefault(handle, set()).add(str(incoming_edge_id))
                    provided_work_edges_by_handle.setdefault(nid, {}).setdefault(handle, set())
                    continue
                connected_nonwork_edges_by_handle.setdefault(nid, {}).setdefault(handle, {}).setdefault(incoming_mode, set()).add(
                    str(incoming_edge_id)
                )
                provided_nonwork_edges_by_handle.setdefault(nid, {}).setdefault(handle, {}).setdefault(incoming_mode, set())

        def _handle_satisfaction_status(node_id: str, handle: str) -> Tuple[str, int, int]:
            connected = connected_work_edges_by_handle.get(node_id, {}).get(handle) or set()
            provided = provided_work_edges_by_handle.get(node_id, {}).get(handle) or set()
            connected_count = len(connected)
            provided_count = len(provided.intersection(connected))
            if connected_count <= 0:
                return "all", 0, 0
            if provided_count <= 0:
                return "none", connected_count, 0
            if provided_count < connected_count:
                return "partial", connected_count, provided_count
            return "all", connected_count, provided_count

        async def _emit_handle_satisfaction_if_changed(node_id: str, handle: str) -> None:
            connected = connected_work_edges_by_handle.get(node_id, {}).get(handle) or set()
            if len(connected) <= 1:
                return
            status, connected_count, provided_count = _handle_satisfaction_status(node_id, handle)
            previous = handle_satisfaction_state.setdefault(node_id, {}).get(handle)
            if previous == status:
                return
            handle_satisfaction_state[node_id][handle] = status
            await _emit(
                {
                    "type": "node_handle_satisfaction",
                    "runId": run_id,
                    "at": iso_now(),
                    "nodeId": node_id,
                    "handle": handle,
                    "status": status,
                    "connectedEdges": connected_count,
                    "providedEdges": provided_count,
                }
            )
            level = "info" if status == "all" else "warn" if status == "partial" else "error"
            await _emit(
                {
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": level,
                    "message": (
                        f"[handle-satisfaction] node={node_id} handle={handle} status={status} "
                        f"provided={provided_count}/{connected_count}"
                    ),
                    "nodeId": node_id,
                }
            )

        def _is_effectively_empty_payload(value: Any) -> bool:
            if value is None:
                return True
            if isinstance(value, str):
                return len(value.strip()) <= 0
            if isinstance(value, (bytes, bytearray)):
                return len(bytes(value).strip()) <= 0
            if isinstance(value, (list, tuple, set)):
                return len(value) <= 0
            if isinstance(value, dict):
                if not value:
                    return True
                payload_value = value.get("payload")
                if "payload" in value and _is_effectively_empty_payload(payload_value):
                    remaining = [k for k in value.keys() if str(k) not in {"payload", "meta", "kind"}]
                    if not remaining:
                        return True
                return False
            return False

        async def _artifact_has_meaningful_payload(artifact_id: str) -> Tuple[bool, str]:
            aid = str(artifact_id or "").strip()
            if not aid:
                return False, "NO_ARTIFACT"
            try:
                payload = await context.artifact_store.read(aid)
            except Exception:
                return False, "NO_ARTIFACT"
            if not isinstance(payload, (bytes, bytearray)):
                return False, "NO_ARTIFACT"
            payload_bytes = bytes(payload)
            if len(payload_bytes.strip()) <= 0:
                return False, "EMPTY_BYTES"
            try:
                decoded = payload_bytes.decode("utf-8")
                parsed = json.loads(decoded)
            except Exception:
                try:
                    decoded_text = payload_bytes.decode("utf-8", errors="ignore")
                except Exception:
                    decoded_text = ""
                if len(decoded_text.strip()) <= 0:
                    return False, "EMPTY_TEXT"
                return True, "NON_EMPTY_TEXT"
            if _is_effectively_empty_payload(parsed):
                return False, "EMPTY_JSON"
            return True, "NON_EMPTY_JSON"

        async def _emit_nonwork_empty_warning(
            *,
            target_node_id: str,
            target_handle: str,
            edge_id: str,
            plane: str,
            upstream_node_id: str,
            reason_code: str,
        ) -> None:
            warning_code = "PARAM_CONTROL_EMPTY_INPUT"
            warning_key = "|".join(
                [
                    str(run_id),
                    str(target_node_id),
                    str(target_handle),
                    str(warning_code),
                ]
            )
            now = iso_now()
            counter = warning_counters.get(warning_key)
            if not isinstance(counter, dict):
                counter = {
                    "count": 0,
                    "firstAt": now,
                    "nodeId": target_node_id,
                    "handle": target_handle,
                    "code": warning_code,
                }
            counter["count"] = int(counter.get("count") or 0) + 1
            warning_counters[warning_key] = counter

            await _emit(
                {
                    "type": "node_warning_summary",
                    "runId": run_id,
                    "at": now,
                    "warningKey": warning_key,
                    "nodeId": target_node_id,
                    "handle": target_handle,
                    "code": warning_code,
                    "plane": plane,
                    "edgeId": edge_id,
                    "reasonCode": reason_code,
                    "upstreamNodeId": upstream_node_id,
                    "count": int(counter.get("count") or 0),
                    "firstAt": str(counter.get("firstAt") or now),
                }
            )

            if warning_key in warning_first_emitted_keys:
                return
            warning_first_emitted_keys.add(warning_key)
            await _emit(
                {
                    "type": "node_input_warning",
                    "runId": run_id,
                    "at": now,
                    "nodeId": target_node_id,
                    "handle": target_handle,
                    "edgeId": edge_id,
                    "plane": plane,
                    "code": warning_code,
                    "reasonCode": reason_code,
                    "upstreamNodeId": upstream_node_id,
                    "warningKey": warning_key,
                }
            )
            await _emit(
                {
                    "type": "log",
                    "runId": run_id,
                    "at": now,
                    "level": "warn",
                    "message": (
                        f"[input-warning] node={target_node_id} handle={target_handle} edge={edge_id} "
                        f"plane={plane} code={warning_code} reason={reason_code} "
                        f"upstream={upstream_node_id}"
                    ),
                    "nodeId": target_node_id,
                }
            )

        def _incoming_edge_infos(node_id: str) -> List[Dict[str, str]]:
            infos: List[Dict[str, str]] = []
            for incoming_edge_id in plan.incoming_edges.get(node_id, []):
                incoming_edge = edges.get(incoming_edge_id) or {}
                if _edge_mode(incoming_edge) != "work":
                    continue
                infos.append(
                    {
                        "edgeId": str(incoming_edge_id),
                        "inputHandle": str(incoming_edge.get("targetHandle") or "in"),
                    }
                )
            infos.sort(key=lambda item: (item.get("inputHandle") or "", item.get("edgeId") or ""))
            return infos

        def _effective_node_runtime_policy(node_id: str) -> Dict[str, Any]:
            node_obj = nodes.get(node_id, {})
            base = _node_processing_policy(node_obj)
            incoming = _incoming_edge_infos(node_id)
            if not incoming:
                return base
            merged_mode = str(base.get("consume_mode") or "once")
            merged_batch = int(base.get("batch_size") or 1)
            inflight_caps = [max(1, int(base.get("max_inflight") or 1))]
            for info in incoming:
                handle = str(info.get("inputHandle") or "in")
                handle_policy = _node_processing_policy(node_obj, input_handle=handle)
                inflight_caps.append(max(1, int(handle_policy.get("max_inflight") or 1)))
                handle_mode = str(handle_policy.get("consume_mode") or "once")
                if handle_mode == "batch":
                    merged_mode = "batch"
                elif handle_mode == "single_item" and merged_mode == "once":
                    merged_mode = "single_item"
                merged_batch = max(merged_batch, max(1, int(handle_policy.get("batch_size") or 1)))
            return {
                "consume_mode": merged_mode,
                "batch_size": merged_batch,
                "max_inflight": min(inflight_caps),
            }

        def _node_has_ready_work(node_id: str) -> bool:
            incoming = _incoming_edge_infos(node_id)
            if not incoming:
                return True
            incoming_by_handle: Dict[str, List[str]] = {}
            for info in incoming:
                handle = str(info.get("inputHandle") or "in")
                edge_id = str(info.get("edgeId") or "")
                if not edge_id:
                    continue
                incoming_by_handle.setdefault(handle, []).append(edge_id)
            if not incoming_by_handle:
                return False
            for handle, edge_ids in incoming_by_handle.items():
                handle_depth = sum(
                    max(0, int(queue_registry.depth(str(edge_id), handle) or 0))
                    for edge_id in edge_ids
                )
                if handle_depth <= 0:
                    return False
            return True

        async def _dequeue_work_batch(node_id: str) -> List[Dict[str, Any]]:
            incoming = _incoming_edge_infos(node_id)
            if not incoming:
                return []
            out: List[Dict[str, Any]] = []
            per_handle_take_map: Dict[str, int] = {}
            incoming_by_handle: Dict[str, List[Dict[str, str]]] = {}
            handle_cursor: Dict[str, int] = {}
            handle_queue_policy: Dict[str, str] = {}
            for info in incoming:
                handle = str(info.get("inputHandle") or "in")
                handle_policy = _node_processing_policy(nodes.get(node_id, {}), input_handle=handle)
                consume_mode = str(handle_policy.get("consume_mode") or "once")
                handle_take = (
                    max(1, int(handle_policy.get("batch_size") or 1))
                    if consume_mode == "batch"
                    else 1
                )
                per_handle_take_map[handle] = max(int(per_handle_take_map.get(handle, 0)), handle_take)
                incoming_by_handle.setdefault(handle, []).append(
                    {
                    "edgeId": str(info.get("edgeId") or ""),
                    "inputHandle": handle,
                    }
                )
                policy = _edge_queue_policy(edges.get(str(info.get("edgeId") or "")) or {})
                existing_policy = str(handle_queue_policy.get(handle) or "")
                if not existing_policy:
                    handle_queue_policy[handle] = policy
                elif existing_policy != policy:
                    # Mixed policies on same handle fall back to deterministic default.
                    handle_queue_policy[handle] = "fifo"
            for handle in incoming_by_handle.keys():
                handle_cursor[handle] = 0
            per_handle_take: List[Tuple[str, int]] = [
                (handle, int(count))
                for handle, count in sorted(per_handle_take_map.items(), key=lambda item: item[0])
            ]
            for handle in _build_fair_dequeue_plan(per_handle_take):
                handle_edges = incoming_by_handle.get(handle) or []
                edge_keys = [str((item or {}).get("edgeId") or "") for item in handle_edges if str((item or {}).get("edgeId") or "")]
                arbitration_policy = str(handle_queue_policy.get(handle) or "fifo")
                selected_edge_id: Optional[str] = None
                next_cursor = int(handle_cursor.get(handle, 0))
                if arbitration_policy == "round_robin":
                    selected_edge_id, next_cursor = next_nonempty_key(
                        edge_keys,
                        start_index=next_cursor,
                        has_items=lambda edge_id: queue_registry.depth(str(edge_id), handle) > 0,
                    )
                    handle_cursor[handle] = next_cursor
                else:
                    fifo_candidates: List[Tuple[float, str]] = []
                    for edge_id in edge_keys:
                        ts = queue_registry.head_enqueued_at(str(edge_id), handle)
                        if ts is None:
                            continue
                        fifo_candidates.append((float(ts), str(edge_id)))
                    if fifo_candidates:
                        fifo_candidates.sort(key=lambda item: (item[0], item[1]))
                        selected_edge_id = str(fifo_candidates[0][1])
                if not selected_edge_id:
                    continue
                incoming_info = {"edgeId": selected_edge_id, "inputHandle": handle}
                item = await queue_registry.dequeue(
                    str(incoming_info.get("edgeId") or ""),
                    handle,
                    timeout_sec=0.0,
                )
                if item is None:
                    continue
                runtime_item_metrics["itemsDequeued"] = int(runtime_item_metrics.get("itemsDequeued", 0)) + 1
                _inc_runtime_metric(
                    plane=_edge_mode(edges.get(str(incoming_info.get("edgeId") or "")) or {}),
                    node_id=node_id,
                    handle=handle,
                    field="itemsDequeued",
                    amount=1,
                )
                out.append(item if isinstance(item, dict) else {"item": item})
                await _emit_handle_satisfaction_if_changed(node_id, handle)
            return out

        def _enqueue_ready_if_possible(node_id: str) -> None:
            if node_id in blocked_descendants:
                return
            if not deps_released.get(node_id, False):
                return
            policy = _effective_node_runtime_policy(node_id)
            consume_mode = str(policy.get("consume_mode") or "once")
            per_node_max = max(1, int(policy.get("max_inflight") or 1))
            in_flight_for_node = int(node_inflight_counts.get(node_id, 0))
            in_ready = ready.count(node_id)
            if in_flight_for_node + in_ready >= per_node_max:
                return
            if consume_mode == "once":
                if node_started_once.get(node_id, False):
                    return
                ready.append(node_id)
                return
            if _node_has_ready_work(node_id):
                ready.append(node_id)
        await _emit(
            {
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "info",
                "message": (
                    f"[scheduler] queue start nodes={len(sub)} ready={len(ready)} "
                    f"caps(g={max_inflight},s={max_source},t={max_transform},m={max_model},l={max_llm},tool={max_tool})"
                ),
            }
        )
        await _emit({"type": "control_signal", "runId": run_id, "at": iso_now(), "signal": "ready"})

        while ready or inflight:
            elapsed_ms = int((asyncio.get_running_loop().time() - run_t0) * 1000)
            if max_runtime_ms > 0 and elapsed_ms > max_runtime_ms:
                timeout_msg = (
                    f"RUN_TIMEOUT: runtime exceeded {max_runtime_ms}ms "
                    f"during queue scheduling (elapsed={elapsed_ms}ms)"
                )
                await _emit({"type": "log", "runId": run_id, "at": iso_now(), "level": "error", "message": timeout_msg})
                await _emit(
                    {
                        "type": "run_finished",
                        "runId": run_id,
                        "at": iso_now(),
                        "status": "failed",
                        "error": timeout_msg,
                        "errorCode": "RUN_TIMEOUT",
                    }
                )
                await _emit_cache_summary_once()
                return
            if cancel_event and cancel_event.is_set():
                cancelled_tasks = 0
                for task in list(inflight.keys()):
                    if not task.done():
                        task.cancel()
                        cancelled_tasks += 1
                await _emit(
                    {
                        "type": "scheduler_cancelled",
                        "runId": run_id,
                        "at": iso_now(),
                        "scheduled": completed_count + len(inflight),
                        "inflightCancelled": cancelled_tasks,
                        "completedBeforeCancel": completed_count,
                    }
                )
                await _emit({"type": "run_cancelled", "runId": run_id, "at": iso_now()})
                await _emit({"type": "run_finished", "runId": run_id, "at": iso_now(), "status": "cancelled"})
                await _emit_cache_summary_once()
                return

            while ready and len(inflight) < max_inflight:
                nid = ready.pop(0)
                if nid in blocked_descendants:
                    continue
                policy = _effective_node_runtime_policy(nid)
                consume_mode = str(policy.get("consume_mode") or "once")
                per_node_max = max(1, int(policy.get("max_inflight") or 1))
                if int(node_inflight_counts.get(nid, 0)) >= per_node_max:
                    # Per-node inflight cap reached; defer.
                    ready.append(nid)
                    break
                work_batch: List[Dict[str, Any]] = []
                if consume_mode in {"single_item", "batch"} and plan.incoming_edges.get(nid, []):
                    if not _node_has_ready_work(nid):
                        continue
                    work_batch = await _dequeue_work_batch(nid)
                    if not work_batch:
                        continue
                task = asyncio.create_task(
                    _run_with_limits(
                        nid,
                        work_batch=work_batch,
                    )
                )
                inflight[task] = {"nodeId": nid, "workBatch": work_batch}
                node_inflight_counts[nid] = int(node_inflight_counts.get(nid, 0)) + 1
                node_started_once[nid] = True

            if not inflight:
                await _emit({"type": "control_signal", "runId": run_id, "at": iso_now(), "signal": "pause"})
                break

            done, _pending = await asyncio.wait(
                set(inflight.keys()),
                timeout=0.05,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                inflight_info = inflight.pop(task, {}) if isinstance(inflight.get(task), dict) else {}
                node_id = str(inflight_info.get("nodeId") or "")
                work_batch = inflight_info.get("workBatch") if isinstance(inflight_info.get("workBatch"), list) else []
                node_inflight_counts[node_id] = max(0, int(node_inflight_counts.get(node_id, 0)) - 1)
                completed_count += 1
                try:
                    result = task.result()
                except asyncio.CancelledError:
                    result = {"ok": False, "cached": False, "cancelled": True}
                except Exception as ex:
                    raise ex
                ok = bool(result.get("ok"))
                cached = bool(result.get("cached"))
                cancelled = bool(result.get("cancelled"))
                if cancelled:
                    total_failed += 1
                    run_failed = True
                elif cached:
                    total_cached += 1
                elif ok:
                    total_succeeded += 1
                if not ok:
                    node_fatal = _is_node_or_edge_fatal(
                        node=nodes.get(node_id, {}),
                        incoming_edge_ids=plan.incoming_edges.get(node_id, []),
                        edges=edges,
                    )
                    node_policy = _node_processing_policy(nodes.get(node_id, {}))
                    soft_fail_skip = bool(
                        not node_fatal
                        and str(node_policy.get("on_error") or "fail_fast") == "skip_failed"
                        and bool(work_batch)
                    )
                    if soft_fail_skip:
                        total_soft_failed += 1
                    else:
                        total_failed += 1
                    await _emit(
                        {
                            "type": "log",
                            "runId": run_id,
                            "at": iso_now(),
                            "level": "warn" if not node_fatal else "error",
                            "message": (
                                f"[scheduler] node failure node={node_id} fatal={str(node_fatal).lower()} "
                                "propagation=localized"
                                if not node_fatal
                                else f"[scheduler] node failure node={node_id} fatal=true propagation=run_stop"
                            ),
                            "nodeId": node_id,
                        }
                    )
                    if soft_fail_skip:
                        failed_count = max(1, len(work_batch or []))
                        runtime_item_metrics["itemsRejected"] = int(runtime_item_metrics.get("itemsRejected", 0)) + failed_count
                        node_accept_reject_counters.setdefault(node_id, {"accepted": 0, "rejected": 0})
                        node_accept_reject_counters[node_id]["rejected"] = int(
                            node_accept_reject_counters[node_id].get("rejected", 0)
                        ) + failed_count
                        if work_batch:
                            for item in work_batch:
                                handle = str((item or {}).get("targetHandle") or "in").strip() or "in"
                                _inc_runtime_metric(
                                    plane="work",
                                    node_id=node_id,
                                    handle=handle,
                                    field="itemsRejected",
                                    amount=1,
                                )
                        else:
                            _inc_runtime_metric(
                                plane="work",
                                node_id=node_id,
                                handle="in",
                                field="itemsRejected",
                                amount=1,
                            )
                        await _emit(
                            {
                                "type": "log",
                                "runId": run_id,
                                "at": iso_now(),
                                "level": "warn",
                                "message": (
                                    f"[scheduler] soft-fail skip node={node_id} on_error=skip_failed "
                                    f"errorCode={str(result.get('errorCode') or '') or 'UNKNOWN'} "
                                    f"items={failed_count}"
                                ),
                                "nodeId": node_id,
                            }
                        )
                        _enqueue_ready_if_possible(node_id)
                        await _emit(
                            {
                                "type": "queue_metrics",
                                "runId": run_id,
                                "at": iso_now(),
                                "scope": "run",
                                "metrics": queue_registry.metrics(),
                                "nodeMetrics": node_runtime_metrics,
                                "runtimeItemMetrics": runtime_item_metrics,
                            }
                        )
                        continue
                    if node_fatal:
                        run_failed = True
                        break
                    # Localized failure: block transitive descendants on this branch.
                    cascade_reason = str(result.get("errorCode") or "").strip()
                    should_cascade = cascade_reason in {
                        "HANDLE_INPUT_NONE_PROVIDED",
                        "HANDLE_INPUT_SATISFACTION_NONE_PROVIDED",
                    }
                    if should_cascade:
                        cascade_nodes = collect_transitive_descendants(
                            adj,
                            node_id,
                            allowed_nodes=sub,
                        )
                        newly_blocked: List[str] = []
                        for blocked_id in cascade_nodes:
                            if blocked_id in blocked_descendants or blocked_id == node_id:
                                continue
                            blocked_descendants.add(blocked_id)
                            newly_blocked.append(blocked_id)
                        if newly_blocked:
                            ready = [ready_node for ready_node in ready if ready_node not in blocked_descendants]
                            await _emit(
                                {
                                    "type": "branch_cascade",
                                    "runId": run_id,
                                    "at": iso_now(),
                                    "originNodeId": node_id,
                                    "blockedNodeIds": newly_blocked,
                                    "reasonCode": cascade_reason,
                                }
                            )
                            for blocked_id in sorted(newly_blocked, key=lambda item: order_index.get(item, 10**9)):
                                await _emit(
                                    {
                                        "type": "control_signal",
                                        "runId": run_id,
                                        "at": iso_now(),
                                        "signal": "blocked",
                                        "nodeId": blocked_id,
                                    }
                                )
                                await _emit(
                                    {
                                        "type": "log",
                                        "runId": run_id,
                                        "at": iso_now(),
                                        "level": "warn",
                                        "message": (
                                            f"[scheduler] branch cascade origin={node_id} blocked={blocked_id} "
                                            f"reason={cascade_reason}"
                                        ),
                                        "nodeId": blocked_id,
                                    }
                                )
                    continue
                # decision / reject semantics
                decision = str(result.get("decision") or "accept").strip().lower() if isinstance(result, dict) else "accept"
                if decision not in {"accept", "reject"}:
                    decision = "accept"
                if decision == "reject":
                    runtime_item_metrics["itemsRejected"] = int(runtime_item_metrics.get("itemsRejected", 0)) + max(1, len(work_batch or []))
                    node_accept_reject_counters.setdefault(node_id, {"accepted": 0, "rejected": 0})
                    node_accept_reject_counters[node_id]["rejected"] = int(node_accept_reject_counters[node_id].get("rejected", 0)) + max(1, len(work_batch or []))
                    if work_batch:
                        for item in work_batch:
                            handle = str((item or {}).get("targetHandle") or "in").strip() or "in"
                            _inc_runtime_metric(
                                plane="work",
                                node_id=node_id,
                                handle=handle,
                                field="itemsRejected",
                                amount=1,
                            )
                    else:
                        _inc_runtime_metric(
                            plane="work",
                            node_id=node_id,
                            handle="in",
                            field="itemsRejected",
                            amount=1,
                        )
                else:
                    runtime_item_metrics["itemsAccepted"] = int(runtime_item_metrics.get("itemsAccepted", 0)) + max(1, len(work_batch or []))
                    node_accept_reject_counters.setdefault(node_id, {"accepted": 0, "rejected": 0})
                    node_accept_reject_counters[node_id]["accepted"] = int(node_accept_reject_counters[node_id].get("accepted", 0)) + max(1, len(work_batch or []))
                    if work_batch:
                        for item in work_batch:
                            handle = str((item or {}).get("targetHandle") or "in").strip() or "in"
                            _inc_runtime_metric(
                                plane="work",
                                node_id=node_id,
                                handle=handle,
                                field="itemsAccepted",
                                amount=1,
                            )
                    else:
                        _inc_runtime_metric(
                            plane="work",
                            node_id=node_id,
                            handle="in",
                            field="itemsAccepted",
                            amount=1,
                        )
                await _emit(
                    {
                        "type": "node_decision",
                        "runId": run_id,
                        "at": iso_now(),
                        "nodeId": node_id,
                        "decision": decision,
                        "count": max(1, len(work_batch or [])),
                        "reasonCode": str(result.get("reasonCode") or ""),
                    }
                )
                if decision == "reject":
                    reject_reason = str(result.get("reasonCode") or "NODE_REJECTED").strip() or "NODE_REJECTED"
                    handle_counts: Dict[str, int] = {}
                    for item in work_batch or []:
                        handle = str((item or {}).get("targetHandle") or "in").strip() or "in"
                        handle_counts[handle] = int(handle_counts.get(handle, 0)) + 1
                    await _emit(
                        {
                            "type": "node_reject",
                            "runId": run_id,
                            "at": iso_now(),
                            "nodeId": node_id,
                            "plane": "work",
                            "reasonCode": reject_reason,
                            "count": max(1, len(work_batch or [])),
                            "handleCounts": handle_counts,
                            "counters": {
                                "itemsRejected": int(runtime_item_metrics.get("itemsRejected", 0)),
                                "nodeRejected": int(
                                    (node_accept_reject_counters.get(node_id) or {}).get("rejected") or 0
                                ),
                            },
                        }
                    )
                    reject_fatal = False
                    node_data = (nodes.get(node_id, {}) or {}).get("data", {}) if isinstance(nodes.get(node_id, {}), dict) else {}
                    params_obj = (node_data.get("params") or {}) if isinstance(node_data, dict) else {}
                    policy_obj = (node_data.get("processingPolicy") or {}) if isinstance(node_data, dict) else {}
                    reject_fatal = bool(
                        (params_obj.get("reject_fatal") if isinstance(params_obj, dict) else False)
                        or (params_obj.get("rejectFatal") if isinstance(params_obj, dict) else False)
                        or (policy_obj.get("reject_fatal") if isinstance(policy_obj, dict) else False)
                        or (policy_obj.get("rejectFatal") if isinstance(policy_obj, dict) else False)
                    )
                    # Reject is non-error; do not release downstream dependencies from this item.
                    _enqueue_ready_if_possible(node_id)
                    await _emit(
                        {
                            "type": "queue_metrics",
                            "runId": run_id,
                            "at": iso_now(),
                            "scope": "run",
                            "metrics": queue_registry.metrics(),
                            "nodeMetrics": node_runtime_metrics,
                            "runtimeItemMetrics": runtime_item_metrics,
                        }
                    )
                    if reject_fatal:
                        await _emit(
                            {
                                "type": "log",
                                "runId": run_id,
                                "at": iso_now(),
                                "level": "error",
                                "message": f"[scheduler] node reject treated as fatal node={node_id} reason={reject_reason}",
                                "nodeId": node_id,
                            }
                        )
                        run_failed = True
                    continue
                # Node succeeded/cached, release downstream dependencies through per-edge queues.
                for edge_info in outbound_edges_by_source.get(node_id, []):
                    edge_id = str(edge_info.get("edgeId") or "")
                    nb = str(edge_info.get("target") or "")
                    target_handle = str(edge_info.get("targetHandle") or "in")
                    source_handle = str(edge_info.get("sourceHandle") or "out")
                    if not edge_id or not nb:
                        continue
                    edge_obj = edges.get(edge_id) or {}
                    edge_mode = _edge_mode(edge_obj)
                    if edge_mode != "work":
                        source_artifact_id = str(get_current_artifact(node_id, source_handle) or "").strip()
                        meaningful, empty_reason = await _artifact_has_meaningful_payload(source_artifact_id)
                        if meaningful:
                            provided_nonwork_edges_by_handle.setdefault(nb, {}).setdefault(target_handle, {}).setdefault(
                                edge_mode, set()
                            ).add(edge_id)
                        else:
                            await _emit_nonwork_empty_warning(
                                target_node_id=nb,
                                target_handle=target_handle,
                                edge_id=edge_id,
                                plane=edge_mode,
                                upstream_node_id=node_id,
                                reason_code=empty_reason,
                            )
                        if not bool(edge_dependency_released.get(edge_id, False)):
                            edge_dependency_released[edge_id] = True
                            indeg[nb] = max(0, indeg.get(nb, 0) - 1)
                            if indeg[nb] == 0:
                                deps_released[nb] = True
                                _enqueue_ready_if_possible(nb)
                        continue
                    work_items = await _expand_edge_work_items(
                        source_node_id=node_id,
                        source_handle=source_handle,
                        edge_obj=edge_obj,
                        edge_id=edge_id,
                        target_handle=target_handle,
                    )
                    overflow_mode = str(
                        (((edge_obj.get("data") or {}) if isinstance(edge_obj.get("data"), dict) else {}).get("queue") or {}).get("overflow")
                        or "block"
                    ).strip().lower()
                    for item in work_items:
                        await queue_registry.enqueue(
                            edge_id,
                            target_handle,
                            item,
                            overflow=overflow_mode if overflow_mode in {"block", "spill", "error"} else "block",
                        )
                        provided_work_edges_by_handle.setdefault(nb, {}).setdefault(target_handle, set()).add(edge_id)
                        runtime_item_metrics["itemsEnqueued"] = int(runtime_item_metrics.get("itemsEnqueued", 0)) + 1
                        _inc_runtime_metric(
                            plane=edge_mode,
                            node_id=nb,
                            handle=target_handle,
                            field="itemsEnqueued",
                            amount=1,
                        )
                    if not bool(edge_dependency_released.get(edge_id, False)):
                        edge_dependency_released[edge_id] = True
                        indeg[nb] = max(0, indeg.get(nb, 0) - 1)
                        if indeg[nb] == 0:
                            deps_released[nb] = True
                            _enqueue_ready_if_possible(nb)
                    await _emit_handle_satisfaction_if_changed(nb, target_handle)
                _enqueue_ready_if_possible(node_id)
                ready.sort(key=lambda n: order_index.get(n, 10**9))
                await _emit(
                    {
                        "type": "queue_metrics",
                        "runId": run_id,
                        "at": iso_now(),
                        "scope": "run",
                        "metrics": queue_registry.metrics(),
                        "nodeMetrics": node_runtime_metrics,
                        "runtimeItemMetrics": runtime_item_metrics,
                    }
                )

            if run_failed:
                for task in list(inflight.keys()):
                    if not task.done():
                        task.cancel()
                if inflight:
                    await asyncio.gather(*list(inflight.keys()), return_exceptions=True)
                break

            if not inflight and not ready:
                for candidate in sorted(sub, key=lambda n: order_index.get(n, 10**9)):
                    if not deps_released.get(candidate, False):
                        continue
                    if node_started_once.get(candidate, False):
                        continue
                    handle_map = connected_work_edges_by_handle.get(candidate, {}) or {}
                    for handle, edge_ids in handle_map.items():
                        if len(edge_ids) <= 1:
                            continue
                        status, connected_count, provided_count = _handle_satisfaction_status(candidate, handle)
                        if status != "none":
                            continue
                        await _emit_handle_satisfaction_if_changed(candidate, handle)
                        await _emit(
                            {
                                "type": "node_finished",
                                "runId": run_id,
                                "at": iso_now(),
                                "nodeId": candidate,
                                "status": "failed",
                                "error": "required work input handle had none provided",
                                "errorCode": "HANDLE_INPUT_NONE_PROVIDED",
                                "errorDetails": {
                                    "expected": {"handle": handle, "connectedEdges": connected_count},
                                    "actual": {"providedEdges": provided_count},
                                },
                                "execution_time_ms": 0.0,
                            }
                        )
                        total_failed += 1
                        await _emit(
                            {
                                "type": "log",
                                "runId": run_id,
                                "at": iso_now(),
                                "level": "warn",
                                "message": (
                                    f"[scheduler] node failure node={candidate} fatal=false propagation=localized "
                                    "reason=HANDLE_INPUT_NONE_PROVIDED"
                                ),
                                "nodeId": candidate,
                            }
                        )
                        break
                    nonwork_handle_map = connected_nonwork_edges_by_handle.get(candidate, {}) or {}
                    for handle, plane_map in nonwork_handle_map.items():
                        for plane, edge_ids in plane_map.items():
                            provided_ids = (
                                provided_nonwork_edges_by_handle.get(candidate, {})
                                .get(handle, {})
                                .get(plane, set())
                            ) or set()
                            missing_ids = sorted(list(set(edge_ids).difference(set(provided_ids))))
                            for missing_edge_id in missing_ids:
                                edge_obj = edges.get(str(missing_edge_id)) or {}
                                upstream_node = str(edge_obj.get("source") or "").strip()
                                if not upstream_node:
                                    continue
                                if int(node_inflight_counts.get(upstream_node, 0)) > 0:
                                    # Suppress warning while upstream is still running/busy.
                                    continue
                                source_handle = str(edge_obj.get("sourceHandle") or "out").strip() or "out"
                                source_artifact_id = str(get_current_artifact(upstream_node, source_handle) or "").strip()
                                meaningful, empty_reason = await _artifact_has_meaningful_payload(source_artifact_id)
                                if meaningful:
                                    provided_nonwork_edges_by_handle.setdefault(candidate, {}).setdefault(handle, {}).setdefault(
                                        plane, set()
                                    ).add(str(missing_edge_id))
                                    continue
                                if not bool(node_started_once.get(upstream_node, False)):
                                    empty_reason = "UPSTREAM_NEVER_STARTED"
                                await _emit_nonwork_empty_warning(
                                    target_node_id=candidate,
                                    target_handle=handle,
                                    edge_id=str(missing_edge_id),
                                    plane=str(plane),
                                    upstream_node_id=upstream_node,
                                    reason_code=empty_reason,
                                )

        await _emit({"type": "control_signal", "runId": run_id, "at": iso_now(), "signal": "drain"})

        total_runtime_ms = int((asyncio.get_running_loop().time() - run_t0) * 1000)
        await _emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "info",
            "message": (
                f"[scheduler] summary executed={total_succeeded + total_failed + total_soft_failed} "
                f"cached={total_cached} failed={total_failed} "
                f"soft_failed={total_soft_failed} "
                f"peak_concurrency={peak_concurrency} runtime_ms={total_runtime_ms}"
            ),
        })
        await _emit_cache_summary_once()

        if run_failed or total_failed > 0:
            await _emit({
                "type": "run_finished",
                "runId": run_id,
                "at": iso_now(),
                "status": "failed"
            })
            await _emit_cache_summary_once()
            return

        await _emit({
            "type": "run_finished",
            "runId": run_id,
            "at": iso_now(),
            "status": "succeeded"
        })
        await _emit_cache_summary_once()
    except asyncio.CancelledError:
        await _emit({
            "type": "run_cancelled",
            "runId": run_id,
            "at": iso_now(),
        })
        await _emit({
            "type": "run_finished",
            "runId": run_id,
            "at": iso_now(),
            "status": "cancelled"
        })
        await _emit_cache_summary_once()
        return
    except Exception as ex:
        traceback.print_exc()
        await _emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "error",
            "message": str(ex)
        })
        await _emit({
            "type": "run_finished",
            "runId": run_id,
            "at": iso_now(),
            "status": "failed"
        })
        await _emit_cache_summary_once()


