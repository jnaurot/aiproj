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
from .events import RunEventBus
from .validator import GraphValidator
from .metadata import GraphContext, NodeOutput
from .artifacts import Artifact, MemoryArtifactStore, RunBindings
from .cache import ExecutionCache
from .node_state import build_exec_key, build_node_state_hash, build_source_fingerprint
from .capabilities import allowed_ports

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
        source_kind = (node.get("data", {}).get("sourceKind") or p.get("source_type") or "file")
        p["source_type"] = source_kind
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
    )


def _tool_is_armed(params: Dict[str, Any]) -> bool:
    return bool(params.get("armed", False))


def _table_payload_schema_from_rows(rows: list[dict[str, Any]]) -> Dict[str, Any]:
    columns: list[Dict[str, Any]] = []
    if rows:
        sample = rows[0]
        for k, v in sample.items():
            columns.append({"name": str(k), "dtype": type(v).__name__ if v is not None else "unknown"})
    return {"schema_version": 1, "type": "table", "columns": columns}


def _source_payload_schema(out_contract: Optional[str], data_value: Any) -> Optional[Dict[str, Any]]:
    if out_contract == "table" and isinstance(data_value, list):
        return _table_payload_schema_from_rows(data_value)
    if out_contract == "json":
        return {
            "schema_version": 1,
            "type": "json",
            "json_shape": "array" if isinstance(data_value, list) else "object",
        }
    if out_contract == "text":
        return {"schema_version": 1, "type": "text", "encoding": "utf-8"}
    if out_contract == "binary":
        return {"schema_version": 1, "type": "binary"}
    return None


def _llm_payload_schema(mime_type: str, data_value: Any) -> Optional[Dict[str, Any]]:
    mt = (mime_type or "").lower()
    if "application/json" in mt:
        parsed = data_value
        if isinstance(parsed, str):
            try:
                parsed = json.loads(parsed)
            except Exception:
                return {"schema_version": 1, "type": "json", "json_shape": "unknown"}
        if isinstance(parsed, dict):
            return {
                "schema_version": 1,
                "type": "json",
                "json_shape": "object",
                "keys_sample": sorted(list(parsed.keys())),
            }
        if isinstance(parsed, list):
            return {"schema_version": 1, "type": "json", "json_shape": "array"}
        return {"schema_version": 1, "type": "json", "json_shape": "unknown"}
    if "text/markdown" in mt:
        return {"schema_version": 1, "type": "text", "encoding": "utf-8", "format": "markdown"}
    if "text/plain" in mt:
        return {"schema_version": 1, "type": "text", "encoding": "utf-8"}
    return None


def _tool_payload_schema(envelope_kind: str, payload: Any) -> Optional[Dict[str, Any]]:
    if envelope_kind == "json":
        if isinstance(payload, dict):
            return {
                "schema_version": 1,
                "type": "json",
                "json_shape": "object",
                "keys_sample": sorted(list(payload.keys())),
            }
        if isinstance(payload, list):
            return {"schema_version": 1, "type": "json", "json_shape": "array"}
        return {"schema_version": 1, "type": "json", "json_shape": "unknown"}
    if envelope_kind == "text":
        return {"schema_version": 1, "type": "text", "encoding": "utf-8"}
    if envelope_kind == "binary":
        return {"schema_version": 1, "type": "binary"}
    return None


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
    "CONTRACT_MISMATCH",
}
_DEFAULT_REASON_BY_DECISION = {
    "cache_hit": "CACHE_HIT",
    "cache_miss": "CACHE_ENTRY_MISSING",
    "cache_hit_contract_mismatch": "CONTRACT_MISMATCH",
}


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


def _declared_out_port(kind: str, node: Dict[str, Any]) -> Optional[str]:
    ports = (node.get("data", {}).get("ports", {}) or {})
    declared = ports.get("out")
    if declared:
        return str(declared)
    if kind == "transform":
        return "table"
    if kind == "llm":
        return "text"
    if kind == "tool":
        return "json"
    return None


def _allowed_ports(kind: str, direction: str, provider: Optional[str] = None) -> set[str]:
    return allowed_ports(kind, direction, provider=provider)


def _cached_artifact_contract_mismatch(kind: str, node: Dict[str, Any], artifact: Artifact) -> Optional[Dict[str, Any]]:
    declared = _declared_out_port(kind, node)
    if not declared:
        return None
    artifact_pt = _infer_artifact_port_type(artifact)
    if declared != artifact_pt:
        return {
            "message": (
                "Contract mismatch (cache hit): "
                f"declared out='{declared}' but cached artifact port_type='{artifact_pt}'"
            ),
            "expectedPortType": declared,
            "actualPortType": artifact_pt,
            "artifactId": artifact.artifact_id,
            "producerExecKey": artifact.exec_key,
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
        return {
            "llm_input_format": "artifact_text_v1",
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

    # ===== PHASE 2: EXECUTION =====
    try:
        plan = compile_plan(graph, run_from, run_mode=run_mode)
        context.planner_ref = plan
        effective_run_mode = "from_start" if run_from is None else (str(run_mode or "from_selected_onward"))
        await context.bus.emit({
            "type": "run_started",
            "runId": run_id,
            "at": iso_now(),
            "runFrom": run_from,
            "runMode": effective_run_mode,
            "plannedNodeIds": sorted(list(plan.subgraph)),
        })
        nodes = node_map(graph)
        edges = edge_map(graph)
        get_current_artifact = context.bindings.get_current_artifact

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
            tool_mode = _tool_side_effect_mode(params) if kind == "tool" else None
            use_cache_for_node = not (kind == "tool" and tool_mode == "effectful")

            up_nodes = sorted(upstream_node_ids(edges, node_id))
            upstream_ids = [aid for aid in (get_current_artifact(nid) for nid in up_nodes) if aid]
            input_refs = resolve_input_refs(edges, node_id, get_current_artifact)

            normalized_params_for_hash = _normalized_params_for_exec_key(
                kind=kind,
                node=n,
                params=params,
            )
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
            )
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
                "use_cache_for_node": use_cache_for_node,
                "upstream_ids": upstream_ids,
                "input_refs": input_refs,
                "node_state_hash": node_state_hash,
                "exec_key": exec_key,
                "artifact_id": exec_key,
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
            use_cache_for_node = resolved["use_cache_for_node"]
            upstream_ids = resolved["upstream_ids"]
            input_refs = resolved["input_refs"]
            node_state_hash = resolved["node_state_hash"]
            exec_key = resolved["exec_key"]
            artifact_id = resolved["artifact_id"]
            cache_resolution = resolved["cache_resolution"]
            cached_artifact_id = resolved["cached_artifact_id"]

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
                    reason="UNCACHEABLE_EFFECTFUL_TOOL",
                )
                if cache_only:
                    msg = (
                        "Selected-only run requires cache for ancestor nodes, "
                        f"but node '{node_id}' is uncacheable (effectful tool)."
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
                mismatch_error = _cached_artifact_contract_mismatch(kind, n, cached_art)
                if mismatch_error:
                    # Contract mismatch on cached artifact is a cache rejection, not a successful cache hit.
                    cache_stats["hit_contract_mismatch"] += 1
                    await _emit_cache_decision(
                        node_id=node_id,
                        node_kind=kind,
                        decision="cache_hit_contract_mismatch",
                        exec_key=exec_key,
                        artifact_id=cached_artifact_id,
                        expected_port_type=mismatch_error["expectedPortType"],
                        actual_port_type=mismatch_error["actualPortType"],
                        producer_exec_key=mismatch_error.get("producerExecKey"),
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
                        "expectedPortType": mismatch_error["expectedPortType"],
                        "actualPortType": mismatch_error["actualPortType"],
                        "artifactId": mismatch_error["artifactId"],
                        "producerExecKey": mismatch_error.get("producerExecKey"),
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
                        required_cols_by_artifact: dict[str, list[str]] = {}

                        for e in edges.values():
                            if e.get("target") != node_id:
                                continue
                            src = e.get("source")
                            if not src:
                                continue
                            src_artifact_id = get_current_artifact(src)
                            if not src_artifact_id:
                                continue
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
                                src_cols = payload_schema.get("columns") if isinstance(payload_schema, dict) else None
                                src_col_names = []
                                if isinstance(src_cols, list):
                                    src_col_names = [
                                        c.get("name") if isinstance(c, dict) else c for c in src_cols
                                    ]
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

                        # 2) node state hash + exec_key (centralized hashing)
                        norm = normalized_params_for_hash

                        # 3) execute (cache resolve already happened before node execution)
                        op = str(norm.get("op") or "")
                        primary_port = "in" if "in" in input_tables else (next(iter(input_tables.keys())) if input_tables else "in")
                        primary_cols = input_columns.get(primary_port, [])
                        primary_cols_set = set(primary_cols)
                        input_artifact_ids = [aid for _, aid in input_refs]

                        if op == "select":
                            expected_cols = [str(c) for c in ((norm.get("select") or {}).get("columns") or [])]
                            missing_cols = [c for c in expected_cols if c not in primary_cols_set]
                            if missing_cols:
                                raise ContractMismatchError(
                                    f"Transform payload schema mismatch: select references missing columns {missing_cols}",
                                    code="PAYLOAD_SCHEMA_MISMATCH",
                                    details=_contract_details(
                                        missing_columns=missing_cols,
                                        expected={"requiredColumns": _sorted_unique_strings(expected_cols)},
                                        actual={"availableColumns": _sorted_unique_strings(primary_cols)},
                                    ),
                                )
                        elif op == "rename":
                            rename_map = (norm.get("rename") or {}).get("map") or {}
                            expected_cols = [str(c) for c in rename_map.keys()]
                            missing_cols = [c for c in expected_cols if c not in primary_cols_set]
                            if missing_cols:
                                raise ContractMismatchError(
                                    f"Transform payload schema mismatch: rename references missing columns {missing_cols}",
                                    code="PAYLOAD_SCHEMA_MISMATCH",
                                    details=_contract_details(
                                        missing_columns=missing_cols,
                                        expected={"requiredColumns": _sorted_unique_strings(expected_cols)},
                                        actual={"availableColumns": _sorted_unique_strings(primary_cols)},
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
                                missing_cols = [c for c in expected_cols if c not in primary_cols_set]
                                if missing_cols:
                                    raise ContractMismatchError(
                                        f"Transform payload schema mismatch: derive references missing columns {missing_cols}",
                                        code="PAYLOAD_SCHEMA_MISMATCH",
                                        details=_contract_details(
                                            missing_columns=missing_cols,
                                            expected={"requiredColumns": _sorted_unique_strings(expected_cols)},
                                            actual={"availableColumns": _sorted_unique_strings(primary_cols)},
                                        ),
                                    )
                        elif op == "join":
                            join_spec = norm.get("join") or {}
                            with_node = str(join_spec.get("withNodeId") or "")
                            if len(input_refs) < 2:
                                raise ContractMismatchError(
                                    "Transform output contract mismatch: join requires two connected inputs",
                                    details=_contract_details(
                                        expected={"inputCount": 2},
                                        actual={"inputCount": int(len(input_refs))},
                                    ),
                                )
                            with_node_artifact = get_current_artifact(with_node)
                            if not with_node_artifact or with_node_artifact not in input_artifact_ids:
                                raise ContractMismatchError(
                                    "Transform output contract mismatch: join.withNodeId must be connected as an input",
                                    details=_contract_details(
                                        expected={"withNodeIdConnected": True},
                                        actual={
                                            "withNodeId": with_node,
                                            "connectedInputArtifactIds": sorted(input_artifact_ids),
                                        },
                                    ),
                                )
                            other_df = join_lookup.get(with_node)
                            other_cols = (
                                [str(c) for c in list(getattr(other_df, "columns", []))]
                                if other_df is not None
                                else []
                            )
                            other_cols_set = set(other_cols)
                            on_specs = join_spec.get("on") or []
                            left_expected = [
                                str(x.get("left"))
                                for x in on_specs
                                if isinstance(x, dict) and x.get("left") is not None
                            ]
                            right_expected = [
                                str(x.get("right"))
                                for x in on_specs
                                if isinstance(x, dict) and x.get("right") is not None
                            ]
                            missing_left = [c for c in left_expected if c not in primary_cols_set]
                            missing_right = [c for c in right_expected if c not in other_cols_set]
                            missing_cols = sorted(set(missing_left + missing_right))
                            if missing_cols:
                                raise ContractMismatchError(
                                    f"Transform payload schema mismatch: join references missing columns {missing_cols}",
                                    code="PAYLOAD_SCHEMA_MISMATCH",
                                    details=_contract_details(
                                        missing_columns=missing_cols,
                                        expected={
                                            "leftRequiredColumns": _sorted_unique_strings(left_expected),
                                            "rightRequiredColumns": _sorted_unique_strings(right_expected),
                                        },
                                        actual={
                                            "leftAvailableColumns": _sorted_unique_strings(primary_cols),
                                            "rightAvailableColumns": _sorted_unique_strings(other_cols),
                                        },
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

                        artifact = Artifact(
                            artifact_id=artifact_id,
                            node_kind=kind,
                            params_hash=node_state_hash,
                            upstream_ids=sorted([aid for _, aid in input_refs]),
                            created_at=datetime.now(timezone.utc),
                            execution_version=context.execution_version,
                            mime_type=res.mime_type,
                            port_type="table",
                            size_bytes=len(res.payload_bytes),
                            storage_uri=f"artifact://{artifact_id}",
                            payload_schema={
                                "schema_version": 1,
                                "type": "table",
                                "columns": [{"name": c, "dtype": "unknown"} for c in (res.meta.get("columns") or [])],
                            },
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
                    for upstream_id in upstream_ids:
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
                        upstream_artifact_ids=upstream_ids,
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
                        ports = (n.get("data", {}).get("ports", {}) or {})
                        out_contract = ports.get("out")  # "table" or "text"/"binary"/etc

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
                            mime_type = "application/octet-stream"
                        else:
                            raise RuntimeError(
                                f"Source output contract mismatch: unsupported out port '{out_contract}'"
                            )

                    elif kind == "llm":
                        ports = (n.get("data", {}).get("ports", {}) or {})
                        out_contract = ports.get("out") or "text"
                        output_cfg = params.get("output") if isinstance(params, dict) else {}
                        output_mode = output_cfg.get("mode") if isinstance(output_cfg, dict) else None

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
                            mime_type = (
                                "text/markdown; charset=utf-8"
                                if output_mode == "markdown"
                                else "text/plain; charset=utf-8"
                            )
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

                    artifact = Artifact(
                        artifact_id=artifact_id,
                        node_kind=kind,
                        params_hash=artifact_params_hash,
                        upstream_ids=sorted(upstream_ids),
                        created_at=datetime.now(timezone.utc),
                        execution_version=context.execution_version,
                        mime_type=mime_type,
                        port_type=((n.get("data", {}).get("ports", {}) or {}).get("out")),
                        size_bytes=len(payload_bytes),
                        storage_uri=f"artifact://{artifact_id}",
                        payload_schema=(
                            _source_payload_schema(
                                (n.get("data", {}).get("ports", {}) or {}).get("out"),
                                data_value,
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
                        ),
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
                                "missingColumns": error_details.get("missingColumns", []),
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
            async with global_sem:
                inflight_current += 1
                if inflight_current > peak_concurrency:
                    peak_concurrency = inflight_current
                if kind_sem is None:
                    try:
                        return await _execute_node(node_id)
                    except asyncio.CancelledError:
                        await context.bus.emit({
                            "type": "node_finished",
                            "runId": run_id,
                            "at": iso_now(),
                            "nodeId": node_id,
                            "status": "cancelled",
                            "execution_time_ms": max(0.0, (asyncio.get_running_loop().time() - t0) * 1000.0),
                        })
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
                        return {"ok": False, "cached": False}
                    finally:
                        inflight_current -= 1
                try:
                    async with kind_sem:
                        try:
                            return await _execute_node(
                                node_id,
                                cache_only=(node_id in plan.cache_only_nodes),
                            )
                        except asyncio.CancelledError:
                            await context.bus.emit({
                                "type": "node_finished",
                                "runId": run_id,
                                "at": iso_now(),
                                "nodeId": node_id,
                                "status": "cancelled",
                                "execution_time_ms": max(0.0, (asyncio.get_running_loop().time() - t0) * 1000.0),
                            })
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
