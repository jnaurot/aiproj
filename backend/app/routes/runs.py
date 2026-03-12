
import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, field_validator

from ..graph_migrations import canonicalize_graph_payload
from ..runner.nodes.transform import load_table_from_artifact_bytes
from ..runner.node_state import build_exec_key, build_node_state_hash, build_source_fingerprint
from ..runner.run import _determinism_env_for_node, _normalized_params_for_exec_key

router = APIRouter()

# # In-memory bus registry (Phase 2). Later replace with Redis etc.
# BUSES: Dict[str, RunEventBus] = {}

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _alpha_input_label(idx: int) -> str:
    n = idx + 1
    out = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return f"Input {out}"


def _normalize_typed_field_type(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    if value in {"table", "json", "text", "binary", "embeddings", "unknown"}:
        return value
    if value in {"string", "str"}:
        return "text"
    if value in {"object", "array"}:
        return "json"
    if value in {"bytes", "bytea"}:
        return "binary"
    return "unknown"


def _native_type_from_dtype_text(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    if not value:
        return "unknown"
    if any(token in value for token in ("int", "int8", "int16", "int32", "int64")):
        return "int"
    if any(token in value for token in ("float", "double", "decimal", "numeric")):
        return "float"
    if "bool" in value:
        return "bool"
    if any(token in value for token in ("datetime", "timestamp", "date", "time")):
        return "datetime"
    if any(token in value for token in ("bytes", "bytea", "blob", "binary")):
        return "binary"
    if any(token in value for token in ("json", "dict", "list", "array", "struct", "map")):
        return "json"
    if value in {"string", "str", "object", "text"}:
        return "string"
    return "unknown"


def _native_type_from_value(value: Any) -> str:
    try:
        if pd.isna(value):
            return "unknown"
    except Exception:
        pass
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "binary"
    if isinstance(value, (dict, list)):
        return "json"
    if hasattr(value, "isoformat"):
        return "datetime"
    if isinstance(value, str):
        return "string"
    return "unknown"


def _typed_field_type_from_native(native_type: str) -> str:
    native = str(native_type or "unknown").strip().lower()
    if native == "json":
        return "json"
    if native == "binary":
        return "binary"
    if native in {"int", "float", "bool", "datetime", "string"}:
        return "text"
    return "unknown"


def _inferred_expected_schema_from_columns(
    columns: Any,
    *,
    first_row: Optional[Dict[str, Any]] = None,
    pandas_dtypes: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    fields: list[Dict[str, Any]] = []
    first_row = first_row if isinstance(first_row, dict) else {}
    pandas_dtypes = pandas_dtypes if isinstance(pandas_dtypes, dict) else {}
    if isinstance(columns, list):
        for col in columns:
            if isinstance(col, dict):
                name = str(col.get("name") or "").strip()
                col_type = col.get("type", col.get("dtype"))
            else:
                name = str(col or "").strip()
                col_type = "unknown"
            if not name:
                continue
            value_native = _native_type_from_value(first_row.get(name))
            dtype_native = _native_type_from_dtype_text(pandas_dtypes.get(name, col_type))
            native_type = value_native if value_native != "unknown" else dtype_native
            typed_type = _typed_field_type_from_native(native_type)
            fields.append(
                {
                    "name": name,
                    "type": typed_type if typed_type != "unknown" else _normalize_typed_field_type(col_type),
                    "nativeType": native_type,
                    "nullable": True,
                }
            )
    return {"type": "table", "fields": fields}


def _extract_builtin_environment(payload_schema: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(payload_schema, dict):
        return None
    env = payload_schema.get("builtin_environment")
    if not isinstance(env, dict):
        return None
    profile_id = str(env.get("profileId") or "").strip()
    source = str(env.get("source") or "").strip()
    install_target = str(env.get("installTarget") or "").strip()
    locked = str(env.get("locked") or "").strip()
    packages_raw = env.get("packages")
    packages: list[str] = []
    if isinstance(packages_raw, list):
        for pkg in packages_raw:
            if isinstance(pkg, str) and pkg.strip():
                packages.append(pkg.strip())
    if not profile_id and not source and not install_target and not packages and not locked:
        return None
    out = {
        "profileId": profile_id,
        "source": source,
        "installTarget": install_target,
        "packages": packages,
    }
    if locked:
        out["locked"] = locked
    return out


def _extract_dataset_lineage(
    *,
    artifact_id: str,
    upstream_ids: list[str],
    payload_schema: Optional[Dict[str, Any]],
    node_id: Optional[str],
    run_id: Optional[str],
    graph_id: Optional[str],
    exec_key: Optional[str],
) -> Dict[str, Any]:
    artifact_meta = (
        payload_schema.get("artifactMetadataV1")
        if isinstance(payload_schema, dict) and isinstance(payload_schema.get("artifactMetadataV1"), dict)
        else {}
    )
    lineage = artifact_meta.get("lineageV1") if isinstance(artifact_meta.get("lineageV1"), dict) else {}
    parent_ids = lineage.get("parentArtifactIds") if isinstance(lineage.get("parentArtifactIds"), list) else []
    snapshot_refs = lineage.get("snapshotRefs") if isinstance(lineage.get("snapshotRefs"), list) else []
    run_refs = lineage.get("runRefs") if isinstance(lineage.get("runRefs"), list) else []

    normalized_parent_ids = [str(a) for a in parent_ids if isinstance(a, str) and a.strip()]
    if not normalized_parent_ids:
        normalized_parent_ids = [str(a) for a in (upstream_ids or []) if isinstance(a, str) and a.strip()]

    normalized_snapshot_refs: list[Dict[str, str]] = []
    for snap in snapshot_refs:
        if not isinstance(snap, dict):
            continue
        sid = str(snap.get("snapshotId") or "").strip().lower()
        if not sid:
            continue
        normalized_snapshot_refs.append({"snapshotId": sid, "role": str(snap.get("role") or "ancestor")})

    normalized_run_refs: list[Dict[str, str]] = []
    for ref in run_refs:
        if not isinstance(ref, dict):
            continue
        aid = str(ref.get("artifactId") or "").strip()
        if not aid:
            continue
        normalized_run_refs.append(
            {
                "role": str(ref.get("role") or "producer"),
                "artifactId": aid,
                "runId": str(ref.get("runId") or ""),
                "graphId": str(ref.get("graphId") or ""),
                "nodeId": str(ref.get("nodeId") or ""),
                "execKey": str(ref.get("execKey") or ""),
            }
        )

    if not normalized_run_refs:
        normalized_run_refs = [
            {
                "role": "producer",
                "artifactId": str(artifact_id),
                "runId": str(run_id or ""),
                "graphId": str(graph_id or ""),
                "nodeId": str(node_id or ""),
                "execKey": str(exec_key or ""),
            }
        ]

    return {
        "schemaVersion": 1,
        "datasetVersionId": str(lineage.get("datasetVersionId") or artifact_id),
        "artifactId": str(artifact_id),
        "parentArtifactIds": sorted(set(normalized_parent_ids)),
        "snapshotRefs": normalized_snapshot_refs,
        "runRefs": normalized_run_refs,
    }



class RunRequest(BaseModel):
    graphId: str
    runFrom: Optional[str] = None
    runMode: Optional[str] = None
    cacheMode: Optional[str] = None
    graph: Dict[str, Any]  # PipelineGraphDTO shape from frontend

    @field_validator("graph")
    @classmethod
    def graph_must_have_nodes_edges(cls, v):
        if "nodes" not in v or "edges" not in v:
            raise ValueError("graph must include 'nodes' and 'edges'")
        return v

    @field_validator("graphId")
    @classmethod
    def graph_id_required(cls, v):
        if not isinstance(v, str) or not v.strip():
            raise ValueError("graphId is required")
        return v.strip()
    
    @field_validator("runMode")
    @classmethod
    def validate_run_mode(cls, v):
        if v is None:
            return v
        mode = str(v).strip().lower()
        if mode not in {"from_selected_onward", "selected_only"}:
            raise ValueError("runMode must be 'from_selected_onward' or 'selected_only'")
        return mode

    @field_validator("cacheMode")
    @classmethod
    def validate_cache_mode(cls, v):
        if v is None:
            return v
        mode = str(v).strip().lower()
        if mode not in {"default_on", "force_off", "force_on"}:
            raise ValueError("cacheMode must be 'default_on', 'force_off', or 'force_on'")
        return mode
    
class RunCreated(BaseModel):
    schemaVersion: int = 1
    runId: str
    graphId: str


class AcceptNodeParamsRequest(BaseModel):
    graph: Dict[str, Any]
    params: Dict[str, Any]


class CacheConfigRequest(BaseModel):
    enabled: Optional[bool] = None
    mode: Optional[str] = None

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, v):
        if v is None:
            return v
        m = str(v).strip().lower()
        if m not in {"default_on", "force_off", "force_on"}:
            raise ValueError("mode must be one of: default_on, force_off, force_on")
        return m

class ResolveSourceRequest(BaseModel):
    graphId: str
    graph: Dict[str, Any]
    nodeId: str
    params: Optional[Dict[str, Any]] = None


class DbSchemaRequest(BaseModel):
    connectionRef: str

    @field_validator("connectionRef")
    @classmethod
    def validate_connection_ref(cls, v):
        ref = str(v or "").strip()
        if not ref:
            raise ValueError("connectionRef is required")
        return ref


def _duckdb_path_from_ref(connection_ref: str) -> str:
    ref = str(connection_ref or "").strip()
    if ref == ":memory:":
        return ":memory:"
    if ref.startswith("duckdb:///"):
        return ref.replace("duckdb:///", "", 1)
    raise ValueError("Tool DB requires DuckDB connectionRef in format duckdb:///... or :memory:")


def _normalize_duckdb_type(native_type: str) -> str:
    t = str(native_type or "").upper()
    if "BOOL" in t:
        return "bool"
    if any(x in t for x in ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT")):
        return "int"
    if any(x in t for x in ("FLOAT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC")):
        return "float"
    if any(x in t for x in ("DATE", "TIME", "TIMESTAMP", "INTERVAL")):
        return "date"
    if any(x in t for x in ("JSON",)):
        return "json"
    if any(x in t for x in ("BLOB", "BYTEA", "BINARY", "VARBINARY")):
        return "binary"
    if any(x in t for x in ("CHAR", "TEXT", "VARCHAR", "UUID")):
        return "string"
    return "unknown"


@router.get("")
async def list_runs(request: Request, include_deleted: bool = Query(default=False)):
    rt = request.app.state.runtime
    rows = await rt.list_runs(include_deleted=include_deleted)
    return {"schemaVersion": 1, "runs": rows}


@router.get("/cache/config")
async def get_cache_config(request: Request):
    rt = request.app.state.runtime
    mode = (
        rt.get_global_cache_mode()
        if hasattr(rt, "get_global_cache_mode")
        else ("default_on" if bool(getattr(rt, "global_cache_enabled", True)) else "force_off")
    )
    enabled = mode != "force_off"
    return {"schemaVersion": 1, "enabled": enabled, "mode": mode}


@router.get("/diagnostics")
async def get_run_diagnostics(request: Request):
    rt = request.app.state.runtime
    get_diag = getattr(rt, "get_diagnostics", None)
    if not callable(get_diag):
        raise HTTPException(404, "diagnostics unavailable")
    return get_diag()


@router.put("/cache/config")
async def set_cache_config(req: CacheConfigRequest, request: Request):
    rt = request.app.state.runtime
    mode = req.mode
    if mode is None:
        mode = "default_on" if bool(req.enabled if req.enabled is not None else True) else "force_off"
    if hasattr(rt, "set_global_cache_mode"):
        rt.set_global_cache_mode(mode)
    else:
        rt.global_cache_enabled = mode != "force_off"
    resolved_mode = (
        rt.get_global_cache_mode()
        if hasattr(rt, "get_global_cache_mode")
        else ("default_on" if bool(getattr(rt, "global_cache_enabled", True)) else "force_off")
    )
    return {"schemaVersion": 1, "enabled": resolved_mode != "force_off", "mode": resolved_mode}


@router.post("", response_model=RunCreated)
async def create_run(req: RunRequest, request: Request):
    print("RUN REQUEST BODY (server received):")
    rt = request.app.state.runtime
    if req.cacheMode and hasattr(rt, "set_global_cache_mode"):
        rt.set_global_cache_mode(str(req.cacheMode))
    run_id = str(uuid4())
    rt.create_run(run_id)
    
    if req.runMode == "selected_only" and not req.runFrom:
        raise HTTPException(400, "runMode='selected_only' requires runFrom")

    graph_id = str(req.graphId)
    canonical_graph, _ = canonicalize_graph_payload(req.graph)
    await rt.start_run(run_id, canonical_graph, req.runFrom, run_mode=req.runMode, graph_id=graph_id)
    
    return RunCreated(schemaVersion=1, runId=run_id, graphId=graph_id)


@router.post("/{run_id}/cancel")
async def cancel_run(run_id: str, request: Request):
    rt = request.app.state.runtime
    h = rt.get_run(run_id)
    if not h:
        raise HTTPException(404, "Unknown runId")
    result = await rt.request_cancel(run_id)
    if not result.get("found"):
        raise HTTPException(404, "Unknown runId")
    if result.get("cancelRequested"):
        return {
            "runId": run_id,
            "status": result.get("status", "cancel_requested"),
            "cancelRequested": True,
        }
    raise HTTPException(
        409,
        detail={
            "runId": run_id,
            "status": result.get("status", "unknown"),
            "cancelRequested": False,
        },
    )


@router.post("/{run_id}/nodes/{node_id}/accept-params")
async def accept_node_params(run_id: str, node_id: str, req: AcceptNodeParamsRequest, request: Request):
    rt = request.app.state.runtime
    h = rt.get_run(run_id)
    if not h:
        raise HTTPException(404, "Unknown runId")
    graph_id = str(getattr(h, "graph_id", "") or "").strip()
    canonical_graph, _ = canonicalize_graph_payload(req.graph)
    try:
        out = await rt.accept_node_params(
            run_id=run_id,
            graph=canonical_graph,
            node_id=node_id,
            params=req.params,
        )
        return out
    except ValueError as ex:
        raise HTTPException(400, str(ex))
    except RuntimeError as ex:
        raise HTTPException(409, str(ex))

@router.post("/resolve/source")
async def resolve_source_node(req: ResolveSourceRequest, request: Request):
    graph_id = str(req.graphId or "").strip()
    if not graph_id:
        raise HTTPException(400, "graphId is required")

    canonical_graph, _ = canonicalize_graph_payload(req.graph)
    nodes = (canonical_graph or {}).get("nodes", []) if isinstance(canonical_graph, dict) else []
    target = None
    for n in nodes:
        if str(n.get("id")) == str(req.nodeId):
            target = n
            break
    if not isinstance(target, dict):
        raise HTTPException(404, "node not found")

    kind = str(((target.get("data") or {}).get("kind")) or "")
    source_kind = str(((target.get("data") or {}).get("sourceKind")) or "file")
    if kind != "source" or source_kind != "file":
        raise HTTPException(400, "resolve/source supports source:file nodes only")

    params_raw = dict(((target.get("data") or {}).get("params")) or {})
    if isinstance(req.params, dict):
        params_raw.update(req.params)

    normalized = _normalized_params_for_exec_key(
        kind="source",
        node=target,
        params=params_raw,
    )
    source_cache_enabled = bool(normalized.get("cache_enabled", True))
    runtime_cache_mode = (
        request.app.state.runtime.get_global_cache_mode()
        if hasattr(request.app.state.runtime, "get_global_cache_mode")
        else ("default_on" if bool(getattr(request.app.state.runtime, "global_cache_enabled", True)) else "force_off")
    )
    runtime_cache_enabled = runtime_cache_mode != "force_off"
    determinism_env = _determinism_env_for_node("source", normalized)
    source_fingerprint = build_source_fingerprint(target, normalized)
    node_state_hash = build_node_state_hash(
        node=target,
        params=normalized,
        execution_version="v1",
        source_fingerprint=source_fingerprint,
    )
    exec_key = build_exec_key(
        graph_id=graph_id,
        node_id=str(req.nodeId),
        node_kind="source",
        node_state_hash=node_state_hash,
        upstream_artifact_ids=[],
        input_refs=[],
        determinism_env=determinism_env,
        execution_version="v1",
        node_impl_version="SOURCE@1",
    )

    store = request.app.state.runtime.artifact_store
    artifact_id: Optional[str] = None
    artifact_meta: Optional[Dict[str, Any]] = None
    if runtime_cache_enabled and source_cache_enabled and await store.exists(exec_key):
        art = await store.get(exec_key)
        if getattr(art, "graph_id", None) and str(art.graph_id) == graph_id:
            artifact_id = exec_key
            artifact_meta = {
                "artifactId": art.artifact_id,
                "mimeType": art.mime_type,
                "payloadType": str(getattr(art, "payload_type", "") or (art.payload_schema or {}).get("type") or ""),
                "sizeBytes": art.size_bytes,
                "createdAt": art.created_at.isoformat() if getattr(art, "created_at", None) else None,
                "contentHash": art.content_hash,
            }

    print(
        "[resolve-source] graphId=%s nodeId=%s snapshotId=%s global_cache_mode=%s cache_enabled=%s exec_key=%s artifact=%s"
        % (
            graph_id,
            str(req.nodeId),
            str(normalized.get("snapshot_id") or ""),
            str(runtime_cache_mode),
            str(source_cache_enabled).lower(),
            exec_key,
            str(artifact_id or ""),
        )
    )
    return {
        "graphId": graph_id,
        "nodeId": str(req.nodeId),
        "execKey": exec_key,
        "artifactId": artifact_id,
        "cacheHit": bool(artifact_id),
        "artifact": artifact_meta,
        "snapshotId": normalized.get("snapshot_id"),
    }


@router.post("/tools/db/schema")
async def get_db_schema(req: DbSchemaRequest):
    try:
        import duckdb
    except Exception:
        raise HTTPException(500, "duckdb is required for Tool DB schema discovery")

    try:
        db_path = _duckdb_path_from_ref(req.connectionRef)
        if db_path != ":memory:":
            db_path = os.path.abspath(db_path)
    except ValueError as ex:
        raise HTTPException(400, str(ex))

    try:
        con = duckdb.connect(database=db_path)
        try:
            table_rows = con.execute(
                """
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                  AND table_type IN ('BASE TABLE', 'VIEW')
                ORDER BY table_schema, table_name
                """
            ).fetchall()

            col_rows = con.execute(
                """
                SELECT
                    table_schema,
                    table_name,
                    column_name,
                    data_type,
                    is_nullable,
                    ordinal_position
                FROM information_schema.columns
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY table_schema, table_name, ordinal_position
                """
            ).fetchall()
        finally:
            con.close()

        col_map: Dict[tuple[str, str], list[Dict[str, Any]]] = {}
        for schema_name, table_name, column_name, data_type, is_nullable, ordinal_pos in col_rows:
            key = (str(schema_name), str(table_name))
            col_map.setdefault(key, []).append(
                {
                    "name": str(column_name),
                    "normalizedType": _normalize_duckdb_type(str(data_type)),
                    "nativeType": str(data_type),
                    "nullable": str(is_nullable).upper() == "YES",
                    "ordinal": int(ordinal_pos),
                }
            )

        tables = []
        for schema_name, table_name, table_type in table_rows:
            key = (str(schema_name), str(table_name))
            kind = "view" if str(table_type).upper() == "VIEW" else "table"
            tables.append(
                {
                    "schema": str(schema_name),
                    "name": str(table_name),
                    "kind": kind,
                    "columns": col_map.get(key, []),
                }
            )

        return {
            "schemaVersion": 1,
            "connectionRef": req.connectionRef,
            "tables": tables,
        }
    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(500, f"DB schema discovery failed: {str(ex)}")


@router.delete("/{run_id}")
async def delete_run(
    run_id: str,
    request: Request,
    mode: str = Query(default="soft"),
    gc: str = Query(default="none"),
):
    rt = request.app.state.runtime
    mode_norm = (mode or "soft").lower()
    gc_norm = (gc or "none").lower()
    if mode_norm not in ("soft", "hard"):
        raise HTTPException(400, "mode must be 'soft' or 'hard'")
    if gc_norm not in ("none", "unreferenced"):
        raise HTTPException(400, "gc must be 'none' or 'unreferenced'")
    if mode_norm == "soft" and gc_norm != "none":
        raise HTTPException(400, "gc is only valid for mode=hard")

    result = await rt.delete_run(run_id, mode=mode_norm, gc=gc_norm)
    if not result.get("runDeleted", False):
        raise HTTPException(404, "Unknown runId")
    return result


@router.delete("/{run_id}/events")
async def prune_run_events(
    run_id: str,
    request: Request,
    keep_last: int = Query(..., ge=0),
    dry_run: bool = Query(default=True),
):
    rt = request.app.state.runtime
    rec = await rt.artifact_store.get_run(run_id)
    handle = rt.get_run(run_id)
    if not rec and not handle:
        raise HTTPException(404, "Unknown runId")

    result = await rt.prune_events(keep_last=keep_last, dry_run=dry_run, run_id=run_id)
    return {
        "runId": run_id,
        "keep_last": int(result.get("keep_last", keep_last)),
        "dry_run": bool(result.get("dry_run", dry_run)),
        "rows_deleted": int(result.get("rows_deleted", 0)),
        "runs_affected": int(result.get("runs_affected", 0)),
        "oldest_remaining_event_id": result.get("oldest_remaining_event_id"),
    }

@router.get("/{run_id}/events")
async def stream_events(
    run_id: str,
    request: Request,
    after_id: int = Query(default=0, ge=0),
    limit: int = Query(default=500, ge=1, le=5000),
):
    rt = request.app.state.runtime
    handle = rt.get_run(run_id)
    rec = await rt.artifact_store.get_run(run_id)
    if not handle and not rec:
        raise HTTPException(404, "Unknown runId")

    accept = (request.headers.get("accept") or "").lower()
    wants_sse = "text/event-stream" in accept

    # Replay mode: persisted events in deterministic order.
    if not wants_sse:
        rows = await rt.list_run_events(run_id, after_id=after_id, limit=limit)
        next_after_id = rows[-1]["id"] if rows else after_id
        return {
            "runId": run_id,
            "afterId": after_id,
            "limit": limit,
            "events": rows,
            "nextAfterId": next_after_id,
        }

    if not handle or not handle.bus:
        raise HTTPException(404, "Run is not active for live streaming; use replay mode")
    bus = handle.bus

    async def event_gen():
        # Send initial comment so proxies don't buffer
        yield ": connected\n\n"

        try:
            while True:
                evt = await bus.next_event()
                # SSE format: `data: <json>\n\n`
                payload = json.dumps(evt, separators=(",", ":"))
                yield f"data: {payload}\n\n"

                if evt.get("type") == "run_finished":
                    break
        except asyncio.CancelledError:
            return
    return StreamingResponse(event_gen(), media_type="text/event-stream")

@router.get("/artifacts/{artifact_id}")
async def get_artifact(artifact_id: str, request: Request, graphId: str = Query(...)):
    rt = request.app.state.runtime
    store = rt.artifact_store
    if not await store.exists(artifact_id):
        raise HTTPException(404, "Artifact not found")

    art = await store.get(artifact_id)          # metadata object
    if not getattr(art, "graph_id", None) or str(art.graph_id) != str(graphId):
        raise HTTPException(404, "Artifact not found")
    mime = getattr(art, "mime_type", None) or "application/octet-stream"
    stream = store.open_payload(artifact_id)
    return StreamingResponse(stream, media_type=mime)


@router.get("/artifacts/{artifact_id}/meta")
async def get_artifact_meta(artifact_id: str, request: Request, graphId: str = Query(...)):
    rt = request.app.state.runtime
    store = rt.artifact_store
    if not await store.exists(artifact_id):
        raise HTTPException(404, "Artifact not found")

    art = await store.get(artifact_id)
    if not getattr(art, "graph_id", None) or str(art.graph_id) != str(graphId):
        raise HTTPException(404, "Artifact not found")
    upstream_ids = art.upstream_ids or []
    payload_schema = art.payload_schema if isinstance(art.payload_schema, dict) else None
    artifact_meta = (
        payload_schema.get("artifactMetadataV1")
        if isinstance(payload_schema, dict) and isinstance(payload_schema.get("artifactMetadataV1"), dict)
        else {}
    )
    component_meta = artifact_meta.get("component") if isinstance(artifact_meta.get("component"), dict) else None
    builtin_env = _extract_builtin_environment(payload_schema)
    producer = {
        "nodeId": art.node_id,
        "runId": art.run_id,
        "graphId": art.graph_id,
        "execKey": art.exec_key,
    }
    if component_meta:
        producer["component"] = component_meta
        producer["aliasNodeId"] = str(component_meta.get("instanceNodeId") or art.node_id or "")
    lineage = _extract_dataset_lineage(
        artifact_id=art.artifact_id,
        upstream_ids=upstream_ids,
        payload_schema=payload_schema,
        node_id=art.node_id,
        run_id=art.run_id,
        graph_id=art.graph_id,
        exec_key=art.exec_key,
    )
    return {
        "schemaVersion": 1,
        "artifactId": art.artifact_id,
        "datasetVersionId": lineage.get("datasetVersionId"),
        "nodeKind": art.node_kind,
        "payloadType": str(getattr(art, "payload_type", "") or (art.payload_schema or {}).get("type") or ""),
        "mimeType": art.mime_type,
        "sizeBytes": art.size_bytes,
        "contentHash": art.content_hash,
        "createdAt": art.created_at.isoformat(),
        "paramsHash": art.params_hash,
        "upstreamCount": len(upstream_ids),
        "upstreamArtifactIds": upstream_ids,
        "inputArtifactIds": upstream_ids,
        "inputRefs": [
            {"artifactId": up_id, "label": _alpha_input_label(i)}
            for i, up_id in enumerate(upstream_ids)
        ],
        "producerNodeId": art.node_id,
        "producerRunId": art.run_id,
        "graphId": art.graph_id,
        "producerExecKey": art.exec_key,
        "producer": producer,
        "lineage": lineage,
        "payloadSchema": payload_schema,
        "builtinEnvironment": builtin_env,
    }


@router.get("/artifacts/{artifact_id}/consumers")
async def get_artifact_consumers(
    artifact_id: str,
    request: Request,
    graphId: str = Query(...),
    limit: int = Query(default=50, ge=1, le=500),
):
    rt = request.app.state.runtime
    store = rt.artifact_store
    if not await store.exists(artifact_id):
        raise HTTPException(404, "Artifact not found")
    art = await store.get(artifact_id)
    if not getattr(art, "graph_id", None) or str(art.graph_id) != str(graphId):
        raise HTTPException(404, "Artifact not found")

    get_fn = getattr(store, "get_consumers", None)
    if not callable(get_fn):
        return {"artifactId": artifact_id, "consumers": []}

    consumers = await get_fn(artifact_id, limit=limit)
    for c in consumers:
        node_id = c.get("consumerNodeId")
        run = c.get("consumerRunId")
        c["label"] = f"{node_id} ({run})" if node_id and run else str(node_id or run or "")
    return {"artifactId": artifact_id, "consumers": consumers}


@router.get("/artifacts/{artifact_id}/lineage")
async def get_artifact_lineage(
    artifact_id: str,
    request: Request,
    graphId: str = Query(...),
    depth: int = Query(default=2, ge=0, le=6),
):
    rt = request.app.state.runtime
    store = rt.artifact_store
    if not await store.exists(artifact_id):
        raise HTTPException(404, "Artifact not found")
    root_art = await store.get(artifact_id)
    if not getattr(root_art, "graph_id", None) or str(root_art.graph_id) != str(graphId):
        raise HTTPException(404, "Artifact not found")

    visited: set[str] = set()

    async def _build(aid: str, d: int):
        if aid in visited:
            return {"artifactId": aid, "cycle": True}
        if not await store.exists(aid):
            return {"artifactId": aid, "missing": True}

        visited.add(aid)
        art = await store.get(aid)
        if not getattr(art, "graph_id", None) or str(art.graph_id) != str(graphId):
            return {"artifactId": aid, "missing": True}
        payload_schema = art.payload_schema if isinstance(art.payload_schema, dict) else None
        artifact_meta = (
            payload_schema.get("artifactMetadataV1")
            if isinstance(payload_schema, dict) and isinstance(payload_schema.get("artifactMetadataV1"), dict)
            else {}
        )
        component_meta = artifact_meta.get("component") if isinstance(artifact_meta.get("component"), dict) else None
        builtin_env = _extract_builtin_environment(payload_schema)
        producer = {
            "nodeId": art.node_id,
            "runId": art.run_id,
            "graphId": art.graph_id,
            "execKey": art.exec_key,
        }
        if component_meta:
            producer["component"] = component_meta
            producer["aliasNodeId"] = str(component_meta.get("instanceNodeId") or art.node_id or "")
        lineage = _extract_dataset_lineage(
            artifact_id=art.artifact_id,
            upstream_ids=art.upstream_ids or [],
            payload_schema=payload_schema,
            node_id=art.node_id,
            run_id=art.run_id,
            graph_id=art.graph_id,
            exec_key=art.exec_key,
        )
        node = {
            "artifactId": art.artifact_id,
            "datasetVersionId": lineage.get("datasetVersionId"),
            "nodeKind": art.node_kind,
            "payloadType": str(getattr(art, "payload_type", "") or (art.payload_schema or {}).get("type") or ""),
            "mimeType": art.mime_type,
            "sizeBytes": art.size_bytes,
            "createdAt": art.created_at.isoformat(),
            "payloadSchema": payload_schema,
            "producer": producer,
            "inputArtifactIds": art.upstream_ids or [],
            "inputRefs": [
                {"artifactId": up_id, "label": _alpha_input_label(i)}
                for i, up_id in enumerate(art.upstream_ids or [])
            ],
            "lineage": lineage,
            "builtinEnvironment": builtin_env,
        }
        if d <= 0:
            return node
        node["inputs"] = [await _build(up_id, d - 1) for up_id in (art.upstream_ids or [])]
        return node

    return {
        "schemaVersion": 1,
        "artifactId": artifact_id,
        "depth": depth,
        "lineage": await _build(artifact_id, depth),
    }


@router.get("/datasets/{dataset_version_id}")
async def get_dataset_version(
    dataset_version_id: str,
    request: Request,
    graphId: str = Query(...),
):
    rt = request.app.state.runtime
    store = rt.artifact_store
    dataset_id = str(dataset_version_id or "").strip()
    if not dataset_id:
        raise HTTPException(400, "dataset_version_id is required")
    if not await store.exists(dataset_id):
        raise HTTPException(404, "Dataset version not found")
    art = await store.get(dataset_id)
    if not getattr(art, "graph_id", None) or str(art.graph_id) != str(graphId):
        raise HTTPException(404, "Dataset version not found")
    payload_schema = art.payload_schema if isinstance(art.payload_schema, dict) else None
    lineage = _extract_dataset_lineage(
        artifact_id=art.artifact_id,
        upstream_ids=art.upstream_ids or [],
        payload_schema=payload_schema,
        node_id=art.node_id,
        run_id=art.run_id,
        graph_id=art.graph_id,
        exec_key=art.exec_key,
    )
    return {
        "schemaVersion": 1,
        "datasetVersionId": lineage.get("datasetVersionId"),
        "artifactId": art.artifact_id,
        "nodeKind": art.node_kind,
        "payloadType": str(getattr(art, "payload_type", "") or (art.payload_schema or {}).get("type") or ""),
        "mimeType": art.mime_type,
        "createdAt": art.created_at.isoformat(),
        "producer": {
            "nodeId": art.node_id,
            "runId": art.run_id,
            "graphId": art.graph_id,
            "execKey": art.exec_key,
        },
        "lineage": lineage,
        "payloadSchema": payload_schema,
    }


@router.get("/artifacts/{artifact_id}/preview")
async def get_artifact_preview(
    artifact_id: str,
    request: Request,
    graphId: str = Query(...),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
):
    rt = request.app.state.runtime
    store = rt.artifact_store
    if not await store.exists(artifact_id):
        raise HTTPException(404, "Artifact not found")

    art = await store.get(artifact_id)
    if not getattr(art, "graph_id", None) or str(art.graph_id) != str(graphId):
        raise HTTPException(404, "Artifact not found")
    data = await store.read(artifact_id)
    mime = getattr(art, "mime_type", None) or "application/octet-stream"

    warning: Optional[str] = None
    try:
        df = load_table_from_artifact_bytes(mime, data)
    except Exception as e:
        warning = f"table_parse_failed: {e}"
        # Fallback preview for non-table payloads to avoid hard 400s in UI.
        ct = str(mime or "").lower()
        if "application/json" in ct:
            try:
                parsed = json.loads(data.decode("utf-8"))
                if isinstance(parsed, list):
                    rows = [r for r in parsed if isinstance(r, dict)]
                    if rows:
                        cols = sorted({str(k) for r in rows for k in r.keys()})
                        page_rows = rows[offset : offset + limit]
                        json_df = pd.DataFrame(rows)
                        first_row = (
                            json_df.iloc[0].where(json_df.iloc[0].notna(), None).to_dict()
                            if len(json_df) > 0
                            else {}
                        )
                        dtype_map = {
                            str(name): str(dtype)
                            for name, dtype in zip(json_df.columns.tolist(), json_df.dtypes.tolist())
                        }
                        return {
                            "artifactId": artifact_id,
                            "mimeType": mime,
                            "offset": offset,
                            "limit": limit,
                            "totalRows": len(rows),
                            "columns": [{"name": c, "type": "unknown"} for c in cols],
                            "rows": page_rows,
                            "inferredExpectedSchema": _inferred_expected_schema_from_columns(
                                [{"name": c, "type": "unknown"} for c in cols],
                                first_row=first_row,
                                pandas_dtypes=dtype_map,
                            ),
                            "warning": warning,
                        }
                if isinstance(parsed, dict):
                    json_cols = [{"name": str(k), "type": "unknown"} for k in parsed.keys()]
                    json_df = pd.DataFrame([parsed])
                    first_row = (
                        json_df.iloc[0].where(json_df.iloc[0].notna(), None).to_dict()
                        if len(json_df) > 0
                        else {}
                    )
                    dtype_map = {
                        str(name): str(dtype)
                        for name, dtype in zip(json_df.columns.tolist(), json_df.dtypes.tolist())
                    }
                    return {
                        "artifactId": artifact_id,
                        "mimeType": mime,
                        "offset": 0,
                        "limit": 1,
                        "totalRows": 1,
                        "columns": json_cols,
                        "rows": [parsed],
                        "inferredExpectedSchema": _inferred_expected_schema_from_columns(
                            json_cols,
                            first_row=first_row,
                            pandas_dtypes=dtype_map,
                        ),
                        "warning": warning,
                    }
            except Exception:
                pass

        try:
            text = data.decode("utf-8", errors="replace")
        except Exception:
            text = ""
        lines = text.splitlines() if text else []
        page_lines = lines[offset : offset + limit]
        return {
            "artifactId": artifact_id,
            "mimeType": mime,
            "offset": offset,
            "limit": limit,
            "totalRows": len(lines),
            "columns": [{"name": "text", "type": "string"}],
            "rows": [{"text": line} for line in page_lines],
            "inferredExpectedSchema": _inferred_expected_schema_from_columns(
                [{"name": "text", "type": "string"}]
            ),
            "warning": warning,
        }

    total_rows = int(len(df))
    page_df = df.iloc[offset : offset + limit]
    rows = page_df.where(page_df.notnull(), None).to_dict(orient="records")

    schema_cols = []
    payload_schema = art.payload_schema if isinstance(art.payload_schema, dict) else None
    if payload_schema and isinstance(payload_schema.get("columns"), list):
        for col in payload_schema["columns"]:
            if isinstance(col, dict):
                schema_cols.append(
                    {
                        "name": str(col.get("name", "")),
                        "type": str(col.get("dtype", col.get("type", "unknown"))),
                    }
                )
            else:
                schema_cols.append({"name": str(col), "type": "unknown"})
    if not schema_cols:
        schema_cols = [
            {"name": str(name), "type": str(dtype)}
            for name, dtype in zip(page_df.columns.tolist(), page_df.dtypes.tolist())
        ]

    out = {
        "artifactId": artifact_id,
        "mimeType": mime,
        "offset": offset,
        "limit": limit,
        "totalRows": total_rows,
        "columns": schema_cols,
        "rows": rows,
        "inferredExpectedSchema": _inferred_expected_schema_from_columns(
            schema_cols,
            first_row=(
                page_df.iloc[0].where(page_df.iloc[0].notna(), None).to_dict()
                if len(page_df) > 0
                else {}
            ),
            pandas_dtypes={
                str(name): str(dtype)
                for name, dtype in zip(df.columns.tolist(), df.dtypes.tolist())
            },
        ),
    }
    if warning:
        out["warning"] = warning
    return out


@router.get("/{run_id}")
async def get_run(run_id: str, request: Request):
    rt = request.app.state.runtime
    h = rt.get_run(run_id)
    if not h:
        rec = await rt.artifact_store.get_run(run_id)
        if not rec:
            raise HTTPException(404, "Unknown runId")
        if rec.get("status") == "deleted":
            raise HTTPException(404, "Unknown runId")
        return {
            "schemaVersion": 1,
            "runId": rec.get("run_id", run_id),
            "graphId": None,
            "status": rec.get("status", "unknown"),
            "error": None,
            "createdAt": rec.get("created_at"),
            "nodeStatus": {},
            "nodeOutputs": {},
            "nodeBindings": {},
        }
    if h.status == "deleted":
        raise HTTPException(404, "Unknown runId")

    return {
        "schemaVersion": 1,
        "runId": h.run_id,
        "graphId": h.graph_id,
        "status": h.status,
        "error": h.error,
        "createdAt": h.created_at,
        "nodeStatus": h.node_status,
        "nodeOutputs": h.node_outputs,
        "nodeBindings": h.node_bindings,
    }
