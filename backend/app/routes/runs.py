
import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, field_validator

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


def _write_graph_revision_v2(
    request: Request,
    *,
    graph_id: str,
    graph: Dict[str, Any],
    message: str,
) -> Optional[str]:
    """
    V2 graph-revision write-through hook.
    Best effort only; run endpoint behavior must not fail because of revision-store issues.
    """
    store = getattr(request.app.state, "graph_revisions", None)
    if store is None:
        return None
    try:
        revision = store.create_revision(
            graph_id=graph_id,
            graph=graph,
            message=message,
            schema_version=1,
        )
        print(
            "[graph-v2-write] graphId=%s revisionId=%s message=%s"
            % (graph_id, revision.revision_id, message)
        )
        return str(revision.revision_id)
    except Exception as ex:
        # Best effort only; never fail existing endpoint behavior.
        print(
            "[graph-v2-write] skipped graphId=%s reason=%s"
            % (graph_id, repr(ex))
        )
        return None

class RunRequest(BaseModel):
    graphId: str
    runFrom: Optional[str] = None
    runMode: Optional[str] = None
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
    run_id = str(uuid4())
    rt.create_run(run_id)
    
    if req.runMode == "selected_only" and not req.runFrom:
        raise HTTPException(400, "runMode='selected_only' requires runFrom")

    graph_id = str(req.graphId)
    _write_graph_revision_v2(
        request,
        graph_id=graph_id,
        graph=req.graph,
        message="create_run",
    )
    await rt.start_run(run_id, req.graph, req.runFrom, run_mode=req.runMode, graph_id=graph_id)
    
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
    if graph_id:
        _write_graph_revision_v2(
            request,
            graph_id=graph_id,
            graph=req.graph,
            message=f"accept_params:{node_id}",
        )
    try:
        out = await rt.accept_node_params(
            run_id=run_id,
            graph=req.graph,
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

    nodes = (req.graph or {}).get("nodes", []) if isinstance(req.graph, dict) else []
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
                "portType": art.port_type,
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
    producer = {
        "nodeId": art.node_id,
        "runId": art.run_id,
        "graphId": art.graph_id,
        "execKey": art.exec_key,
    }
    if component_meta:
        producer["component"] = component_meta
        producer["aliasNodeId"] = str(component_meta.get("instanceNodeId") or art.node_id or "")
    return {
        "schemaVersion": 1,
        "artifactId": art.artifact_id,
        "nodeKind": art.node_kind,
        "portType": art.port_type,
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
        "payloadSchema": payload_schema,
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
        producer = {
            "nodeId": art.node_id,
            "runId": art.run_id,
            "graphId": art.graph_id,
            "execKey": art.exec_key,
        }
        if component_meta:
            producer["component"] = component_meta
            producer["aliasNodeId"] = str(component_meta.get("instanceNodeId") or art.node_id or "")
        node = {
            "artifactId": art.artifact_id,
            "nodeKind": art.node_kind,
            "portType": art.port_type,
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
                        return {
                            "artifactId": artifact_id,
                            "mimeType": mime,
                            "offset": offset,
                            "limit": limit,
                            "totalRows": len(rows),
                            "columns": [{"name": c, "type": "unknown"} for c in cols],
                            "rows": page_rows,
                            "warning": warning,
                        }
                if isinstance(parsed, dict):
                    return {
                        "artifactId": artifact_id,
                        "mimeType": mime,
                        "offset": 0,
                        "limit": 1,
                        "totalRows": 1,
                        "columns": [{"name": str(k), "type": "unknown"} for k in parsed.keys()],
                        "rows": [parsed],
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
