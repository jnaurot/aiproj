
import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, field_validator

from ..runner.nodes.transform import load_table_from_artifact_bytes

router = APIRouter()

# # In-memory bus registry (Phase 2). Later replace with Redis etc.
# BUSES: Dict[str, RunEventBus] = {}

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

class RunRequest(BaseModel):
    runFrom: Optional[str] = None
    graph: Dict[str, Any]  # PipelineGraphDTO shape from frontend

    @field_validator("graph")
    @classmethod
    def graph_must_have_nodes_edges(cls, v):
        if "nodes" not in v or "edges" not in v:
            raise ValueError("graph must include 'nodes' and 'edges'")
        return v
    
class RunCreated(BaseModel):
    runId: str


@router.get("")
async def list_runs(request: Request, include_deleted: bool = Query(default=False)):
    rt = request.app.state.runtime
    rows = await rt.list_runs(include_deleted=include_deleted)
    return {"runs": rows}


@router.post("", response_model=RunCreated)
async def create_run(req: RunRequest, request: Request):
    print("RUN REQUEST BODY (server received):")
    rt = request.app.state.runtime
    run_id = str(uuid4())
    rt.create_run(run_id)
    
    await rt.start_run(run_id, req.graph, req.runFrom)
    
    return RunCreated(runId=run_id)


@router.post("/{run_id}/cancel")
async def cancel_run(run_id: str, request: Request):
    rt = request.app.state.runtime
    h = rt.get_run(run_id)
    if not h:
        raise HTTPException(404, "Unknown runId")
    if h.task:
        h.task.cancel()
    return {"ok": True}


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

@router.get("/{run_id}/events")
async def stream_events(run_id: str, request: Request):
    rt = request.app.state.runtime
    handle = rt.get_run(run_id)
    if not handle:
        raise HTTPException(404, "Unknown runId")

    bus = handle.bus
    await bus.emit({"type": "debug_ping", "runId": run_id, "at": iso_now()})

    if not bus:
        raise HTTPException(status_code=404, detail="Unknown runId")

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
async def get_artifact(artifact_id: str, request: Request):
    rt = request.app.state.runtime
    store = rt.artifact_store
    if not await store.exists(artifact_id):
        raise HTTPException(404, "Artifact not found")

    art = await store.get(artifact_id)          # metadata object
    mime = getattr(art, "mime_type", None) or "application/octet-stream"
    stream = store.open_payload(artifact_id)
    return StreamingResponse(stream, media_type=mime)


@router.get("/artifacts/{artifact_id}/meta")
async def get_artifact_meta(artifact_id: str, request: Request):
    rt = request.app.state.runtime
    store = rt.artifact_store
    if not await store.exists(artifact_id):
        raise HTTPException(404, "Artifact not found")

    art = await store.get(artifact_id)
    return {
        "artifactId": art.artifact_id,
        "nodeKind": art.node_kind,
        "mimeType": art.mime_type,
        "sizeBytes": art.size_bytes,
        "createdAt": art.created_at.isoformat(),
        "paramsHash": art.params_hash,
        "upstreamCount": len(art.upstream_ids or []),
        "payloadSchema": art.payload_schema,
    }


@router.get("/artifacts/{artifact_id}/preview")
async def get_artifact_preview(
    artifact_id: str,
    request: Request,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
):
    rt = request.app.state.runtime
    store = rt.artifact_store
    if not await store.exists(artifact_id):
        raise HTTPException(404, "Artifact not found")

    art = await store.get(artifact_id)
    data = await store.read(artifact_id)
    mime = getattr(art, "mime_type", None) or "application/octet-stream"

    try:
        df = load_table_from_artifact_bytes(mime, data)
    except Exception as e:
        raise HTTPException(400, f"Artifact preview supports table-like payloads only: {e}")

    total_rows = int(len(df))
    page_df = df.iloc[offset : offset + limit]
    rows = page_df.where(page_df.notnull(), None).to_dict(orient="records")

    schema_cols = []
    payload_schema = art.payload_schema if isinstance(art.payload_schema, dict) else None
    if payload_schema and isinstance(payload_schema.get("columns"), list):
        for col in payload_schema["columns"]:
            if isinstance(col, dict):
                schema_cols.append(
                    {"name": str(col.get("name", "")), "type": str(col.get("type", "unknown"))}
                )
            else:
                schema_cols.append({"name": str(col), "type": "unknown"})
    if not schema_cols:
        schema_cols = [
            {"name": str(name), "type": str(dtype)}
            for name, dtype in zip(page_df.columns.tolist(), page_df.dtypes.tolist())
        ]

    return {
        "artifactId": artifact_id,
        "mimeType": mime,
        "offset": offset,
        "limit": limit,
        "totalRows": total_rows,
        "columns": schema_cols,
        "rows": rows,
    }


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
            "runId": rec.get("run_id", run_id),
            "status": rec.get("status", "unknown"),
            "error": None,
            "createdAt": rec.get("created_at"),
            "nodeStatus": {},
            "nodeOutputs": {},
        }
    if h.status == "deleted":
        raise HTTPException(404, "Unknown runId")

    return {
        "runId": h.run_id,
        "status": h.status,
        "error": h.error,
        "createdAt": h.created_at,
        "nodeStatus": h.node_status,
        "nodeOutputs": h.node_outputs,
    }
