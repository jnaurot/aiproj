
import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, field_validator

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

    run_id = await rt.resolve_artifact_owner(artifact_id)
    if not run_id:
        raise HTTPException(404, "Artifact not found")

    handle = rt.get_run(run_id)
    if not handle:
        raise HTTPException(404, "Artifact owner run not found")

    store = handle.artifact_store
    if not await store.exists(artifact_id):
        raise HTTPException(404, "Artifact not found")

    meta = await store.get_meta(artifact_id)  # or store.get(...)
    data = await store.read(artifact_id)

    mime = (meta.get("mime_type") if isinstance(meta, dict) else None) or "application/octet-stream"
    return Response(content=data, media_type=mime)

@router.get("/{run_id}")
async def get_run(run_id: str, request: Request):
    rt = request.app.state.runtime
    h = rt.get_run(run_id)
    if not h:
        raise HTTPException(404, "Unknown runId")

    return {
        "runId": h.run_id,
        "status": h.status,
        "error": h.error,
        "createdAt": h.created_at,
        "nodeStatus": h.node_status,
        "nodeOutputs": h.node_outputs,
    }