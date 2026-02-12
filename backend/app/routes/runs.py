import asyncio
from pprint import pformat
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ..runner.events import RunEventBus
from ..runner.run import run_graph

router = APIRouter()

# In-memory bus registry (Phase 2). Later replace with Redis etc.
BUSES: Dict[str, RunEventBus] = {}

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

class RunRequest(BaseModel):
    runFrom: Optional[str] = None
    graph: Dict[str, Any]  # PipelineGraphDTO shape from frontend

class RunCreated(BaseModel):
    runId: str

@router.post("", response_model=RunCreated)
async def create_run(req: RunRequest, bg: BackgroundTasks):
    
    print("RUN REQUEST BODY (server received):")
    try:
        print(json.dumps(req.model_dump(), indent=2)[:8000])
    except Exception:
        print(pformat(req)[:8000])
    
    run_id = str(uuid4())
    bus = RunEventBus(run_id)
    BUSES[run_id] = bus

    # start background execution
    bg.add_task(run_graph, run_id, req.graph, req.runFrom, bus)

    return RunCreated(runId=run_id)

@router.get("/{run_id}/events")
async def stream_events(run_id: str):
    bus = BUSES.get(run_id)
    if not bus:
        raise HTTPException(status_code=404, detail="Unknown runId")

    async def event_gen():
        # Send initial comment so proxies don't buffer
        yield ": connected\n\n"

        while True:
            evt = await bus.next_event()
            # SSE format: `data: <json>\n\n`
            payload = json.dumps(evt, separators=(",", ":"))
            yield f"data: {payload}\n\n"

            if evt.get("type") == "run_finished":
                break

    return StreamingResponse(event_gen(), media_type="text/event-stream")
