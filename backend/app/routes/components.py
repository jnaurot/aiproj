from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, field_validator

router = APIRouter()


class ComponentRevisionWriteRequest(BaseModel):
    componentId: Optional[str] = None
    revisionId: Optional[str] = None
    parentRevisionId: Optional[str] = None
    message: Optional[str] = None
    schemaVersion: int = 1
    graph: Dict[str, Any]
    api: Dict[str, Any]
    configSchema: Optional[Dict[str, Any]] = None

    @field_validator("graph")
    @classmethod
    def graph_must_have_nodes_edges(cls, v):
        if not isinstance(v, dict) or "nodes" not in v or "edges" not in v:
            raise ValueError("graph must include 'nodes' and 'edges'")
        return v

    @field_validator("api")
    @classmethod
    def api_must_have_inputs_outputs(cls, v):
        if not isinstance(v, dict):
            raise ValueError("api must be an object")
        if not isinstance(v.get("inputs"), list) or not isinstance(v.get("outputs"), list):
            raise ValueError("api must include inputs[] and outputs[]")
        return v


@router.post("")
async def create_component_revision(req: ComponentRevisionWriteRequest, request: Request):
    store = getattr(request.app.state, "component_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="component revision store unavailable")

    definition = {
        "graph": req.graph,
        "api": req.api,
        "configSchema": req.configSchema if isinstance(req.configSchema, dict) else {},
    }
    try:
        revision = store.create_revision(
            component_id=req.componentId,
            revision_id=req.revisionId,
            parent_revision_id=req.parentRevisionId,
            message=req.message,
            schema_version=req.schemaVersion,
            definition=definition,
        )
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex))

    return {
        "schemaVersion": 1,
        "componentId": revision.component_id,
        "revisionId": revision.revision_id,
        "parentRevisionId": revision.parent_revision_id,
        "createdAt": revision.created_at,
        "message": revision.message,
        "checksum": revision.checksum,
    }


@router.get("")
async def list_components(
    request: Request,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    store = getattr(request.app.state, "component_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="component revision store unavailable")
    rows = store.list_components(limit=limit, offset=offset)
    return {"schemaVersion": 1, "components": rows}


@router.get("/{component_id}/revisions")
async def list_component_revisions(
    component_id: str,
    request: Request,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    store = getattr(request.app.state, "component_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="component revision store unavailable")
    rows = store.list_revisions(component_id, limit=limit, offset=offset)
    return {"schemaVersion": 1, "componentId": component_id, "revisions": rows}


@router.get("/{component_id}/revisions/{revision_id}")
async def get_component_revision(component_id: str, revision_id: str, request: Request):
    store = getattr(request.app.state, "component_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="component revision store unavailable")
    row = store.get_revision(component_id, revision_id)
    if row is None:
        raise HTTPException(status_code=404, detail="revision not found")
    return {
        "schemaVersion": 1,
        "componentId": row.component_id,
        "revisionId": row.revision_id,
        "parentRevisionId": row.parent_revision_id,
        "createdAt": row.created_at,
        "message": row.message,
        "revisionSchemaVersion": row.schema_version,
        "checksum": row.checksum,
        "definition": row.definition,
    }

