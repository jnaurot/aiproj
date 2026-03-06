from __future__ import annotations

import os
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, field_validator

from ..feature_flags import get_feature_flags

router = APIRouter()


class GraphRevisionWriteRequest(BaseModel):
    graphId: Optional[str] = None
    revisionId: Optional[str] = None
    parentRevisionId: Optional[str] = None
    message: Optional[str] = None
    schemaVersion: int = 1
    graph: Dict[str, Any]

    @field_validator("graph")
    @classmethod
    def graph_must_have_nodes_edges(cls, v):
        if not isinstance(v, dict) or "nodes" not in v or "edges" not in v:
            raise ValueError("graph must include 'nodes' and 'edges'")
        return v


class GraphFeatureFlagsUpdateRequest(BaseModel):
    GRAPH_STORE_V2_READ: Optional[bool] = None
    GRAPH_STORE_V2_WRITE: Optional[bool] = None
    GRAPH_EXPORT_V2: Optional[bool] = None


@router.get("/feature-flags")
async def graph_feature_flags():
    return {"schemaVersion": 1, "flags": get_feature_flags()}


@router.put("/feature-flags")
async def set_graph_feature_flags(req: GraphFeatureFlagsUpdateRequest):
    updates = req.model_dump(exclude_none=True)
    for key, value in updates.items():
        os.environ[key] = "1" if bool(value) else "0"
    return {"schemaVersion": 1, "flags": get_feature_flags()}


@router.post("")
async def create_graph_revision(req: GraphRevisionWriteRequest, request: Request):
    flags = get_feature_flags()
    if not flags.get("GRAPH_STORE_V2_WRITE", False):
        raise HTTPException(
            status_code=503,
            detail="GRAPH_STORE_V2_WRITE is disabled",
        )

    store = getattr(request.app.state, "graph_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="graph revision store unavailable")

    try:
        revision = store.create_revision(
            graph_id=req.graphId,
            graph=req.graph,
            message=req.message,
            parent_revision_id=req.parentRevisionId,
            revision_id=req.revisionId,
            schema_version=req.schemaVersion,
        )
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex))

    return {
        "schemaVersion": 1,
        "graphId": revision.graph_id,
        "revisionId": revision.revision_id,
        "parentRevisionId": revision.parent_revision_id,
        "createdAt": revision.created_at,
        "message": revision.message,
        "checksum": revision.checksum,
    }


@router.get("/{graph_id}/latest")
async def get_latest_graph_revision(graph_id: str, request: Request):
    flags = get_feature_flags()
    if not flags.get("GRAPH_STORE_V2_READ", False):
        raise HTTPException(status_code=503, detail="GRAPH_STORE_V2_READ is disabled")

    store = getattr(request.app.state, "graph_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="graph revision store unavailable")

    row = store.get_latest(graph_id)
    if row is None:
        raise HTTPException(status_code=404, detail="graph not found")

    return {
        "schemaVersion": 1,
        "graphId": row.graph_id,
        "revisionId": row.revision_id,
        "parentRevisionId": row.parent_revision_id,
        "createdAt": row.created_at,
        "message": row.message,
        "revisionSchemaVersion": row.schema_version,
        "checksum": row.checksum,
        "graph": row.graph,
    }


@router.get("/{graph_id}/revisions")
async def list_graph_revisions(
    graph_id: str,
    request: Request,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    flags = get_feature_flags()
    if not flags.get("GRAPH_STORE_V2_READ", False):
        raise HTTPException(status_code=503, detail="GRAPH_STORE_V2_READ is disabled")

    store = getattr(request.app.state, "graph_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="graph revision store unavailable")

    rows = store.list_revisions(graph_id, limit=limit, offset=offset)
    return {"schemaVersion": 1, "graphId": graph_id, "revisions": rows}


@router.get("/{graph_id}/revisions/{revision_id}")
async def get_graph_revision(graph_id: str, revision_id: str, request: Request):
    flags = get_feature_flags()
    if not flags.get("GRAPH_STORE_V2_READ", False):
        raise HTTPException(status_code=503, detail="GRAPH_STORE_V2_READ is disabled")

    store = getattr(request.app.state, "graph_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="graph revision store unavailable")

    row = store.get_revision(graph_id, revision_id)
    if row is None:
        raise HTTPException(status_code=404, detail="revision not found")

    return {
        "schemaVersion": 1,
        "graphId": row.graph_id,
        "revisionId": row.revision_id,
        "parentRevisionId": row.parent_revision_id,
        "createdAt": row.created_at,
        "message": row.message,
        "revisionSchemaVersion": row.schema_version,
        "checksum": row.checksum,
        "graph": row.graph,
    }
