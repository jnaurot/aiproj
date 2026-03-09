from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, field_validator

from ..feature_flags import get_feature_flags
from ..graph_migrations import canonicalize_graph_payload, find_component_edge_handle_errors

router = APIRouter()


class GraphRevisionWriteRequest(BaseModel):
    graphId: Optional[str] = None
    graphName: Optional[str] = None
    versionName: Optional[str] = None
    revisionKind: Optional[str] = None
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


class GraphImportRequest(BaseModel):
    package: Dict[str, Any]
    targetGraphId: Optional[str] = None
    message: Optional[str] = None


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_graph_export_package(
    *,
    graph_id: str,
    revision_id: str,
    graph: Dict[str, Any],
    include_artifacts: bool = False,
    include_schemas: bool = True,
) -> Dict[str, Any]:
    component_dependencies = _extract_component_dependencies(graph)
    warnings: list[str] = []
    if include_artifacts:
        warnings.append("Artifacts export is not implemented yet; package contains graph only.")
    return {
        "manifest": {
            "packageType": "aipgraph",
            "packageVersion": 2,
            "schemaVersion": 1,
            "engineVersion": "aiproj-flow",
            "exportedAt": _iso_now(),
            "source": {"graphId": graph_id, "revisionId": revision_id},
            "includes": {
                "artifacts": bool(include_artifacts and False),
                "schemas": bool(include_schemas),
            },
            "dependencies": {
                "components": component_dependencies,
            },
            "warnings": warnings,
        },
        "graph": graph,
        "schemas": {} if include_schemas else None,
        "artifacts": [] if include_artifacts else None,
    }


def _extract_component_dependencies(graph: Dict[str, Any]) -> list[Dict[str, str]]:
    nodes = graph.get("nodes", []) if isinstance(graph, dict) else []
    deps: dict[tuple[str, str], Dict[str, str]] = {}
    for node in nodes:
        if not isinstance(node, dict):
            continue
        data = node.get("data", {}) if isinstance(node.get("data"), dict) else {}
        if str(data.get("kind") or "") != "component":
            continue
        params = data.get("params", {}) if isinstance(data.get("params"), dict) else {}
        component_ref = params.get("componentRef") if isinstance(params.get("componentRef"), dict) else {}
        component_id = str(component_ref.get("componentId") or "").strip()
        revision_id = str(component_ref.get("revisionId") or "").strip()
        api_version = str(component_ref.get("apiVersion") or "v1").strip() or "v1"
        if not component_id or not revision_id:
            continue
        deps[(component_id, revision_id)] = {
            "componentId": component_id,
            "revisionId": revision_id,
            "apiVersion": api_version,
        }
    return sorted(deps.values(), key=lambda d: (str(d.get("componentId") or ""), str(d.get("revisionId") or "")))


def _validate_component_dependencies(
    *,
    manifest: Dict[str, Any],
    graph: Dict[str, Any],
    component_store: Any,
) -> Dict[str, Any]:
    dependencies_obj = manifest.get("dependencies") if isinstance(manifest, dict) else {}
    dependencies = dependencies_obj.get("components") if isinstance(dependencies_obj, dict) else None
    declared = dependencies if isinstance(dependencies, list) else _extract_component_dependencies(graph)
    unresolved: list[Dict[str, str]] = []
    for dep in declared:
        if not isinstance(dep, dict):
            continue
        component_id = str(dep.get("componentId") or "").strip()
        revision_id = str(dep.get("revisionId") or "").strip()
        if not component_id or not revision_id:
            continue
        if component_store is None:
            unresolved.append(
                {
                    "componentId": component_id,
                    "revisionId": revision_id,
                    "reason": "component_store_unavailable",
                }
            )
            continue
        revision = component_store.get_revision(component_id, revision_id)
        if revision is None:
            unresolved.append(
                {
                    "componentId": component_id,
                    "revisionId": revision_id,
                    "reason": "revision_not_found",
                }
            )
    return {
        "declared": declared,
        "unresolved": unresolved,
    }


def _normalize_import_package(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("package must be an object")
    manifest = payload.get("manifest")
    graph = payload.get("graph")
    if isinstance(manifest, dict):
        ptype = str(manifest.get("packageType") or "").strip()
        pver = int(manifest.get("packageVersion") or 0)
        if ptype != "aipgraph":
            raise ValueError("manifest.packageType must be 'aipgraph'")
        if pver == 2:
            if not isinstance(graph, dict):
                raise ValueError("package must include manifest and graph")
            if "nodes" not in graph or "edges" not in graph:
                raise ValueError("package.graph must include nodes and edges")
            normalized_graph, migration_notes = canonicalize_graph_payload(graph)
            handle_errors = find_component_edge_handle_errors(normalized_graph)
            if handle_errors:
                raise ValueError(
                    "graph contains unresolved component output handles: "
                    + "; ".join(str(err.get("edgeId") or "") for err in handle_errors if isinstance(err, dict))
                )
            return {
                "manifest": manifest,
                "graph": normalized_graph,
                "migrationReport": {
                    "format": "aipgraph_v2",
                    "migrated": bool(len(migration_notes) > 0),
                    "warnings": [],
                    "notes": migration_notes,
                },
            }
        if pver == 1:
            if not isinstance(graph, dict):
                raise ValueError("legacy package must include graph")
            graph_candidate = graph
            report_format = "aipgraph_v1_legacy"
        else:
            raise ValueError("manifest.packageVersion must be 2 or legacy 1")
    else:
        # Legacy import compatibility: accept raw graph objects (no manifest).
        if isinstance(graph, dict) and "nodes" in graph and "edges" in graph:
            graph_candidate = graph
        elif "nodes" in payload and "edges" in payload:
            graph_candidate = payload
        else:
            raise ValueError("package must include manifest+graph or legacy graph nodes/edges")
        report_format = "raw_graph_legacy"
        manifest = {
            "packageType": "aipgraph",
            "packageVersion": 2,
            "schemaVersion": 1,
            "engineVersion": "aiproj-flow",
            "exportedAt": _iso_now(),
            "source": {
                "graphId": str(((graph_candidate.get("meta") or {}).get("graphId") or "")).strip(),
                "revisionId": "",
            },
            "includes": {"artifacts": False, "schemas": True},
            "dependencies": {"components": _extract_component_dependencies(graph_candidate)},
            "warnings": ["Legacy graph import shim applied (no manifest)."],
        }

    if "nodes" not in graph_candidate or "edges" not in graph_candidate:
        raise ValueError("package.graph must include nodes and edges")
    normalized_graph, migration_notes = canonicalize_graph_payload(graph_candidate)
    handle_errors = find_component_edge_handle_errors(normalized_graph)
    if handle_errors:
        raise ValueError(
            "graph contains unresolved component output handles: "
            + "; ".join(str(err.get("edgeId") or "") for err in handle_errors if isinstance(err, dict))
        )
    migration_notes = [
        {
            "code": "LEGACY_IMPORT_SHIM_APPLIED",
            "message": "Legacy import shim translated package to canonical v2 graph payload.",
        },
        *migration_notes,
    ]
    legacy_manifest = {
        "packageType": "aipgraph",
        "packageVersion": 2,
        "schemaVersion": 1,
        "engineVersion": "aiproj-flow",
        "exportedAt": _iso_now(),
        "source": {
            "graphId": str(((normalized_graph.get("meta") or {}).get("graphId") or "")).strip(),
            "revisionId": "",
        },
        "includes": {"artifacts": False, "schemas": True},
        "dependencies": {"components": _extract_component_dependencies(normalized_graph)},
        "warnings": [f"Legacy packageVersion={int(manifest.get('packageVersion') or 1)} translated on import."]
        if isinstance(manifest, dict)
        else ["Legacy graph translated on import."],
    }
    return {
        "manifest": legacy_manifest,
        "graph": normalized_graph,
        "migrationReport": {
            "format": report_format,
            "migrated": True,
            "warnings": [],
            "notes": migration_notes,
        },
    }


@router.get("/feature-flags")
async def graph_feature_flags():
    return {"schemaVersion": 1, "flags": get_feature_flags()}


@router.put("/feature-flags")
async def set_graph_feature_flags(req: GraphFeatureFlagsUpdateRequest):
    raise HTTPException(status_code=410, detail="Feature-flag mutation is disabled after Phase 8 cutover")


@router.get("/{graph_id}/export")
async def export_graph_package_v2(
    graph_id: str,
    request: Request,
    revisionId: Optional[str] = Query(default=None),
    include_artifacts: bool = Query(default=False),
    include_schemas: bool = Query(default=True),
):
    store = getattr(request.app.state, "graph_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="graph revision store unavailable")

    if revisionId:
        row = store.get_revision(graph_id, str(revisionId))
    else:
        row = store.get_latest(graph_id)
    if row is None:
        raise HTTPException(status_code=404, detail="graph not found")

    pkg = _build_graph_export_package(
        graph_id=str(row.graph_id),
        revision_id=str(row.revision_id),
        graph=row.graph,
        include_artifacts=include_artifacts,
        include_schemas=include_schemas,
    )
    return {"schemaVersion": 1, "package": pkg}


@router.post("/import")
async def import_graph_package_v2(req: GraphImportRequest, request: Request):
    store = getattr(request.app.state, "graph_revisions", None)
    component_store = getattr(request.app.state, "component_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="graph revision store unavailable")
    try:
        normalized = _normalize_import_package(req.package)
        manifest = normalized["manifest"]
        graph = normalized["graph"]
        report = normalized["migrationReport"]
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex))

    dependency_check = _validate_component_dependencies(
        manifest=manifest,
        graph=graph,
        component_store=component_store,
    )
    unresolved = dependency_check.get("unresolved") if isinstance(dependency_check, dict) else []
    if isinstance(unresolved, list) and unresolved:
        report["warnings"].append("Some component dependencies are unresolved in this environment.")
        report["unresolvedComponentDependencies"] = unresolved
    report["componentDependencies"] = dependency_check.get("declared") if isinstance(dependency_check, dict) else []

    target_graph_id = str(req.targetGraphId or "").strip() or None
    message = str(req.message).strip() if isinstance(req.message, str) and str(req.message).strip() else "import:v2"
    try:
        created = store.create_revision(
            graph_id=target_graph_id,
            graph=graph,
            message=message,
            revision_kind="import",
            schema_version=1,
        )
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex))

    return {
        "schemaVersion": 1,
        "graphId": created.graph_id,
        "revisionId": created.revision_id,
        "createdAt": created.created_at,
        "migrationReport": report,
        "graph": created.graph,
    }


@router.post("")
async def create_graph_revision(req: GraphRevisionWriteRequest, request: Request):
    store = getattr(request.app.state, "graph_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="graph revision store unavailable")

    normalized_graph, migration_notes = canonicalize_graph_payload(req.graph)
    handle_errors = find_component_edge_handle_errors(normalized_graph)
    if handle_errors:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "COMPONENT_OUTPUT_HANDLE_UNRESOLVED",
                "message": "graph contains unresolved component output handles",
                "errors": handle_errors,
            },
        )
    try:
        revision = store.create_revision(
            graph_id=req.graphId,
            graph=normalized_graph,
            message=req.message,
            graph_name=req.graphName,
            version_name=req.versionName,
            revision_kind=req.revisionKind or "save_graph",
            parent_revision_id=req.parentRevisionId,
            revision_id=req.revisionId,
            schema_version=req.schemaVersion,
        )
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex))

    return {
        "schemaVersion": 1,
        "graphId": revision.graph_id,
        "graphName": revision.graph_name,
        "revisionId": revision.revision_id,
        "parentRevisionId": revision.parent_revision_id,
        "createdAt": revision.created_at,
        "message": revision.message,
        "versionName": revision.version_name,
        "revisionKind": revision.revision_kind,
        "checksum": revision.checksum,
        "migrationNotes": migration_notes,
    }


@router.get("")
async def list_graphs(
    request: Request,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    store = getattr(request.app.state, "graph_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="graph revision store unavailable")
    rows = store.list_graphs(limit=limit, offset=offset)
    return {"schemaVersion": 1, "graphs": rows}


@router.get("/{graph_id}/latest")
async def get_latest_graph_revision(graph_id: str, request: Request):
    store = getattr(request.app.state, "graph_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="graph revision store unavailable")

    row = store.get_latest(graph_id)
    if row is None:
        raise HTTPException(status_code=404, detail="graph not found")

    return {
        "schemaVersion": 1,
        "graphId": row.graph_id,
        "graphName": row.graph_name,
        "revisionId": row.revision_id,
        "parentRevisionId": row.parent_revision_id,
        "createdAt": row.created_at,
        "message": row.message,
        "versionName": row.version_name,
        "revisionKind": row.revision_kind,
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
    store = getattr(request.app.state, "graph_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="graph revision store unavailable")

    rows = store.list_revisions(graph_id, limit=limit, offset=offset)
    return {"schemaVersion": 1, "graphId": graph_id, "revisions": rows}


@router.get("/{graph_id}/revisions/{revision_id}")
async def get_graph_revision(graph_id: str, revision_id: str, request: Request):
    store = getattr(request.app.state, "graph_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="graph revision store unavailable")

    row = store.get_revision(graph_id, revision_id)
    if row is None:
        raise HTTPException(status_code=404, detail="revision not found")

    return {
        "schemaVersion": 1,
        "graphId": row.graph_id,
        "graphName": row.graph_name,
        "revisionId": row.revision_id,
        "parentRevisionId": row.parent_revision_id,
        "createdAt": row.created_at,
        "message": row.message,
        "versionName": row.version_name,
        "revisionKind": row.revision_kind,
        "revisionSchemaVersion": row.schema_version,
        "checksum": row.checksum,
        "graph": row.graph,
    }


@router.delete("/{graph_id}")
async def delete_graph(graph_id: str, request: Request):
    store = getattr(request.app.state, "graph_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="graph revision store unavailable")
    result = store.delete_graph(graph_id)
    if not result.get("deleted"):
        reason = str(result.get("reason") or "delete_failed")
        if reason == "graph_not_found":
            raise HTTPException(status_code=404, detail="graph not found")
        raise HTTPException(status_code=400, detail=reason)
    return {"schemaVersion": 1, **result}


@router.delete("/{graph_id}/revisions/{revision_id}")
async def delete_graph_revision(graph_id: str, revision_id: str, request: Request):
    store = getattr(request.app.state, "graph_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="graph revision store unavailable")
    result = store.delete_revision(graph_id, revision_id)
    if not result.get("deleted"):
        reason = str(result.get("reason") or "delete_failed")
        if reason in {"graph_not_found", "revision_not_found"}:
            raise HTTPException(status_code=404, detail=reason)
        raise HTTPException(status_code=400, detail=reason)
    return {"schemaVersion": 1, **result}
