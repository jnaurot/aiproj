from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, field_validator

from app.component_contracts import (
    COMPONENT_SCHEMA_VERSION,
    canonicalize_component_definition,
    validate_component_definition,
)
from app.executors.builtin_profiles import missing_packages_for_packages, resolve_builtin_environment

router = APIRouter()


def _post_canonical_port_schema_diagnostics(definition: Dict[str, Any]) -> list[dict[str, str]]:
    diagnostics: list[dict[str, str]] = []
    api = definition.get("api")
    outputs = api.get("outputs") if isinstance(api, dict) else None
    if not isinstance(outputs, list):
        return diagnostics
    for idx, output in enumerate(outputs):
        if not isinstance(output, dict):
            continue
        port_type = str(output.get("portType") or "").strip().lower()
        typed_schema = output.get("typedSchema")
        typed_type = (
            str(typed_schema.get("type") or "").strip().lower()
            if isinstance(typed_schema, dict)
            else ""
        )
        if port_type and typed_type and port_type != typed_type:
            diagnostics.append(
                {
                    "code": "TYPED_SCHEMA_PORT_MISMATCH",
                    "path": f"api.outputs[{idx}].typedSchema.type",
                    "message": "typedSchema.type must match portType after canonicalization",
                    "severity": "error",
                }
            )
    return diagnostics


def _component_builtin_environment_diagnostics(definition: Dict[str, Any]) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    graph = definition.get("graph") if isinstance(definition.get("graph"), dict) else {}
    nodes = graph.get("nodes") if isinstance(graph.get("nodes"), list) else []
    profile_map: Dict[str, Dict[str, Any]] = {}
    invalid_entries: list[dict[str, Any]] = []
    for idx, node in enumerate(nodes):
        if not isinstance(node, dict):
            continue
        node_id = str(node.get("id") or "").strip() or f"nodes[{idx}]"
        data = node.get("data") if isinstance(node.get("data"), dict) else {}
        if str((data or {}).get("kind") or "").strip().lower() != "tool":
            continue
        params = data.get("params") if isinstance(data.get("params"), dict) else {}
        provider = str((params or {}).get("provider") or "").strip().lower()
        builtin_cfg = params.get("builtin")
        if not isinstance(builtin_cfg, dict):
            if provider != "builtin":
                continue
            builtin_cfg = {}
        try:
            resolved = resolve_builtin_environment(builtin_cfg)
        except ValueError as ex:
            invalid_entries.append(
                {
                    "nodeId": node_id,
                    "profileId": str((builtin_cfg or {}).get("profileId") or "core").strip() or "core",
                    "message": str(ex),
                }
            )
            continue
        profile_id = str(resolved.get("profileId") or "core").strip() or "core"
        packages = [
            str(pkg).strip()
            for pkg in (resolved.get("packages") if isinstance(resolved.get("packages"), list) else [])
            if str(pkg).strip()
        ]
        entry = profile_map.setdefault(
            profile_id,
            {"profileId": profile_id, "packages": [], "_pkg_set": set(), "_node_ids": set()},
        )
        for pkg in packages:
            if pkg in entry["_pkg_set"]:
                continue
            entry["_pkg_set"].add(pkg)
            entry["packages"].append(pkg)
        entry["_node_ids"].add(node_id)

    if profile_map:
        profile_ids = sorted(profile_map.keys())
        diagnostics.append(
            {
                "code": "COMPONENT_ENV_PROFILES_REQUIRED",
                "path": "graph.nodes",
                "message": f"Component requires builtin environment profiles: {', '.join(profile_ids)}",
                "severity": "warning",
            }
        )

    for profile_id in sorted(profile_map.keys()):
        entry = profile_map[profile_id]
        packages = [
            str(pkg).strip()
            for pkg in (entry.get("packages") if isinstance(entry.get("packages"), list) else [])
            if str(pkg).strip()
        ]
        missing = missing_packages_for_packages(packages)
        if not missing:
            continue
        diagnostics.append(
            {
                "code": "COMPONENT_ENV_PROFILES_MISSING",
                "path": "graph.nodes",
                "message": (
                    f"Profile '{profile_id}' missing packages: {', '.join(missing)}. "
                    "Install profile: POST /env/profiles/install."
                ),
                "severity": "warning",
            }
        )

    for invalid in invalid_entries:
        diagnostics.append(
            {
                "code": "COMPONENT_ENV_PROFILE_INVALID",
                "path": "graph.nodes",
                "message": (
                    f"Tool node '{invalid.get('nodeId')}' has invalid builtin profile "
                    f"'{invalid.get('profileId')}': {invalid.get('message')}"
                ),
                "severity": "error",
            }
        )
    return diagnostics


class ComponentRevisionWriteRequest(BaseModel):
    componentId: Optional[str] = None
    revisionId: Optional[str] = None
    parentRevisionId: Optional[str] = None
    message: Optional[str] = None
    schemaVersion: int = COMPONENT_SCHEMA_VERSION
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


class ComponentRenameRequest(BaseModel):
    componentId: str


class ComponentValidateRequest(BaseModel):
    graph: Dict[str, Any]
    api: Dict[str, Any]
    configSchema: Optional[Dict[str, Any]] = None
    schemaVersion: int = COMPONENT_SCHEMA_VERSION


@router.post("/validate")
async def validate_component_revision(req: ComponentValidateRequest):
    raw_definition = {
        "graph": req.graph,
        "api": req.api,
        "configSchema": req.configSchema if isinstance(req.configSchema, dict) else {},
    }
    raw_diagnostics = [d.as_dict() for d in validate_component_definition(raw_definition)]
    normalized_definition, migration_notes = canonicalize_component_definition(
        raw_definition, int(req.schemaVersion or COMPONENT_SCHEMA_VERSION)
    )
    normalized_diagnostics = [d.as_dict() for d in validate_component_definition(normalized_definition)]
    normalized_diagnostics.extend(_post_canonical_port_schema_diagnostics(normalized_definition))
    normalized_diagnostics.extend(_component_builtin_environment_diagnostics(normalized_definition))
    diagnostics = raw_diagnostics + [d for d in normalized_diagnostics if d not in raw_diagnostics]
    ok = len([d for d in diagnostics if d.get("severity") == "error"]) == 0
    return {
        "schemaVersion": 1,
        "componentSchemaVersion": COMPONENT_SCHEMA_VERSION,
        "ok": ok,
        "diagnostics": diagnostics,
        "migrationNotes": migration_notes,
        "normalizedDefinition": normalized_definition,
    }


@router.post("")
async def create_component_revision(req: ComponentRevisionWriteRequest, request: Request):
    store = getattr(request.app.state, "component_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="component revision store unavailable")

    raw_definition = {
        "graph": req.graph,
        "api": req.api,
        "configSchema": req.configSchema if isinstance(req.configSchema, dict) else {},
    }
    raw_diagnostics = [d.as_dict() for d in validate_component_definition(raw_definition)]
    raw_errors = [d for d in raw_diagnostics if d.get("severity") == "error"]
    if raw_errors:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "COMPONENT_VALIDATION_FAILED",
                "message": "Component definition failed preflight validation",
                "diagnostics": raw_errors,
            },
        )
    definition, migration_notes = canonicalize_component_definition(
        raw_definition, int(req.schemaVersion or COMPONENT_SCHEMA_VERSION)
    )
    diagnostics = [d.as_dict() for d in validate_component_definition(definition)]
    diagnostics.extend(_post_canonical_port_schema_diagnostics(definition))
    diagnostics.extend(_component_builtin_environment_diagnostics(definition))
    errors = [d for d in diagnostics if d.get("severity") == "error"]
    if errors:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "COMPONENT_VALIDATION_FAILED",
                "message": "Component definition failed preflight validation",
                "diagnostics": errors,
            },
        )

    try:
        revision = store.create_revision(
            component_id=req.componentId,
            revision_id=req.revisionId,
            parent_revision_id=req.parentRevisionId,
            message=req.message,
            schema_version=COMPONENT_SCHEMA_VERSION,
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
        "componentSchemaVersion": COMPONENT_SCHEMA_VERSION,
        "migrationNotes": migration_notes,
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
        "componentSchemaVersion": COMPONENT_SCHEMA_VERSION,
        "componentId": row.component_id,
        "revisionId": row.revision_id,
        "parentRevisionId": row.parent_revision_id,
        "createdAt": row.created_at,
        "message": row.message,
        "revisionSchemaVersion": row.schema_version,
        "checksum": row.checksum,
        "definition": row.definition,
        "contractSnapshot": row.definition.get("contractSnapshot")
        if isinstance(row.definition, dict)
        else None,
    }


@router.patch("/{component_id}")
async def rename_component(component_id: str, req: ComponentRenameRequest, request: Request):
    store = getattr(request.app.state, "component_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="component revision store unavailable")
    from_id = str(component_id or "").strip()
    to_id = str(req.componentId or "").strip()
    try:
        result = store.rename_component(from_component_id=from_id, to_component_id=to_id)
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex))
    if not result.get("ok"):
        reason = str(result.get("reason") or "")
        if reason == "not_found":
            raise HTTPException(status_code=404, detail=f"component not found: {from_id}")
        if reason == "already_exists":
            raise HTTPException(status_code=409, detail=f"component already exists: {to_id}")
        raise HTTPException(status_code=400, detail=f"rename failed: {reason or 'unknown'}")
    return {
        "schemaVersion": 1,
        "componentId": str(result.get("componentId") or to_id),
        "renamedFrom": from_id,
    }


@router.delete("/{component_id}")
async def delete_component(component_id: str, request: Request):
    store = getattr(request.app.state, "component_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="component revision store unavailable")
    cid = str(component_id or "").strip()
    try:
        result = store.delete_component(cid)
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex))
    if not result.get("ok"):
        reason = str(result.get("reason") or "")
        if reason == "not_found":
            raise HTTPException(status_code=404, detail=f"component not found: {cid}")
        raise HTTPException(status_code=400, detail=f"delete failed: {reason or 'unknown'}")
    return {
        "schemaVersion": 1,
        "componentId": cid,
        "deletedRevisions": int(result.get("deletedRevisions") or 0),
        "deletedComponents": int(result.get("deletedComponents") or 0),
    }


@router.delete("/{component_id}/revisions/{revision_id}")
async def delete_component_revision(component_id: str, revision_id: str, request: Request):
    store = getattr(request.app.state, "component_revisions", None)
    if store is None:
        raise HTTPException(status_code=500, detail="component revision store unavailable")
    cid = str(component_id or "").strip()
    rid = str(revision_id or "").strip()
    try:
        result = store.delete_revision(cid, rid)
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex))
    if not result.get("ok"):
        reason = str(result.get("reason") or "")
        if reason == "component_not_found":
            raise HTTPException(status_code=404, detail=f"component not found: {cid}")
        if reason == "revision_not_found":
            raise HTTPException(status_code=404, detail=f"revision not found: {cid}@{rid}")
        raise HTTPException(status_code=400, detail=f"delete failed: {reason or 'unknown'}")
    return {
        "schemaVersion": 1,
        "componentId": cid,
        "revisionId": rid,
        "deletedRevisions": int(result.get("deletedRevisions") or 0),
        "remainingLatestRevisionId": result.get("remainingLatestRevisionId"),
        "componentDeleted": bool(result.get("componentDeleted")),
    }

