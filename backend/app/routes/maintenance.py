import hashlib
import json
import os
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, Request

from ..runner.artifacts import Artifact

router = APIRouter()


def _maintenance_enabled() -> bool:
    return (os.getenv("ENABLE_MAINTENANCE_ENDPOINTS") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _write_maintenance_audit_artifact(request: Request, payload: dict) -> str | None:
    rt = request.app.state.runtime
    store = rt.artifact_store
    body = {
        "type": "maintenance_audit",
        "at": _iso_now(),
        "payload": payload,
    }
    payload_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8")
    artifact_id = hashlib.sha256(payload_bytes).hexdigest()
    artifact = Artifact(
        artifact_id=artifact_id,
        node_kind="maintenance",
        params_hash=hashlib.sha256(
            json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
        ).hexdigest(),
        upstream_ids=[],
        created_at=datetime.now(timezone.utc),
        execution_version="v1",
        mime_type="application/json",
        size_bytes=len(payload_bytes),
        storage_uri=f"artifact://{artifact_id}",
        payload_schema={"type": "maintenance_audit"},
        run_id="maintenance",
        node_id="maintenance.gc",
        exec_key=None,
    )
    await store.write(artifact, payload_bytes)
    return artifact_id


@router.post("/gc")
async def gc_maintenance(
    request: Request,
    mode: str = Query(default="dry_run"),
    limit: int | None = Query(default=None, ge=1),
    max_seconds: int | None = Query(default=None, ge=1),
    verbose: bool = Query(default=False),
    sample_limit: int = Query(default=20, ge=1, le=500),
):
    if not _maintenance_enabled():
        raise HTTPException(403, "Maintenance endpoints are disabled")

    mode_norm = (mode or "dry_run").lower()
    if mode_norm not in ("dry_run", "delete"):
        raise HTTPException(400, "mode must be 'dry_run' or 'delete'")

    rt = request.app.state.runtime
    result = await rt.artifact_store.gc_orphan_blobs(
        mode=mode_norm,
        limit=limit,
        max_seconds=max_seconds,
    )
    orphan_hashes = result.get("orphan_hashes") or []
    response = {
        "mode": result.get("mode", mode_norm),
        "referenced_count": int(result.get("referenced_hashes", 0)),
        "orphan_count": len(orphan_hashes),
        "blobs_deleted": int(result.get("blobs_deleted", 0)),
        "scanned_blobs": int(result.get("scanned_blobs", 0)),
        "orphan_samples": orphan_hashes[:sample_limit],
    }

    if verbose:
        response["orphan_hashes"] = orphan_hashes

    if mode_norm == "delete":
        audit_payload = {
            "operation": "maintenance.gc",
            "mode": mode_norm,
            "limit": limit,
            "max_seconds": max_seconds,
            "result": response,
            "remote_addr": getattr(getattr(request, "client", None), "host", None),
        }
        audit_id = await _write_maintenance_audit_artifact(request, audit_payload)
        response["audit_artifact_id"] = audit_id

    return response


@router.post("/events/prune")
async def prune_events_maintenance(
    request: Request,
    keep_last: int = Query(..., ge=0),
    dry_run: bool = Query(default=True),
):
    if not _maintenance_enabled():
        raise HTTPException(403, "Maintenance endpoints are disabled")

    rt = request.app.state.runtime
    result = await rt.prune_events(keep_last=keep_last, dry_run=dry_run, run_id=None)
    response = {
        "keep_last": int(result.get("keep_last", keep_last)),
        "dry_run": bool(result.get("dry_run", dry_run)),
        "rows_deleted": int(result.get("rows_deleted", 0)),
        "runs_affected": int(result.get("runs_affected", 0)),
        "oldest_remaining_event_id": result.get("oldest_remaining_event_id"),
    }

    if not dry_run:
        audit_payload = {
            "operation": "maintenance.events.prune",
            "keep_last": keep_last,
            "dry_run": False,
            "result": response,
            "remote_addr": getattr(getattr(request, "client", None), "host", None),
        }
        audit_id = await _write_maintenance_audit_artifact(request, audit_payload)
        response["audit_artifact_id"] = audit_id

    return response
