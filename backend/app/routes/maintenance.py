import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request

from ..runner.artifacts import Artifact
from ..runner.node_state import build_exec_key, build_node_state_hash

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
    graph_id = "maintenance"
    node_id = "maintenance.gc"
    node = {"data": {"kind": "maintenance", "ports": {}, "schema": {}, "settings": {}}}
    node_state_hash = build_node_state_hash(
        node=node,
        params=payload,
        execution_version="v1",
    )
    exec_key = build_exec_key(
        graph_id=graph_id,
        node_id=node_id,
        node_kind="maintenance",
        node_state_hash=node_state_hash,
        upstream_artifact_ids=[],
        input_refs=[],
        determinism_env={},
        execution_version="v1",
    )
    artifact_id = exec_key
    artifact = Artifact(
        artifact_id=artifact_id,
        node_kind="maintenance",
        params_hash=node_state_hash,
        upstream_ids=[],
        created_at=datetime.now(timezone.utc),
        execution_version="v1",
        mime_type="application/json",
        port_type="json",
        size_bytes=len(payload_bytes),
        storage_uri=f"artifact://{artifact_id}",
        payload_schema={"type": "maintenance_audit"},
        run_id="maintenance",
        graph_id=graph_id,
        node_id=node_id,
        exec_key=exec_key,
    )
    await store.write(artifact, payload_bytes)
    return artifact_id


def _delete_children(path: Path) -> dict:
    removed_files = 0
    removed_dirs = 0
    if not path.exists():
        return {"removedFiles": 0, "removedDirs": 0}
    for child in list(path.iterdir()):
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
            removed_dirs += 1
        else:
            try:
                child.unlink(missing_ok=True)
            except Exception:
                pass
            removed_files += 1
    return {"removedFiles": removed_files, "removedDirs": removed_dirs}


def _wipe_disk_backends(rt) -> dict:
    details: dict = {"kind": "disk"}
    store = getattr(rt, "artifact_store", None)
    cache = getattr(rt, "cache", None)
    events = getattr(rt, "event_store", None)

    # Artifact metadata + snapshot metadata + consumers + runs.
    index = getattr(store, "_index", None)
    idx_conn = getattr(index, "_conn", None)
    if idx_conn is not None:
        cur = idx_conn.cursor()
        cur.execute("DELETE FROM artifact_consumers")
        cur.execute("DELETE FROM artifacts")
        cur.execute("DELETE FROM runs")
        cur.execute("DELETE FROM snapshots")
        cur.execute("DELETE FROM run_experiments")
        idx_conn.commit()
        details["artifactRowsCleared"] = True
    else:
        details["artifactRowsCleared"] = False

    # Blob files.
    blob_root = getattr(store, "_blob_root", None)
    if isinstance(blob_root, Path):
        details["blobs"] = _delete_children(blob_root)
    else:
        details["blobs"] = {"removedFiles": 0, "removedDirs": 0}

    # Execution cache rows.
    cache_conn = getattr(cache, "_conn", None)
    if cache_conn is not None:
        cur = cache_conn.cursor()
        cur.execute("DELETE FROM execution_cache")
        cache_conn.commit()
        details["cacheRowsCleared"] = True
    else:
        details["cacheRowsCleared"] = False

    # Event rows.
    event_conn = getattr(events, "_conn", None)
    if event_conn is not None:
        cur = event_conn.cursor()
        cur.execute("DELETE FROM run_events")
        event_conn.commit()
        details["eventRowsCleared"] = True
    else:
        details["eventRowsCleared"] = False

    return details


def _wipe_memory_backends(rt) -> dict:
    details: dict = {"kind": "memory"}
    store = getattr(rt, "artifact_store", None)
    cache = getattr(rt, "cache", None)
    events = getattr(rt, "event_store", None)

    for attr in ("_meta", "_blob", "_runs", "_consumers", "_snapshots", "_experiments"):
        ref = getattr(store, attr, None)
        if isinstance(ref, dict):
            ref.clear()
    details["artifactRowsCleared"] = True

    index = getattr(cache, "_index", None)
    if isinstance(index, dict):
        index.clear()
        details["cacheRowsCleared"] = True
    else:
        details["cacheRowsCleared"] = False

    rows = getattr(events, "_rows", None)
    if isinstance(rows, list):
        rows.clear()
        details["eventRowsCleared"] = True
    else:
        details["eventRowsCleared"] = False
    return details


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


@router.post("/reset-storage")
async def reset_storage_maintenance(
    request: Request,
    confirm: str = Query(default=""),
):
    """
    Deletes all artifacts, cache rows, and persisted run events.
    Guarded behind ENABLE_MAINTENANCE_ENDPOINTS and a confirm token.
    """
    if not _maintenance_enabled():
        raise HTTPException(403, "Maintenance endpoints are disabled")
    if str(confirm).strip().upper() != "RESET_ALL":
        raise HTTPException(400, "confirm must be RESET_ALL")

    rt = request.app.state.runtime
    store = getattr(rt, "artifact_store", None)
    if hasattr(store, "_index") and hasattr(store, "_blob_root"):
        details = _wipe_disk_backends(rt)
    else:
        details = _wipe_memory_backends(rt)

    # Runtime in-memory handles/state
    if hasattr(rt, "runs") and isinstance(rt.runs, dict):
        rt.runs.clear()
    if hasattr(rt, "_artifact_owner") and isinstance(rt._artifact_owner, dict):
        rt._artifact_owner.clear()
    model_registry = getattr(request.app.state, "model_registry", None)
    if model_registry is not None and hasattr(model_registry, "clear_all"):
        model_registry.clear_all()

    response = {
        "ok": True,
        "at": _iso_now(),
        "storageReset": details,
    }
    print("[maintenance-reset-storage]", response)
    return response
