from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import uuid
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Protocol, Set, Tuple

from pydantic import BaseModel

logger = logging.getLogger(__name__)


_PAYLOAD_TYPES = {"table", "json", "text", "binary", "embeddings", "image", "audio", "video"}
_ARTIFACT_METADATA_VERSION = 1
_TERMINAL_RUN_STATUSES = {"succeeded", "failed", "canceled", "cancelled", "skipped"}
_REQUIRED_ARTIFACT_META_KEYS = {
    "metadataVersion",
    "execKey",
    "nodeId",
    "nodeType",
    "nodeImplVersion",
    "paramsFingerprint",
    "upstreamArtifactIds",
    "contractFingerprint",
    "schemaFingerprint",
    "mimeType",
    "payloadType",
    "createdAt",
}


def _bool_env(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return bool(default)
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _retention_mode() -> str:
    mode = str(os.getenv("ARTIFACT_RETENTION_MODE", "by_node") or "by_node").strip().lower()
    return mode if mode in {"off", "by_run", "by_node"} else "by_node"


def _retention_keep_recent_runs() -> int:
    raw = str(os.getenv("ARTIFACT_KEEP_RECENT_RUNS", "5") or "5").strip()
    try:
        return max(0, int(raw))
    except Exception:
        return 5


def _retention_keep_node_versions() -> int:
    """Number of distinct artifact versions to keep per (graph_id, node_id) in by_node mode."""
    raw = str(os.getenv("ARTIFACT_KEEP_NODE_VERSIONS", "2") or "2").strip()
    try:
        return max(1, int(raw))
    except Exception:
        return 2


def _retention_include_failed() -> bool:
    return _bool_env("ARTIFACT_RETENTION_INCLUDE_FAILED", True)


def _retention_include_canceled() -> bool:
    return _bool_env("ARTIFACT_RETENTION_INCLUDE_CANCELED", True)


def _is_terminal_status(status: str) -> bool:
    return str(status or "").strip().lower() in _TERMINAL_RUN_STATUSES


def _should_include_status_for_retention(status: str) -> bool:
    s = str(status or "").strip().lower()
    if s == "succeeded":
        return True
    if s in {"failed"}:
        return _retention_include_failed()
    if s in {"canceled", "cancelled"}:
        return _retention_include_canceled()
    if s == "skipped":
        return True
    return False


def _normalize_payload_schema(payload_schema: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(payload_schema, dict):
        return payload_schema
    out = dict(payload_schema)
    t = str(out.get("type") or "").lower()
    if t == "string":
        out["type"] = "text"
        out.setdefault("encoding", "utf-8")
    return out


def _infer_payload_type(
    *,
    payload_schema: Optional[Dict[str, Any]],
    mime_type: Optional[str],
    node_kind: Optional[str] = None,
) -> str:
    ps = _normalize_payload_schema(payload_schema)
    if isinstance(ps, dict):
        t = str(ps.get("type") or "").lower()
        if t in _PAYLOAD_TYPES:
            return t
    mt = str(mime_type or "").lower()
    if "json" in mt:
        return "json"
    if "markdown" in mt or mt.startswith("text/"):
        return "text"
    if "csv" in mt or "tsv" in mt or "parquet" in mt:
        return "table"
    if mt.startswith("image/"):
        return "image"
    if mt.startswith("audio/"):
        return "audio"
    if mt.startswith("video/"):
        return "video"
    if node_kind == "transform":
        return "table"
    return "binary"


def _validate_artifact_metadata_v1(artifact: "Artifact") -> None:
    if not artifact.run_id:
        return
    ps = artifact.payload_schema if isinstance(artifact.payload_schema, dict) else {}
    meta = ps.get("artifactMetadataV1") if isinstance(ps, dict) else None
    if not isinstance(meta, dict):
        raise ValueError("Runtime artifact writes require payload_schema.artifactMetadataV1")
    missing = [k for k in _REQUIRED_ARTIFACT_META_KEYS if k not in meta]
    if missing:
        raise ValueError(f"ArtifactMetadataV1 missing required keys: {','.join(sorted(missing))}")
    if int(meta.get("metadataVersion") or -1) != _ARTIFACT_METADATA_VERSION:
        raise ValueError("ArtifactMetadataV1.metadataVersion must be 1")


# ----------------------------
# Models
# ----------------------------

class Artifact(BaseModel):
    artifact_id: str  # execution key identity for runtime artifacts
    node_kind: str
    params_hash: str
    upstream_ids: List[str]
    created_at: datetime
    execution_version: str

    mime_type: str
    payload_type: Optional[str] = None
    size_bytes: int

    storage_uri: str  # memory://<id>, file://..., s3://...

    payload_schema: Optional[Dict[str, Any]] = None
    content_hash: Optional[str] = None
    run_id: Optional[str] = None
    graph_id: Optional[str] = None
    node_id: Optional[str] = None
    exec_key: Optional[str] = None


class RunArtifactBinding(BaseModel):
    run_id: str
    graph_id: str
    node_id: str
    handle: str = "out"
    artifact_id: str
    status: str  # "computed" | "cached" | "reused"
    bound_at: datetime


# ----------------------------
# Store interface
# ----------------------------

class ArtifactStore(Protocol):
    async def exists(self, artifact_id: str) -> bool: ...
    async def get(self, artifact_id: str) -> Artifact: ...
    async def read(self, artifact_id: str) -> bytes: ...
    async def open_payload(self, artifact_id: str) -> AsyncIterator[bytes]: ...
    async def write(self, artifact: Artifact, data: bytes) -> str: ...
    async def record_run(self, run_id: str, status: str) -> None: ...
    async def update_run_status(self, run_id: str, status: str) -> None: ...
    async def get_run(self, run_id: str) -> Optional[Dict[str, Any]]: ...
    async def list_runs(self, include_deleted: bool = False) -> List[Dict[str, Any]]: ...
    async def upsert_run_pause_snapshot(self, run_id: str, snapshot: Dict[str, Any]) -> None: ...
    async def get_run_pause_snapshot(self, run_id: str) -> Optional[Dict[str, Any]]: ...
    async def delete_run_pause_snapshot(self, run_id: str) -> None: ...
    async def delete_run(self, run_id: str, mode: str = "soft", gc: str = "none") -> Dict[str, Any]: ...
    async def record_consumers(
        self,
        *,
        input_artifact_ids: List[str],
        consumer_run_id: str,
        consumer_node_id: str,
        consumer_exec_key: Optional[str],
        output_artifact_id: str,
    ) -> None: ...
    async def get_consumers(self, artifact_id: str, limit: int = 50) -> List[Dict[str, Any]]: ...
    async def gc_orphan_blobs(
        self, mode: str = "dry_run", limit: Optional[int] = None, max_seconds: Optional[int] = None
    ) -> Dict[str, Any]: ...
    async def delete_node_artifacts(self, *, graph_id: str, node_id: str) -> Dict[str, Any]: ...
    async def write_snapshot_from_file(
        self,
        *,
        snapshot_id: str,
        file_path: str | Path,
        metadata: Dict[str, Any],
        mime_type: Optional[str] = None,
    ) -> str: ...
    async def get_snapshot_metadata(self, snapshot_id: str) -> Optional[Dict[str, Any]]: ...
    async def get_latest_node_artifact(
        self,
        *,
        graph_id: str,
        node_id: str,
        exclude_artifact_id: Optional[str] = None,
    ) -> Optional[str]: ...
    async def upsert_run_experiment(self, summary: Dict[str, Any]) -> None: ...
    async def get_run_experiment(self, run_id: str) -> Optional[Dict[str, Any]]: ...
    async def get_run_graph_id(self, run_id: str) -> Optional[str]: ...
    async def list_run_experiments(
        self,
        *,
        graph_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]: ...
    async def put_checkpoint_pins(self, graph_id: str, pins: List[Tuple[str, str]]) -> None: ...
    async def get_checkpoint_pinned_artifact_ids(self) -> Set[str]: ...
    async def record_artifact_usage(self, run_id: str, artifact_id: str) -> None: ...


# ----------------------------
# In-memory implementation
# ----------------------------

class MemoryArtifactStore:
    """
    Minimal, correct, async-compatible artifact store.
    - Metadata stored separately from bytes
    - storage_uri uses memory://<artifact_id>
    """
    def __init__(self) -> None:
        self._meta: Dict[str, Artifact] = {}
        self._blob: Dict[str, bytes] = {}
        self._runs: Dict[str, Dict[str, Any]] = {}
        self._run_pause_snapshots: Dict[str, Dict[str, Any]] = {}
        self._consumers: Dict[str, List[Dict[str, Any]]] = {}
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._experiments: Dict[str, Dict[str, Any]] = {}
        self._meta_cache: Dict[str, Artifact] = {}
        self._blob_cache: Dict[str, bytes] = {}
        self._memo_stats: Dict[str, int] = {
            "meta_hit": 0,
            "meta_miss": 0,
            "blob_hit": 0,
            "blob_miss": 0,
        }
        self._checkpoint_pins: Dict[str, Dict[str, str]] = {}  # graph_id -> {node_id: artifact_id}
        # Tracks which artifact_ids each run consumed (including cache hits that
        # don't create a new artifact).  Used by retention sweep to protect shared
        # artifacts that are still referenced by surviving runs even when the run
        # that *wrote* them is being pruned.
        self._run_artifact_refs: Dict[str, Set[str]] = {}

    def _prune_node_artifacts(self, *, graph_id: str, node_id: str, keep_last: int = 5) -> List[str]:
        if not graph_id or not node_id:
            return []
        rows = [
            (aid, art)
            for aid, art in self._meta.items()
            if str(art.graph_id or "") == str(graph_id) and str(art.node_id or "") == str(node_id)
        ]
        rows.sort(key=lambda x: x[1].created_at, reverse=True)
        keep = max(0, int(keep_last))
        to_delete = [aid for aid, _ in rows[keep:]]
        if not to_delete:
            return []
        # Protect checkpoint-pinned artifacts from per-node pruning.
        pinned_artifact_ids = set()
        for node_map in self._checkpoint_pins.values():
            pinned_artifact_ids.update(node_map.values())
        to_delete = [aid for aid in to_delete if aid not in pinned_artifact_ids]
        if not to_delete:
            return []
        delete_set = set(to_delete)
        for aid in to_delete:
            self._meta.pop(aid, None)
            self._blob.pop(aid, None)
            self._meta_cache.pop(aid, None)
            self._blob_cache.pop(aid, None)
        for input_id, consumers in list(self._consumers.items()):
            self._consumers[input_id] = [c for c in consumers if c.get("outputArtifactId") not in delete_set]
            if not self._consumers[input_id]:
                self._consumers.pop(input_id, None)
        return to_delete

    async def exists(self, artifact_id: str) -> bool:
        return artifact_id in self._meta

    async def get(self, artifact_id: str) -> Artifact:
        cached = self._meta_cache.get(artifact_id)
        if cached is not None:
            self._memo_stats["meta_hit"] += 1
            return cached
        self._memo_stats["meta_miss"] += 1
        if artifact_id not in self._meta:
            raise KeyError(f"Artifact not found: {artifact_id}")
        art = self._meta[artifact_id]
        self._meta_cache[artifact_id] = art
        return art

    async def read(self, artifact_id: str) -> bytes:
        cached = self._blob_cache.get(artifact_id)
        if cached is not None:
            self._memo_stats["blob_hit"] += 1
            return cached
        self._memo_stats["blob_miss"] += 1
        if artifact_id not in self._blob:
            raise KeyError(f"Artifact bytes not found: {artifact_id}")
        data = self._blob[artifact_id]
        self._blob_cache[artifact_id] = data
        return data

    async def open_payload(self, artifact_id: str) -> AsyncIterator[bytes]:
        data = await self.read(artifact_id)
        chunk_size = 64 * 1024
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]

    async def write(self, artifact: Artifact, data: bytes) -> str:
        _validate_artifact_metadata_v1(artifact)
        if artifact.run_id and (
            not artifact.graph_id or not artifact.node_id or not artifact.exec_key
        ):
            raise ValueError("Runtime artifact writes require graph_id, node_id, and exec_key")
        if artifact.run_id and not str(artifact.params_hash or "").strip():
            raise ValueError("Runtime artifact writes require non-empty params_hash (node_state_hash)")
        if artifact.exec_key and artifact.artifact_id != artifact.exec_key:
            raise ValueError("artifact_id must equal exec_key when exec_key is present")
        # Enforce immutability: don't overwrite
        if artifact.artifact_id in self._meta:
            logger.debug(
                "artifact_write_skip_existing store=memory artifact_id=%s run_id=%s node_id=%s exec_key=%s",
                artifact.artifact_id,
                artifact.run_id,
                artifact.node_id,
                artifact.exec_key,
            )
            return artifact.artifact_id
        content_hash = hashlib.sha256(data).hexdigest()
        logger.debug(
            "artifact_write store=memory artifact_id=%s run_id=%s node_id=%s exec_key=%s size_bytes=%s content_hash=%s",
            artifact.artifact_id,
            artifact.run_id,
            artifact.node_id,
            artifact.exec_key,
            len(data),
            content_hash,
        )
        artifact_to_store = artifact.model_copy(
            update={"content_hash": content_hash, "size_bytes": len(data)}
        )
        # Atomic commit order:
        # 1) payload bytes
        # 2) metadata row
        # 3) validate committed artifact
        self._blob[artifact.artifact_id] = data
        self._meta[artifact.artifact_id] = artifact_to_store
        self._blob_cache[artifact.artifact_id] = data
        self._meta_cache[artifact.artifact_id] = artifact_to_store
        committed = self._meta.get(artifact.artifact_id)
        if committed is None:
            raise RuntimeError(f"Artifact metadata commit failed: {artifact.artifact_id}")
        if artifact.artifact_id not in self._blob:
            raise RuntimeError(f"Artifact payload commit failed: {artifact.artifact_id}")
        if str(committed.content_hash or "") != content_hash:
            raise RuntimeError(
                f"Artifact commit validation failed (content_hash mismatch): {artifact.artifact_id}"
            )
        if int(committed.size_bytes or -1) != len(data):
            raise RuntimeError(
                f"Artifact commit validation failed (size mismatch): {artifact.artifact_id}"
            )
        return artifact.artifact_id

    async def record_artifact_usage(self, run_id: str, artifact_id: str) -> None:
        """Record that *run_id* used *artifact_id* (even when the artifact was
        created by an earlier run and this run only consumed it via a cache hit).
        This information is used by the retention sweep to protect artifacts that
        are still needed by surviving runs from being deleted simply because the
        run that originally *wrote* the artifact is old enough to be pruned."""
        rid = str(run_id or "").strip()
        aid = str(artifact_id or "").strip()
        if not rid or not aid:
            return
        if rid not in self._run_artifact_refs:
            self._run_artifact_refs[rid] = set()
        self._run_artifact_refs[rid].add(aid)

    async def record_run(self, run_id: str, status: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        is_terminal = _is_terminal_status(status)
        self._runs[run_id] = {
            "run_id": run_id,
            "created_at": now,
            "updated_at": now,
            "completed_at": now if is_terminal else None,
            "status": status,
            "deleted_at": None,
        }

    async def update_run_status(self, run_id: str, status: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        is_terminal = _is_terminal_status(status)
        rec = self._runs.get(run_id)
        if not rec:
            await self.record_run(run_id, status)
        else:
            rec["status"] = status
            rec["updated_at"] = now
            if is_terminal:
                rec["completed_at"] = now
        if is_terminal:
            mode = _retention_mode()
            if mode == "by_run":
                await self._apply_run_retention(trigger_run_id=run_id)
            elif mode == "by_node":
                await self._apply_node_retention(trigger_run_id=run_id)

    async def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        return self._runs.get(run_id)

    async def list_runs(self, include_deleted: bool = False) -> List[Dict[str, Any]]:
        runs = list(self._runs.values())
        if not include_deleted:
            runs = [r for r in runs if r.get("status") != "deleted"]
        runs.sort(key=lambda r: r.get("completed_at") or r.get("created_at", ""), reverse=True)
        return runs

    async def _apply_run_retention(self, *, trigger_run_id: str) -> None:
        if _retention_mode() != "by_run":
            return
        keep_recent = _retention_keep_recent_runs()
        active_statuses = {"pending", "running", "cancel_requested", "pausing", "paused", "resuming"}
        candidates = []
        for run_id, rec in list(self._runs.items()):
            status = str(rec.get("status") or "").strip().lower()
            if status == "deleted" or status in active_statuses:
                continue
            if not _is_terminal_status(status):
                continue
            if not _should_include_status_for_retention(status):
                continue
            candidates.append(
                {
                    "run_id": run_id,
                    "status": status,
                    "completed_at": str(rec.get("completed_at") or rec.get("created_at") or ""),
                }
            )
        candidates.sort(key=lambda row: row.get("completed_at") or "", reverse=True)
        keep_ids = {str(row.get("run_id") or "") for row in candidates[:keep_recent]}
        prune_ids = [str(row.get("run_id") or "") for row in candidates if str(row.get("run_id") or "") not in keep_ids]
        if not prune_ids:
            return
        # Build the set of artifact IDs that must survive pruning:
        # (a) checkpoint-pinned artifacts, and
        # (b) artifacts referenced (via cache hit) by any run we are keeping —
        #     even if those artifacts were *written* by a run we are pruning.
        # NOTE: by_run retention has known limitations with partial "run-from-selected"
        # runs.  Use ARTIFACT_RETENTION_MODE=by_node to avoid false evictions.
        pinned_artifact_ids = await self.get_checkpoint_pinned_artifact_ids()
        for rid in keep_ids:
            pinned_artifact_ids |= self._run_artifact_refs.get(rid, set())
        logger.info(
            "artifact_retention_sweep_started mode=by_run keep_recent_runs=%s trigger_run_id=%s terminal_candidates=%s prune_candidates=%s pinned_artifacts=%s",
            keep_recent,
            trigger_run_id,
            len(candidates),
            len(prune_ids),
            len(pinned_artifact_ids),
        )
        pruned_run_ids: List[str] = []
        removed_artifacts = 0
        pinned_protected = 0
        for rid in prune_ids:
            # Find artifacts belonging to this run that are NOT checkpoint-pinned.
            run_artifact_ids = [
                aid for aid, art in self._meta.items()
                if str(art.run_id or "").strip() == rid
            ]
            pinned_in_run = [aid for aid in run_artifact_ids if aid in pinned_artifact_ids]
            if pinned_in_run:
                # Only delete non-pinned artifacts; keep the run record alive
                # so pinned artifacts remain accessible.
                to_delete = [aid for aid in run_artifact_ids if aid not in pinned_artifact_ids]
                for aid in to_delete:
                    self._meta.pop(aid, None)
                    self._blob.pop(aid, None)
                    self._meta_cache.pop(aid, None)
                    self._blob_cache.pop(aid, None)
                for input_id, consumers in list(self._consumers.items()):
                    self._consumers[input_id] = [
                        c for c in consumers
                        if c.get("outputArtifactId") not in set(to_delete)
                    ]
                    if not self._consumers[input_id]:
                        self._consumers.pop(input_id, None)
                removed = len(to_delete)
                pinned_protected += len(pinned_in_run)
                logger.info(
                    "artifact_retention_run_partial_prune run_id=%s artifacts_removed=%s pinned_protected=%s",
                    rid,
                    removed,
                    len(pinned_in_run),
                )
            else:
                out = await self.delete_run(rid, mode="hard", gc="unreferenced")
                if bool(out.get("runDeleted")):
                    pruned_run_ids.append(rid)
                    removed_artifacts += int(out.get("artifactsRemoved") or 0)
                    logger.info(
                        "artifact_retention_run_pruned run_id=%s artifacts_removed=%s blobs_deleted=%s",
                        rid,
                        int(out.get("artifactsRemoved") or 0),
                        int(out.get("blobsDeleted") or 0),
                    )
        logger.info(
            "artifact_retention_sweep_finished mode=by_run keep_recent_runs=%s trigger_run_id=%s pruned_runs=%s artifacts_removed=%s pinned_protected=%s",
            keep_recent,
            trigger_run_id,
            len(pruned_run_ids),
            removed_artifacts,
            pinned_protected,
        )

    async def _apply_node_retention(self, *, trigger_run_id: str) -> None:
        """Retention by node: keep the N most-recent distinct artifacts per
        (graph_id, node_id).  Checkpoint-pinned artifacts are never deleted.
        Artifacts without a graph_id or node_id (non-runtime artifacts) are
        left untouched."""
        keep_versions = _retention_keep_node_versions()
        pinned_artifact_ids = await self.get_checkpoint_pinned_artifact_ids()

        # Group artifacts by (graph_id, node_id).
        groups: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
        for aid, art in list(self._meta.items()):
            gid = str(art.graph_id or "").strip()
            nid = str(art.node_id or "").strip()
            if not gid or not nid:
                continue
            key = (gid, nid)
            groups.setdefault(key, []).append((str(art.created_at or ""), aid))

        to_delete: List[str] = []
        pinned_protected = 0
        for entries in groups.values():
            entries.sort(key=lambda x: x[0], reverse=True)  # newest first
            for i, (_, aid) in enumerate(entries):
                if i < keep_versions:
                    continue  # within the keep window
                if aid in pinned_artifact_ids:
                    pinned_protected += 1
                    continue  # checkpoint-pinned — never delete
                to_delete.append(aid)

        if not to_delete:
            return

        to_delete_set = set(to_delete)
        for aid in to_delete:
            self._meta.pop(aid, None)
            self._blob.pop(aid, None)
            self._meta_cache.pop(aid, None)
            self._blob_cache.pop(aid, None)
        for input_id, consumers in list(self._consumers.items()):
            self._consumers[input_id] = [
                c for c in consumers
                if c.get("outputArtifactId") not in to_delete_set
            ]
            if not self._consumers[input_id]:
                self._consumers.pop(input_id, None)
        logger.info(
            "artifact_node_retention_sweep store=memory trigger_run_id=%s keep_versions=%s "
            "artifacts_pruned=%s pinned_protected=%s",
            trigger_run_id,
            keep_versions,
            len(to_delete),
            pinned_protected,
        )

    async def upsert_run_pause_snapshot(self, run_id: str, snapshot: Dict[str, Any]) -> None:
        rid = str(run_id or "").strip()
        if not rid:
            raise ValueError("run_id is required")
        self._run_pause_snapshots[rid] = dict(snapshot or {})

    async def get_run_pause_snapshot(self, run_id: str) -> Optional[Dict[str, Any]]:
        rid = str(run_id or "").strip()
        if not rid:
            return None
        snap = self._run_pause_snapshots.get(rid)
        return dict(snap) if isinstance(snap, dict) else None

    async def delete_run_pause_snapshot(self, run_id: str) -> None:
        rid = str(run_id or "").strip()
        if not rid:
            return
        self._run_pause_snapshots.pop(rid, None)

    async def delete_run(self, run_id: str, mode: str = "soft", gc: str = "none") -> Dict[str, Any]:
        mode = (mode or "soft").lower()
        if mode not in ("soft", "hard"):
            raise ValueError(f"Unsupported delete mode: {mode}")

        if mode == "soft":
            rec = self._runs.get(run_id)
            if not rec:
                return {"runDeleted": False, "mode": "soft", "artifactsRemoved": 0, "cacheRowsRemoved": 0, "blobsDeleted": 0, "artifactIdsRemoved": []}
            rec["status"] = "deleted"
            rec["deleted_at"] = datetime.now(timezone.utc).isoformat()
            exp = self._experiments.get(run_id)
            if isinstance(exp, dict):
                exp["status"] = "deleted"
            return {"runDeleted": True, "mode": "soft", "artifactsRemoved": 0, "cacheRowsRemoved": 0, "blobsDeleted": 0, "artifactIdsRemoved": []}

        artifact_ids = []
        for aid, art in list(self._meta.items()):
            if art.run_id == run_id:
                artifact_ids.append(aid)
                self._meta.pop(aid, None)
                self._blob.pop(aid, None)
                self._meta_cache.pop(aid, None)
                self._blob_cache.pop(aid, None)
        for input_id, rows in list(self._consumers.items()):
            self._consumers[input_id] = [
                r
                for r in rows
                if r.get("consumerRunId") != run_id and r.get("outputArtifactId") not in artifact_ids
            ]
            if not self._consumers[input_id]:
                self._consumers.pop(input_id, None)
        run_deleted = (self._runs.pop(run_id, None) is not None) or bool(artifact_ids)
        self._run_pause_snapshots.pop(str(run_id), None)
        self._experiments.pop(run_id, None)
        self._run_artifact_refs.pop(run_id, None)
        return {
            "runDeleted": run_deleted,
            "mode": "hard",
            "artifactsRemoved": len(artifact_ids),
            "cacheRowsRemoved": 0,
            "blobsDeleted": len(artifact_ids),
            "artifactIdsRemoved": artifact_ids,
        }

    async def record_consumers(
        self,
        *,
        input_artifact_ids: List[str],
        consumer_run_id: str,
        consumer_node_id: str,
        consumer_exec_key: Optional[str],
        output_artifact_id: str,
    ) -> None:
        created_at = datetime.now(timezone.utc).isoformat()
        for input_id in sorted(set(input_artifact_ids or [])):
            if not input_id:
                continue
            row = {
                "inputArtifactId": input_id,
                "consumerRunId": consumer_run_id,
                "consumerNodeId": consumer_node_id,
                "consumerExecKey": consumer_exec_key,
                "outputArtifactId": output_artifact_id,
                "createdAt": created_at,
            }
            rows = self._consumers.setdefault(input_id, [])
            exists = any(
                r.get("consumerRunId") == consumer_run_id
                and r.get("consumerNodeId") == consumer_node_id
                and r.get("outputArtifactId") == output_artifact_id
                for r in rows
            )
            if not exists:
                rows.append(row)

    async def get_consumers(self, artifact_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        rows = list(self._consumers.get(artifact_id, []))
        rows.sort(key=lambda r: str(r.get("createdAt", "")), reverse=True)
        return rows[: max(1, int(limit))]

    async def gc_orphan_blobs(
        self, mode: str = "dry_run", limit: Optional[int] = None, max_seconds: Optional[int] = None
    ) -> Dict[str, Any]:
        mode = (mode or "dry_run").lower()
        if mode not in ("dry_run", "delete"):
            raise ValueError("mode must be 'dry_run' or 'delete'")
        return {
            "mode": mode,
            "referenced_hashes": 0,
            "orphan_hashes": [],
            "blobs_deleted": 0,
            "scanned_blobs": 0,
        }

    async def delete_node_artifacts(self, *, graph_id: str, node_id: str) -> Dict[str, Any]:
        ids = [
            aid
            for aid, art in self._meta.items()
            if str(art.graph_id or "") == str(graph_id) and str(art.node_id or "") == str(node_id)
        ]
        if not ids:
            return {"graphId": graph_id, "nodeId": node_id, "artifactsRemoved": 0, "artifactIdsRemoved": []}
        delete_set = set(ids)
        for aid in ids:
            self._meta.pop(aid, None)
            self._blob.pop(aid, None)
            self._meta_cache.pop(aid, None)
            self._blob_cache.pop(aid, None)
        for input_id, consumers in list(self._consumers.items()):
            self._consumers[input_id] = [c for c in consumers if c.get("outputArtifactId") not in delete_set]
            if not self._consumers[input_id]:
                self._consumers.pop(input_id, None)
        return {"graphId": graph_id, "nodeId": node_id, "artifactsRemoved": len(ids), "artifactIdsRemoved": ids}

    async def write_snapshot_from_file(
        self,
        *,
        snapshot_id: str,
        file_path: str | Path,
        metadata: Dict[str, Any],
        mime_type: Optional[str] = None,
    ) -> str:
        sid = str(snapshot_id or "").strip().lower()
        if not sid:
            raise ValueError("snapshot_id is required")
        path = Path(file_path)
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != sid:
            raise ValueError("snapshot_id must equal SHA-256(file_bytes)")
        art = Artifact(
            artifact_id=sid,
            node_kind="snapshot",
            params_hash="snapshot",
            upstream_ids=[],
            created_at=datetime.now(timezone.utc),
            execution_version="snapshot_v1",
            mime_type=mime_type or str(metadata.get("mimeType") or "application/octet-stream"),
            payload_type="binary",
            size_bytes=len(data),
            storage_uri=f"memory://snapshots/{sid}",
            payload_schema={"schema_version": 1, "type": "binary", "snapshot": True},
            graph_id="__snapshots__",
            node_id=None,
            run_id=None,
            exec_key=None,
        )
        await self.write(art, data)
        self._snapshots[sid] = dict(metadata or {})
        return sid

    async def get_snapshot_metadata(self, snapshot_id: str) -> Optional[Dict[str, Any]]:
        sid = str(snapshot_id or "").strip().lower()
        if not sid:
            return None
        meta = self._snapshots.get(sid)
        return dict(meta) if isinstance(meta, dict) else None

    async def get_latest_node_artifact(
        self,
        *,
        graph_id: str,
        node_id: str,
        exclude_artifact_id: Optional[str] = None,
    ) -> Optional[str]:
        gid = str(graph_id or "").strip()
        nid = str(node_id or "").strip()
        if not gid or not nid:
            return None
        ex = str(exclude_artifact_id or "").strip()
        rows: List[Tuple[str, Artifact]] = []
        for aid, art in self._meta.items():
            if str(art.graph_id or "") != gid:
                continue
            if str(art.node_id or "") != nid:
                continue
            if ex and str(aid) == ex:
                continue
            rows.append((aid, art))
        if not rows:
            return None
        rows.sort(key=lambda x: x[1].created_at, reverse=True)
        return str(rows[0][0])

    async def put_checkpoint_pins(self, graph_id: str, pins: List[Tuple[str, str]]) -> None:
        """Replace all checkpoint pins for *graph_id* with *pins* ``[(artifact_id, node_id), ...]``."""
        gid = str(graph_id or "").strip()
        if not gid:
            return
        self._checkpoint_pins[gid] = {str(nid): str(aid) for aid, nid in pins if aid and nid}

    async def get_checkpoint_pinned_artifact_ids(self) -> Set[str]:
        """Return the set of artifact IDs that are currently checkpoint-pinned (across all graphs)."""
        result: Set[str] = set()
        for node_map in self._checkpoint_pins.values():
            result.update(node_map.values())
        return result

    async def upsert_run_experiment(self, summary: Dict[str, Any]) -> None:
        if not isinstance(summary, dict):
            raise ValueError("summary must be a dict")
        run_id = str(summary.get("runId") or "").strip()
        if not run_id:
            raise ValueError("summary.runId is required")
        graph_id = str(summary.get("graphId") or "").strip()
        created_at = str(summary.get("createdAt") or datetime.now(timezone.utc).isoformat())
        status = str(summary.get("status") or "unknown")
        metrics = summary.get("metrics") if isinstance(summary.get("metrics"), dict) else {}
        analytics = summary.get("analytics") if isinstance(summary.get("analytics"), dict) else {}
        metrics_out = dict(metrics)
        if analytics:
            metrics_out["__analytics"] = analytics
        self._experiments[run_id] = {
            "runId": run_id,
            "graphId": graph_id,
            "createdAt": created_at,
            "status": status,
            "params": summary.get("params") if isinstance(summary.get("params"), dict) else {},
            "metrics": metrics_out,
            "analytics": analytics,
            "environment": summary.get("environment") if isinstance(summary.get("environment"), dict) else {},
            "artifacts": summary.get("artifacts") if isinstance(summary.get("artifacts"), list) else [],
            "artifactIds": summary.get("artifactIds") if isinstance(summary.get("artifactIds"), list) else [],
        }

    async def get_run_experiment(self, run_id: str) -> Optional[Dict[str, Any]]:
        rid = str(run_id or "").strip()
        if not rid:
            return None
        row = self._experiments.get(rid)
        if not isinstance(row, dict):
            return None
        out = dict(row)
        if not isinstance(out.get("analytics"), dict):
            metrics = out.get("metrics") if isinstance(out.get("metrics"), dict) else {}
            out["analytics"] = metrics.get("__analytics") if isinstance(metrics.get("__analytics"), dict) else {}
        return out

    async def get_run_graph_id(self, run_id: str) -> Optional[str]:
        rid = str(run_id or "").strip()
        if not rid:
            return None
        experiment = self._experiments.get(rid)
        if isinstance(experiment, dict):
            from_experiment = str(experiment.get("graphId") or "").strip()
            if from_experiment:
                return from_experiment
        for art in self._meta.values():
            if str(art.run_id or "").strip() != rid:
                continue
            gid = str(art.graph_id or "").strip()
            if gid:
                return gid
        return None

    async def list_run_experiments(
        self,
        *,
        graph_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        gid = str(graph_id or "").strip()
        rows = list(self._experiments.values())
        if gid:
            rows = [r for r in rows if str(r.get("graphId") or "") == gid]
        rows.sort(key=lambda r: str(r.get("createdAt") or ""), reverse=True)
        start = max(0, int(offset))
        end = start + max(1, int(limit))
        out: List[Dict[str, Any]] = []
        for row in rows[start:end]:
            if not isinstance(row, dict):
                continue
            rec = dict(row)
            if not isinstance(rec.get("analytics"), dict):
                metrics = rec.get("metrics") if isinstance(rec.get("metrics"), dict) else {}
                rec["analytics"] = metrics.get("__analytics") if isinstance(metrics.get("__analytics"), dict) else {}
            out.append(rec)
        return out

    def get_memo_stats(self) -> Dict[str, int]:
        return {
            **self._memo_stats,
            "meta_entries": int(len(self._meta_cache)),
            "blob_entries": int(len(self._blob_cache)),
        }


class _SqliteArtifactIndex:
    def __init__(self, db_path: Path, *, blob_root: Optional[Path] = None) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._blob_root = blob_root
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

    def _blob_path(self, content_hash: str) -> Path:
        if self._blob_root is None:
            raise RuntimeError("Blob root is not configured for sqlite artifact index")
        ch = content_hash.lower()
        return self._blob_root / ch[:2] / ch[2:4] / f"{ch}.bin"

    def _init_schema(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    content_hash TEXT NOT NULL,
                    node_kind TEXT NOT NULL,
                    params_hash TEXT NOT NULL,
                    upstream_ids_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    execution_version TEXT NOT NULL,
                    mime_type TEXT,
                    payload_type TEXT,
                    size_bytes INTEGER NOT NULL,
                    storage_uri TEXT NOT NULL,
                    payload_schema_json TEXT,
                    run_id TEXT,
                    graph_id TEXT,
                    node_id TEXT,
                    exec_key TEXT
                )
                """
            )
            # Migration: older DBs won't have payload_type.
            cols = [r[1] for r in cur.execute("PRAGMA table_info(artifacts)").fetchall()]
            if "payload_type" not in cols:
                cur.execute("ALTER TABLE artifacts ADD COLUMN payload_type TEXT")
            if "graph_id" not in cols:
                cur.execute("ALTER TABLE artifacts ADD COLUMN graph_id TEXT")
            if "node_id" not in cols:
                cur.execute("ALTER TABLE artifacts ADD COLUMN node_id TEXT")
            if "exec_key" not in cols:
                cur.execute("ALTER TABLE artifacts ADD COLUMN exec_key TEXT")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_run_id ON artifacts(run_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_graph_id ON artifacts(graph_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_node_id ON artifacts(node_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_graph_node ON artifacts(graph_id, node_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_graph_node_created ON artifacts(graph_id, node_id, created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_exec_key ON artifacts(exec_key)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_content_hash ON artifacts(content_hash)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_payload_type ON artifacts(payload_type)")
            # Backfill legacy rows with null payload_type.
            null_rows = cur.execute(
                """
                SELECT artifact_id, mime_type, payload_schema_json, node_kind
                FROM artifacts
                WHERE payload_type IS NULL OR TRIM(payload_type) = ''
                """
            ).fetchall()
            for aid, mime_type, payload_schema_json, node_kind in null_rows:
                payload_schema = None
                if payload_schema_json:
                    try:
                        payload_schema = json.loads(payload_schema_json)
                    except Exception:
                        payload_schema = None
                inferred = _infer_payload_type(
                    payload_schema=payload_schema,
                    mime_type=mime_type,
                    node_kind=node_kind,
                )
                cur.execute(
                    "UPDATE artifacts SET payload_type=? WHERE artifact_id=?",
                    (inferred, aid),
                )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS artifact_consumers (
                    input_artifact_id TEXT NOT NULL,
                    consumer_run_id TEXT NOT NULL,
                    consumer_node_id TEXT NOT NULL,
                    consumer_exec_key TEXT,
                    output_artifact_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(input_artifact_id, consumer_run_id, consumer_node_id, output_artifact_id)
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_artifact_consumers_input ON artifact_consumers(input_artifact_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_artifact_consumers_run ON artifact_consumers(consumer_run_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_artifact_consumers_output ON artifact_consumers(output_artifact_id)"
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    updated_at TEXT,
                    completed_at TEXT,
                    status TEXT NOT NULL,
                    deleted_at TEXT
                )
                """
            )
            run_cols = [r[1] for r in cur.execute("PRAGMA table_info(runs)").fetchall()]
            if "updated_at" not in run_cols:
                cur.execute("ALTER TABLE runs ADD COLUMN updated_at TEXT")
            if "completed_at" not in run_cols:
                cur.execute("ALTER TABLE runs ADD COLUMN completed_at TEXT")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_runs_completed_at ON runs(completed_at)")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS run_pause_snapshots (
                    run_id TEXT PRIMARY KEY,
                    snapshot_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_run_pause_snapshots_updated_at ON run_pause_snapshots(updated_at)")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON snapshots(created_at)")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS run_experiments (
                    run_id TEXT PRIMARY KEY,
                    graph_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    params_json TEXT NOT NULL,
                    metrics_json TEXT NOT NULL,
                    environment_json TEXT NOT NULL,
                    artifacts_json TEXT NOT NULL,
                    artifact_ids_json TEXT NOT NULL
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_run_experiments_graph_created ON run_experiments(graph_id, created_at DESC)")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS checkpoint_pins (
                    graph_id TEXT NOT NULL,
                    artifact_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (graph_id, node_id)
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_checkpoint_pins_artifact ON checkpoint_pins(artifact_id)")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS run_artifact_refs (
                    run_id TEXT NOT NULL,
                    artifact_id TEXT NOT NULL,
                    PRIMARY KEY (run_id, artifact_id)
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_run_artifact_refs_run ON run_artifact_refs(run_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_run_artifact_refs_artifact ON run_artifact_refs(artifact_id)")
            self._conn.commit()

    def exists(self, artifact_id: str) -> bool:
        with self._lock:
            cur = self._conn.cursor()
            row = cur.execute(
                "SELECT 1 FROM artifacts WHERE artifact_id=? LIMIT 1", (artifact_id,)
            ).fetchone()
            return bool(row)

    def get(self, artifact_id: str) -> Artifact:
        with self._lock:
            cur = self._conn.cursor()
            row = cur.execute(
                """
                SELECT artifact_id, content_hash, node_kind, params_hash, upstream_ids_json,
                       created_at, execution_version, mime_type, payload_type, size_bytes, storage_uri,
                       payload_schema_json, run_id, graph_id, node_id, exec_key
                FROM artifacts
                WHERE artifact_id=?
                """,
                (artifact_id,),
            ).fetchone()
        if not row:
            raise KeyError(f"Artifact not found: {artifact_id}")

        payload_schema = json.loads(row[11]) if row[11] else None
        payload_schema = _normalize_payload_schema(payload_schema)
        created_at = datetime.fromisoformat(row[5])
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        inferred_payload_type = _infer_payload_type(payload_schema=payload_schema, mime_type=row[7], node_kind=row[2])

        return Artifact(
            artifact_id=row[0],
            content_hash=row[1],
            node_kind=row[2],
            params_hash=row[3],
            upstream_ids=json.loads(row[4]),
            created_at=created_at,
            execution_version=row[6],
            mime_type=row[7] or "application/octet-stream",
            payload_type=row[8] or inferred_payload_type,
            size_bytes=int(row[9]),
            storage_uri=row[10],
            payload_schema=payload_schema,
            run_id=row[12],
            graph_id=row[13],
            node_id=row[14],
            exec_key=row[15],
        )

    def put(self, artifact: Artifact) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT OR IGNORE INTO artifacts (
                    artifact_id, content_hash, node_kind, params_hash, upstream_ids_json,
                    created_at, execution_version, mime_type, payload_type, size_bytes, storage_uri,
                    payload_schema_json, run_id, graph_id, node_id, exec_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact.artifact_id,
                    artifact.content_hash,
                    artifact.node_kind,
                    artifact.params_hash,
                    json.dumps(artifact.upstream_ids or [], ensure_ascii=False),
                    artifact.created_at.isoformat(),
                    artifact.execution_version,
                    artifact.mime_type,
                    artifact.payload_type,
                    int(artifact.size_bytes),
                    artifact.storage_uri,
                    json.dumps(artifact.payload_schema, ensure_ascii=False)
                    if artifact.payload_schema is not None
                    else None,
                    artifact.run_id,
                    artifact.graph_id,
                    artifact.node_id,
                    artifact.exec_key,
                ),
            )
            self._conn.commit()

    def _prune_node_artifacts_locked(
        self,
        *,
        cur: sqlite3.Cursor,
        graph_id: str,
        node_id: str,
        keep_last: int = 5,
    ) -> List[str]:
        keep = max(0, int(keep_last))
        rows = cur.execute(
            """
            SELECT artifact_id, content_hash
            FROM artifacts
            WHERE graph_id=? AND node_id=?
            ORDER BY created_at DESC
            """,
            (graph_id, node_id),
        ).fetchall()
        to_delete = rows[keep:]
        if not to_delete:
            return []
        # Protect checkpoint-pinned artifacts from per-node pruning.
        pinned_artifact_ids = self.get_checkpoint_pinned_artifact_ids(cur=cur)
        to_delete = [(aid, ch) for aid, ch in to_delete if aid not in pinned_artifact_ids]
        if not to_delete:
            return []
        ids = [r[0] for r in to_delete]
        hashes = [r[1] for r in to_delete if r[1]]
        placeholders = ",".join(["?"] * len(ids))
        cur.execute(
            f"DELETE FROM artifact_consumers WHERE output_artifact_id IN ({placeholders})",
            tuple(ids),
        )
        cur.execute(
            f"DELETE FROM artifacts WHERE artifact_id IN ({placeholders})",
            tuple(ids),
        )
        for content_hash in hashes:
            still_used = cur.execute(
                "SELECT 1 FROM artifacts WHERE content_hash=? LIMIT 1",
                (content_hash,),
            ).fetchone()
            if still_used:
                continue
            try:
                path = Path(self._blob_path(content_hash))
                if path.exists():
                    path.unlink()
            except Exception:
                pass
        return ids

    def delete_node_artifacts(self, *, graph_id: str, node_id: str) -> Dict[str, Any]:
        with self._lock:
            cur = self._conn.cursor()
            rows = cur.execute(
                """
                SELECT artifact_id, content_hash
                FROM artifacts
                WHERE graph_id=? AND node_id=?
                ORDER BY created_at DESC
                """,
                (graph_id, node_id),
            ).fetchall()
            if not rows:
                return {
                    "graphId": graph_id,
                    "nodeId": node_id,
                    "artifactsRemoved": 0,
                    "artifactIdsRemoved": [],
                    "blobsDeleted": 0,
                }
            ids = [r[0] for r in rows]
            hashes = [r[1] for r in rows if r[1]]
            placeholders = ",".join(["?"] * len(ids))
            cur.execute(
                f"DELETE FROM artifact_consumers WHERE output_artifact_id IN ({placeholders})",
                tuple(ids),
            )
            cur.execute(
                f"DELETE FROM artifacts WHERE artifact_id IN ({placeholders})",
                tuple(ids),
            )
            blobs_deleted = 0
            for content_hash in hashes:
                still_used = cur.execute(
                    "SELECT 1 FROM artifacts WHERE content_hash=? LIMIT 1",
                    (content_hash,),
                ).fetchone()
                if still_used:
                    continue
                try:
                    path = Path(self._blob_path(content_hash))
                    if path.exists():
                        path.unlink()
                        blobs_deleted += 1
                except Exception:
                    pass
            self._conn.commit()
        return {
            "graphId": graph_id,
            "nodeId": node_id,
            "artifactsRemoved": len(ids),
            "artifactIdsRemoved": ids,
            "blobsDeleted": blobs_deleted,
        }

    def record_run(self, run_id: str, status: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        is_terminal = _is_terminal_status(status)
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT INTO runs (run_id, created_at, updated_at, completed_at, status, deleted_at)
                VALUES (?, ?, ?, ?, ?, NULL)
                ON CONFLICT(run_id) DO UPDATE SET
                    updated_at=excluded.updated_at,
                    completed_at=COALESCE(excluded.completed_at, runs.completed_at),
                    status=excluded.status
                """,
                (run_id, now, now, now if is_terminal else None, status),
            )
            self._conn.commit()

    def update_run_status(self, run_id: str, status: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        is_terminal = _is_terminal_status(status)
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT INTO runs (run_id, created_at, updated_at, completed_at, status, deleted_at)
                VALUES (?, ?, ?, ?, ?, NULL)
                ON CONFLICT(run_id) DO UPDATE SET
                    updated_at=excluded.updated_at,
                    completed_at=CASE
                        WHEN excluded.completed_at IS NOT NULL THEN excluded.completed_at
                        ELSE runs.completed_at
                    END,
                    status=excluded.status
                """,
                (run_id, now, now, now if is_terminal else None, status),
            )
            if is_terminal:
                mode = _retention_mode()
                if mode == "by_run":
                    self._apply_run_retention_locked(cur=cur, trigger_run_id=run_id)
                elif mode == "by_node":
                    self._apply_node_retention_locked(cur=cur, trigger_run_id=run_id)
            self._conn.commit()

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.cursor()
            row = cur.execute(
                "SELECT run_id, created_at, updated_at, completed_at, status, deleted_at FROM runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
        if not row:
            return None
        return {
            "run_id": row[0],
            "created_at": row[1],
            "updated_at": row[2],
            "completed_at": row[3],
            "status": row[4],
            "deleted_at": row[5],
        }

    def list_runs(self, include_deleted: bool = False) -> List[Dict[str, Any]]:
        where = "" if include_deleted else "WHERE status <> 'deleted'"
        with self._lock:
            cur = self._conn.cursor()
            rows = cur.execute(
                f"""
                SELECT run_id, created_at, updated_at, completed_at, status, deleted_at
                FROM runs
                {where}
                ORDER BY COALESCE(completed_at, created_at) DESC
                """
            ).fetchall()
        return [
            {
                "run_id": r[0],
                "created_at": r[1],
                "updated_at": r[2],
                "completed_at": r[3],
                "status": r[4],
                "deleted_at": r[5],
            }
            for r in rows
        ]

    def upsert_run_pause_snapshot(self, run_id: str, snapshot: Dict[str, Any]) -> None:
        rid = str(run_id or "").strip()
        if not rid:
            raise ValueError("run_id is required")
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT INTO run_pause_snapshots (run_id, snapshot_json, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    snapshot_json=excluded.snapshot_json,
                    updated_at=excluded.updated_at
                """,
                (rid, json.dumps(snapshot or {}, ensure_ascii=False), now, now),
            )
            self._conn.commit()

    def get_run_pause_snapshot(self, run_id: str) -> Optional[Dict[str, Any]]:
        rid = str(run_id or "").strip()
        if not rid:
            return None
        with self._lock:
            cur = self._conn.cursor()
            row = cur.execute(
                "SELECT snapshot_json FROM run_pause_snapshots WHERE run_id=?",
                (rid,),
            ).fetchone()
        if not row:
            return None
        try:
            parsed = json.loads(str(row[0] or "{}"))
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None

    def delete_run_pause_snapshot(self, run_id: str) -> None:
        rid = str(run_id or "").strip()
        if not rid:
            return
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("DELETE FROM run_pause_snapshots WHERE run_id=?", (rid,))
            self._conn.commit()

    def upsert_run_experiment(self, summary: Dict[str, Any]) -> None:
        if not isinstance(summary, dict):
            raise ValueError("summary must be a dict")
        run_id = str(summary.get("runId") or "").strip()
        graph_id = str(summary.get("graphId") or "").strip()
        if not run_id:
            raise ValueError("summary.runId is required")
        if not graph_id:
            raise ValueError("summary.graphId is required")
        created_at = str(summary.get("createdAt") or datetime.now(timezone.utc).isoformat())
        status = str(summary.get("status") or "unknown")
        params = summary.get("params") if isinstance(summary.get("params"), dict) else {}
        metrics = summary.get("metrics") if isinstance(summary.get("metrics"), dict) else {}
        analytics = summary.get("analytics") if isinstance(summary.get("analytics"), dict) else {}
        metrics_out = dict(metrics)
        if analytics:
            metrics_out["__analytics"] = analytics
        environment = summary.get("environment") if isinstance(summary.get("environment"), dict) else {}
        artifacts = summary.get("artifacts") if isinstance(summary.get("artifacts"), list) else []
        artifact_ids = summary.get("artifactIds") if isinstance(summary.get("artifactIds"), list) else []
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT INTO run_experiments (
                    run_id, graph_id, created_at, status,
                    params_json, metrics_json, environment_json, artifacts_json, artifact_ids_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    graph_id=excluded.graph_id,
                    created_at=excluded.created_at,
                    status=excluded.status,
                    params_json=excluded.params_json,
                    metrics_json=excluded.metrics_json,
                    environment_json=excluded.environment_json,
                    artifacts_json=excluded.artifacts_json,
                    artifact_ids_json=excluded.artifact_ids_json
                """,
                (
                    run_id,
                    graph_id,
                    created_at,
                    status,
                    json.dumps(params, ensure_ascii=False),
                    json.dumps(metrics_out, ensure_ascii=False),
                    json.dumps(environment, ensure_ascii=False),
                    json.dumps(artifacts, ensure_ascii=False),
                    json.dumps(artifact_ids, ensure_ascii=False),
                ),
            )
            self._conn.commit()

    def get_run_experiment(self, run_id: str) -> Optional[Dict[str, Any]]:
        rid = str(run_id or "").strip()
        if not rid:
            return None
        with self._lock:
            cur = self._conn.cursor()
            row = cur.execute(
                """
                SELECT run_id, graph_id, created_at, status, params_json, metrics_json, environment_json, artifacts_json, artifact_ids_json
                FROM run_experiments
                WHERE run_id=?
                """,
                (rid,),
            ).fetchone()
        if not row:
            return None
        metrics = json.loads(row[5] or "{}")
        analytics = metrics.get("__analytics") if isinstance(metrics, dict) and isinstance(metrics.get("__analytics"), dict) else {}
        return {
            "runId": str(row[0]),
            "graphId": str(row[1]),
            "createdAt": str(row[2]),
            "status": str(row[3]),
            "params": json.loads(row[4] or "{}"),
            "metrics": metrics,
            "analytics": analytics,
            "environment": json.loads(row[6] or "{}"),
            "artifacts": json.loads(row[7] or "[]"),
            "artifactIds": json.loads(row[8] or "[]"),
        }

    def get_run_graph_id(self, run_id: str) -> Optional[str]:
        rid = str(run_id or "").strip()
        if not rid:
            return None
        exp = self.get_run_experiment(rid)
        if isinstance(exp, dict):
            exp_gid = str(exp.get("graphId") or "").strip()
            if exp_gid:
                return exp_gid
        with self._lock:
            cur = self._conn.cursor()
            row = cur.execute(
                """
                SELECT graph_id
                FROM artifacts
                WHERE run_id=? AND graph_id IS NOT NULL AND graph_id <> ''
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (rid,),
            ).fetchone()
        if not row:
            return None
        gid = str(row[0] or "").strip()
        return gid or None

    def list_run_experiments(
        self,
        *,
        graph_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        gid = str(graph_id or "").strip()
        lim = max(1, int(limit))
        off = max(0, int(offset))
        with self._lock:
            cur = self._conn.cursor()
            if gid:
                rows = cur.execute(
                    """
                    SELECT run_id, graph_id, created_at, status, params_json, metrics_json, environment_json, artifacts_json, artifact_ids_json
                    FROM run_experiments
                    WHERE graph_id=?
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                    """,
                    (gid, lim, off),
                ).fetchall()
            else:
                rows = cur.execute(
                    """
                    SELECT run_id, graph_id, created_at, status, params_json, metrics_json, environment_json, artifacts_json, artifact_ids_json
                    FROM run_experiments
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                    """,
                    (lim, off),
                ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            metrics = json.loads(row[5] or "{}")
            analytics = metrics.get("__analytics") if isinstance(metrics, dict) and isinstance(metrics.get("__analytics"), dict) else {}
            out.append(
                {
                    "runId": str(row[0]),
                    "graphId": str(row[1]),
                    "createdAt": str(row[2]),
                    "status": str(row[3]),
                    "params": json.loads(row[4] or "{}"),
                    "metrics": metrics,
                    "analytics": analytics,
                    "environment": json.loads(row[6] or "{}"),
                    "artifacts": json.loads(row[7] or "[]"),
                    "artifactIds": json.loads(row[8] or "[]"),
                }
            )
        return out

    def record_artifact_usage(self, run_id: str, artifact_id: str) -> None:
        """Persist a (run_id, artifact_id) reference so the retention sweep can
        protect this artifact as long as *run_id* is in the keep window — even
        when *artifact_id* was originally written by an older, pruned run."""
        rid = str(run_id or "").strip()
        aid = str(artifact_id or "").strip()
        if not rid or not aid:
            return
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO run_artifact_refs (run_id, artifact_id) VALUES (?, ?)",
                (rid, aid),
            )
            self._conn.commit()

    def record_consumers(
        self,
        *,
        input_artifact_ids: List[str],
        consumer_run_id: str,
        consumer_node_id: str,
        consumer_exec_key: Optional[str],
        output_artifact_id: str,
    ) -> None:
        ids = sorted(set(input_artifact_ids or []))
        if not ids:
            return
        created_at = datetime.now(timezone.utc).isoformat()
        with self._lock:
            cur = self._conn.cursor()
            for input_id in ids:
                if not input_id:
                    continue
                cur.execute(
                    """
                    INSERT OR IGNORE INTO artifact_consumers (
                        input_artifact_id, consumer_run_id, consumer_node_id,
                        consumer_exec_key, output_artifact_id, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        input_id,
                        consumer_run_id,
                        consumer_node_id,
                        consumer_exec_key,
                        output_artifact_id,
                        created_at,
                    ),
                )
            self._conn.commit()

    def get_consumers(self, artifact_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        lim = max(1, int(limit))
        with self._lock:
            cur = self._conn.cursor()
            rows = cur.execute(
                """
                SELECT input_artifact_id, consumer_run_id, consumer_node_id,
                       consumer_exec_key, output_artifact_id, created_at
                FROM artifact_consumers
                WHERE input_artifact_id=?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (artifact_id, lim),
            ).fetchall()
        return [
            {
                "inputArtifactId": r[0],
                "consumerRunId": r[1],
                "consumerNodeId": r[2],
                "consumerExecKey": r[3],
                "outputArtifactId": r[4],
                "createdAt": r[5],
            }
            for r in rows
        ]

    def put_checkpoint_pins(self, graph_id: str, pins: List[Tuple[str, str]]) -> None:
        """Replace all checkpoint pins for *graph_id* with *pins*.

        Each entry is ``(artifact_id, node_id)``.  The primary key is
        ``(graph_id, node_id)`` — one pin per node per graph — so this
        effectively upserts the current set.
        """
        gid = str(graph_id or "").strip()
        if not gid:
            return
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("DELETE FROM checkpoint_pins WHERE graph_id=?", (gid,))
            for artifact_id, node_id in pins:
                aid = str(artifact_id or "").strip()
                nid = str(node_id or "").strip()
                if not aid or not nid:
                    continue
                cur.execute(
                    """
                    INSERT INTO checkpoint_pins (graph_id, artifact_id, node_id, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (gid, aid, nid, now),
                )
            self._conn.commit()

    def get_checkpoint_pinned_artifact_ids(self, cur: Optional[sqlite3.Cursor] = None) -> Set[str]:
        """Return the set of artifact IDs that are currently checkpoint-pinned (across all graphs).

        If *cur* is provided, the call assumes the caller already holds the lock
        and the table can be queried through the given cursor (no extra locking).
        """
        if cur is not None:
            rows = cur.execute("SELECT DISTINCT artifact_id FROM checkpoint_pins").fetchall()
        else:
            with self._lock:
                cur = self._conn.cursor()
                rows = cur.execute("SELECT DISTINCT artifact_id FROM checkpoint_pins").fetchall()
        return {str(r[0]) for r in rows if r[0]}

    def _delete_run_locked(self, *, cur: sqlite3.Cursor, run_id: str) -> Dict[str, Any]:
        rows = cur.execute(
            "SELECT artifact_id, content_hash FROM artifacts WHERE run_id=?",
            (run_id,),
        ).fetchall()
        artifact_ids = [r[0] for r in rows]
        candidate_hashes = {r[1] for r in rows if r[1]}

        cur.execute("DELETE FROM artifact_consumers WHERE consumer_run_id=?", (run_id,))
        if artifact_ids:
            cur.execute(
                f"DELETE FROM artifact_consumers WHERE output_artifact_id IN ({','.join(['?'] * len(artifact_ids))})",
                tuple(artifact_ids),
            )
        cur.execute("DELETE FROM artifacts WHERE run_id=?", (run_id,))
        artifacts_removed = int(cur.rowcount or 0)
        cur.execute("DELETE FROM runs WHERE run_id=?", (run_id,))
        deleted_run_rows = int(cur.rowcount or 0)
        cur.execute("DELETE FROM run_experiments WHERE run_id=?", (run_id,))
        cur.execute("DELETE FROM run_pause_snapshots WHERE run_id=?", (run_id,))
        cur.execute("DELETE FROM run_artifact_refs WHERE run_id=?", (run_id,))
        return {
            "runDeleted": bool(deleted_run_rows) or bool(artifacts_removed),
            "artifactsRemoved": artifacts_removed,
            "artifactIdsRemoved": artifact_ids,
            "candidateHashes": candidate_hashes,
        }

    def _apply_node_retention_locked(self, *, cur: sqlite3.Cursor, trigger_run_id: str) -> None:
        """Retention by node: keep the N most-recent distinct artifacts per
        (graph_id, node_id).  Checkpoint-pinned artifacts are never deleted.
        Uses a window function to rank artifacts per node, which requires
        SQLite >= 3.25 (2018-09-15)."""
        keep_versions = _retention_keep_node_versions()
        pinned_artifact_ids = self.get_checkpoint_pinned_artifact_ids(cur=cur)

        # Rank artifacts within each (graph_id, node_id) group by created_at DESC.
        # Rows with rn > keep_versions are candidates for deletion.
        rows = cur.execute(
            """
            SELECT artifact_id
            FROM (
                SELECT artifact_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY graph_id, node_id
                           ORDER BY created_at DESC
                       ) AS rn
                FROM artifacts
                WHERE graph_id IS NOT NULL AND TRIM(graph_id) != ''
                  AND node_id  IS NOT NULL AND TRIM(node_id)  != ''
            ) ranked
            WHERE rn > ?
            """,
            (keep_versions,),
        ).fetchall()

        to_delete = [
            str(r[0]) for r in rows
            if r[0] and str(r[0]) not in pinned_artifact_ids
        ]
        pinned_protected = len([r for r in rows if r[0] and str(r[0]) in pinned_artifact_ids])

        if not to_delete:
            logger.debug(
                "artifact_node_retention_sweep store=sqlite trigger_run_id=%s keep_versions=%s "
                "nothing_to_prune pinned_protected=%s",
                trigger_run_id, keep_versions, pinned_protected,
            )
            return

        batch_size = 500
        total_deleted = 0
        for i in range(0, len(to_delete), batch_size):
            batch = to_delete[i : i + batch_size]
            ph = ",".join(["?"] * len(batch))
            cur.execute(
                f"DELETE FROM artifact_consumers WHERE output_artifact_id IN ({ph})",
                tuple(batch),
            )
            cur.execute(
                f"DELETE FROM artifacts WHERE artifact_id IN ({ph})",
                tuple(batch),
            )
            total_deleted += len(batch)

        logger.info(
            "artifact_node_retention_sweep store=sqlite trigger_run_id=%s keep_versions=%s "
            "artifacts_pruned=%s pinned_protected=%s",
            trigger_run_id,
            keep_versions,
            total_deleted,
            pinned_protected,
        )

    def _apply_run_retention_locked(self, *, cur: sqlite3.Cursor, trigger_run_id: str) -> None:
        if _retention_mode() != "by_run":
            return
        keep_recent = _retention_keep_recent_runs()
        active_statuses = {"pending", "running", "cancel_requested", "pausing", "paused", "resuming"}
        rows = cur.execute(
            """
            SELECT run_id, status, COALESCE(completed_at, created_at) AS sort_ts
            FROM runs
            WHERE status <> 'deleted'
            ORDER BY sort_ts DESC
            """
        ).fetchall()
        candidates: List[Tuple[str, str, str]] = []
        for row in rows:
            run_id = str(row[0] or "").strip()
            status = str(row[1] or "").strip().lower()
            sort_ts = str(row[2] or "")
            if not run_id:
                continue
            if status in active_statuses:
                continue
            if not _is_terminal_status(status):
                continue
            if not _should_include_status_for_retention(status):
                continue
            candidates.append((run_id, status, sort_ts))
        keep_ids = {rid for rid, _, _ in candidates[:keep_recent]}
        prune_ids = [rid for rid, _, _ in candidates if rid not in keep_ids]
        if not prune_ids:
            return
        # Build the set of artifact IDs that must survive pruning:
        # (a) checkpoint-pinned artifacts, and
        # (b) artifacts referenced (via cache hit) by any run we are keeping —
        #     even if those artifacts were *written* by a run we are pruning.
        # NOTE: by_run retention has known limitations with partial "run-from-selected"
        # runs.  Use ARTIFACT_RETENTION_MODE=by_node to avoid false evictions.
        pinned_artifact_ids = self.get_checkpoint_pinned_artifact_ids(cur=cur)
        if keep_ids:
            placeholders = ",".join(["?"] * len(keep_ids))
            ref_rows = cur.execute(
                f"SELECT DISTINCT artifact_id FROM run_artifact_refs WHERE run_id IN ({placeholders})",
                tuple(keep_ids),
            ).fetchall()
            for row in ref_rows:
                if row[0]:
                    pinned_artifact_ids.add(str(row[0]))
        logger.info(
            "artifact_retention_sweep_started mode=by_run keep_recent_runs=%s trigger_run_id=%s terminal_candidates=%s prune_candidates=%s pinned_artifacts=%s",
            keep_recent,
            trigger_run_id,
            len(candidates),
            len(prune_ids),
            len(pinned_artifact_ids),
        )
        pinned_protected = 0
        for rid in prune_ids:
            # Check if any artifacts in this run are checkpoint-pinned.
            art_rows = cur.execute(
                "SELECT artifact_id FROM artifacts WHERE run_id=?",
                (rid,),
            ).fetchall()
            run_art_ids = {str(r[0]) for r in art_rows if r[0]}
            pinned_in_run = run_art_ids & pinned_artifact_ids
            if pinned_in_run:
                # Only delete non-pinned artifacts; keep the run record alive.
                to_delete = run_art_ids - pinned_artifact_ids
                if to_delete:
                    placeholders = ",".join(["?"] * len(to_delete))
                    cur.execute(
                        f"DELETE FROM artifact_consumers WHERE output_artifact_id IN ({placeholders})",
                        tuple(to_delete),
                    )
                    cur.execute(
                        f"DELETE FROM artifacts WHERE artifact_id IN ({placeholders})",
                        tuple(to_delete),
                    )
                pinned_protected += len(pinned_in_run)
                logger.info(
                    "artifact_retention_run_partial_prune run_id=%s artifacts_removed=%s pinned_protected=%s",
                    rid,
                    len(to_delete),
                    len(pinned_in_run),
                )
            else:
                deleted = self._delete_run_locked(cur=cur, run_id=rid)
                logger.info(
                    "artifact_retention_run_pruned run_id=%s artifacts_removed=%s",
                    rid,
                    int(deleted.get("artifactsRemoved") or 0),
                )
        logger.info(
            "artifact_retention_sweep_finished mode=by_run keep_recent_runs=%s trigger_run_id=%s pruned_runs=%s pinned_protected=%s",
            keep_recent,
            trigger_run_id,
            len(prune_ids),
            pinned_protected,
        )

    def delete_run(self, run_id: str, mode: str = "soft", gc: str = "none") -> Dict[str, Any]:
        mode = (mode or "soft").lower()
        gc = (gc or "none").lower()
        if mode not in ("soft", "hard"):
            raise ValueError(f"Unsupported delete mode: {mode}")
        if gc not in ("none", "unreferenced"):
            raise ValueError(f"Unsupported gc mode: {gc}")

        if mode == "soft":
            with self._lock:
                cur = self._conn.cursor()
                now = datetime.now(timezone.utc).isoformat()
                cur.execute(
                    "UPDATE runs SET status='deleted', deleted_at=?, updated_at=? WHERE run_id=?",
                    (now, now, run_id),
                )
                cur.execute(
                    "UPDATE run_experiments SET status='deleted' WHERE run_id=?",
                    (run_id,),
                )
                cur.execute("DELETE FROM run_pause_snapshots WHERE run_id=?", (run_id,))
                changed = cur.rowcount
                self._conn.commit()
            return {
                "runDeleted": bool(changed),
                "mode": "soft",
                "artifactsRemoved": 0,
                "cacheRowsRemoved": 0,
                "blobsDeleted": 0,
                "artifactIdsRemoved": [],
            }

        with self._lock:
            cur = self._conn.cursor()
            deleted = self._delete_run_locked(cur=cur, run_id=run_id)
            artifact_ids = list(deleted.get("artifactIdsRemoved") or [])
            candidate_hashes = set(deleted.get("candidateHashes") or set())
            artifacts_removed = int(deleted.get("artifactsRemoved") or 0)
            run_deleted = bool(deleted.get("runDeleted"))
            self._conn.commit()

        blobs_deleted = 0
        if gc == "unreferenced" and candidate_hashes:
            for content_hash in candidate_hashes:
                with self._lock:
                    cur = self._conn.cursor()
                    still_used = cur.execute(
                        "SELECT 1 FROM artifacts WHERE content_hash=? LIMIT 1",
                        (content_hash,),
                    ).fetchone()
                if still_used:
                    continue
                path = Path(self._blob_path(content_hash))
                try:
                    if path.exists():
                        path.unlink()
                        blobs_deleted += 1
                except Exception:
                    # Non-fatal; future sweep can clean up.
                    pass

        return {
            "runDeleted": run_deleted,
            "mode": "hard",
            "artifactsRemoved": int(artifacts_removed),
            "cacheRowsRemoved": 0,
            "blobsDeleted": int(blobs_deleted),
            "artifactIdsRemoved": artifact_ids,
        }

    def upsert_snapshot_metadata(self, snapshot_id: str, metadata: Dict[str, Any]) -> None:
        sid = str(snapshot_id or "").strip().lower()
        if not sid:
            raise ValueError("snapshot_id is required")
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT INTO snapshots (snapshot_id, metadata_json, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(snapshot_id) DO UPDATE SET metadata_json=excluded.metadata_json
                """,
                (
                    sid,
                    json.dumps(metadata or {}, ensure_ascii=False),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            self._conn.commit()

    def get_snapshot_metadata(self, snapshot_id: str) -> Optional[Dict[str, Any]]:
        sid = str(snapshot_id or "").strip().lower()
        if not sid:
            return None
        with self._lock:
            cur = self._conn.cursor()
            row = cur.execute(
                "SELECT metadata_json FROM snapshots WHERE snapshot_id=?",
                (sid,),
            ).fetchone()
        if not row:
            return None
        try:
            meta = json.loads(row[0]) if row[0] else {}
            return meta if isinstance(meta, dict) else None
        except Exception:
            return None

    def latest_node_artifact_id(
        self,
        *,
        graph_id: str,
        node_id: str,
        exclude_artifact_id: Optional[str] = None,
    ) -> Optional[str]:
        gid = str(graph_id or "").strip()
        nid = str(node_id or "").strip()
        if not gid or not nid:
            return None
        ex = str(exclude_artifact_id or "").strip()
        with self._lock:
            cur = self._conn.cursor()
            if ex:
                row = cur.execute(
                    """
                    SELECT artifact_id
                    FROM artifacts
                    WHERE graph_id=? AND node_id=? AND artifact_id<>?
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (gid, nid, ex),
                ).fetchone()
            else:
                row = cur.execute(
                    """
                    SELECT artifact_id
                    FROM artifacts
                    WHERE graph_id=? AND node_id=?
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (gid, nid),
                ).fetchone()
        return str(row[0]) if row and row[0] else None

    def gc_orphan_blobs(
        self, mode: str = "dry_run", limit: Optional[int] = None, max_seconds: Optional[int] = None
    ) -> Dict[str, Any]:
        import time

        mode = (mode or "dry_run").lower()
        if mode not in ("dry_run", "delete"):
            raise ValueError("mode must be 'dry_run' or 'delete'")

        with self._lock:
            cur = self._conn.cursor()
            refs = cur.execute(
                "SELECT DISTINCT content_hash FROM artifacts WHERE content_hash IS NOT NULL"
            ).fetchall()
        referenced = {str(r[0]).lower() for r in refs if r and r[0]}

        start = time.monotonic()
        orphan_hashes: List[str] = []
        scanned = 0
        for p in self._blob_root.rglob("*.bin"):
            scanned += 1
            name = p.stem.lower()
            if len(name) == 64 and all(c in "0123456789abcdef" for c in name):
                if name not in referenced:
                    orphan_hashes.append(name)
                    if limit is not None and len(orphan_hashes) >= limit:
                        break
            if max_seconds is not None and (time.monotonic() - start) >= max_seconds:
                break

        deleted = 0
        if mode == "delete":
            for h in orphan_hashes:
                path = self._blob_path(h)
                try:
                    if path.exists():
                        path.unlink()
                        deleted += 1
                except Exception:
                    pass

        return {
            "mode": mode,
            "referenced_hashes": len(referenced),
            "orphan_hashes": orphan_hashes,
            "blobs_deleted": deleted,
            "scanned_blobs": scanned,
        }


class DiskArtifactStore:
    """
    Disk-backed immutable artifact store with blob dedupe (content_hash) + SQLite metadata index.
    """

    def __init__(self, root_dir: str | Path) -> None:
        self._root = Path(root_dir).resolve()
        self._blob_root = self._root / "blobs" / "sha256"
        self._blob_root.mkdir(parents=True, exist_ok=True)
        self._index = _SqliteArtifactIndex(
            self._root / "meta" / "artifacts.sqlite",
            blob_root=self._blob_root,
        )
        self._meta_cache: Dict[str, Artifact] = {}
        self._blob_cache: Dict[str, bytes] = {}
        self._memo_stats: Dict[str, int] = {
            "meta_hit": 0,
            "meta_miss": 0,
            "blob_hit": 0,
            "blob_miss": 0,
        }

    def _blob_path(self, content_hash: str) -> Path:
        ch = content_hash.lower()
        return self._blob_root / ch[:2] / ch[2:4] / f"{ch}.bin"

    def _blob_uri(self, content_hash: str) -> str:
        return str(self._blob_path(content_hash))

    def _write_blob_atomic(self, content_hash: str, data: bytes) -> str:
        final_path = self._blob_path(content_hash)
        final_path.parent.mkdir(parents=True, exist_ok=True)
        if final_path.exists():
            return str(final_path)

        tmp_name = f"{final_path.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}"
        tmp_path = final_path.parent / tmp_name
        with open(tmp_path, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        if final_path.exists():
            try:
                os.remove(tmp_path)
            except FileNotFoundError:
                pass
        else:
            os.replace(tmp_path, final_path)
        return str(final_path)

    async def exists(self, artifact_id: str) -> bool:
        return self._index.exists(artifact_id)

    async def get(self, artifact_id: str) -> Artifact:
        cached = self._meta_cache.get(artifact_id)
        if cached is not None:
            self._memo_stats["meta_hit"] += 1
            return cached
        self._memo_stats["meta_miss"] += 1
        art = self._index.get(artifact_id)
        self._meta_cache[artifact_id] = art
        return art

    async def read(self, artifact_id: str) -> bytes:
        cached = self._blob_cache.get(artifact_id)
        if cached is not None:
            self._memo_stats["blob_hit"] += 1
            return cached
        self._memo_stats["blob_miss"] += 1
        art = await self.get(artifact_id)
        if not art.content_hash:
            raise KeyError(f"Artifact missing content hash: {artifact_id}")
        path = self._blob_path(art.content_hash)
        if not path.exists():
            raise KeyError(f"Artifact bytes not found: {artifact_id}")
        data = path.read_bytes()
        self._blob_cache[artifact_id] = data
        return data

    async def open_payload(self, artifact_id: str) -> AsyncIterator[bytes]:
        art = self._index.get(artifact_id)
        if not art.content_hash:
            raise KeyError(f"Artifact missing content hash: {artifact_id}")
        path = self._blob_path(art.content_hash)
        if not path.exists():
            raise KeyError(f"Artifact bytes not found: {artifact_id}")
        chunk_size = 64 * 1024
        with open(path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    async def write(self, artifact: Artifact, data: bytes) -> str:
        _validate_artifact_metadata_v1(artifact)
        if artifact.run_id and (
            not artifact.graph_id or not artifact.node_id or not artifact.exec_key
        ):
            raise ValueError("Runtime artifact writes require graph_id, node_id, and exec_key")
        if artifact.run_id and not str(artifact.params_hash or "").strip():
            raise ValueError("Runtime artifact writes require non-empty params_hash (node_state_hash)")
        if artifact.exec_key and artifact.artifact_id != artifact.exec_key:
            raise ValueError("artifact_id must equal exec_key when exec_key is present")
        if self._index.exists(artifact.artifact_id):
            logger.debug(
                "artifact_write_skip_existing store=disk artifact_id=%s run_id=%s node_id=%s exec_key=%s",
                artifact.artifact_id,
                artifact.run_id,
                artifact.node_id,
                artifact.exec_key,
            )
            return artifact.artifact_id

        content_hash = hashlib.sha256(data).hexdigest()
        storage_uri = self._write_blob_atomic(content_hash, data)
        logger.debug(
            "artifact_write store=disk artifact_id=%s run_id=%s node_id=%s exec_key=%s size_bytes=%s content_hash=%s storage_uri=%s",
            artifact.artifact_id,
            artifact.run_id,
            artifact.node_id,
            artifact.exec_key,
            len(data),
            content_hash,
            storage_uri,
        )
        artifact_to_store = artifact.model_copy(
            update={
                "content_hash": content_hash,
                "storage_uri": storage_uri,
                "size_bytes": len(data),
            }
        )
        # Atomic commit order:
        # 1) payload blob
        # 2) metadata row
        # 3) validate committed artifact
        self._index.put(artifact_to_store)
        committed = self._index.get(artifact.artifact_id)
        self._meta_cache[artifact.artifact_id] = committed
        self._blob_cache[artifact.artifact_id] = data
        if str(committed.content_hash or "") != content_hash:
            raise RuntimeError(
                f"Artifact commit validation failed (content_hash mismatch): {artifact.artifact_id}"
            )
        if int(committed.size_bytes or -1) != len(data):
            raise RuntimeError(
                f"Artifact commit validation failed (size mismatch): {artifact.artifact_id}"
            )
        blob_path = self._blob_path(content_hash)
        if not blob_path.exists():
            raise RuntimeError(
                f"Artifact commit validation failed (payload missing): {artifact.artifact_id}"
            )
        return artifact.artifact_id

    async def record_run(self, run_id: str, status: str) -> None:
        self._index.record_run(run_id, status)

    async def update_run_status(self, run_id: str, status: str) -> None:
        self._index.update_run_status(run_id, status)

    async def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        return self._index.get_run(run_id)

    async def list_runs(self, include_deleted: bool = False) -> List[Dict[str, Any]]:
        return self._index.list_runs(include_deleted=include_deleted)

    async def upsert_run_pause_snapshot(self, run_id: str, snapshot: Dict[str, Any]) -> None:
        self._index.upsert_run_pause_snapshot(run_id, snapshot)

    async def get_run_pause_snapshot(self, run_id: str) -> Optional[Dict[str, Any]]:
        return self._index.get_run_pause_snapshot(run_id)

    async def delete_run_pause_snapshot(self, run_id: str) -> None:
        self._index.delete_run_pause_snapshot(run_id)

    async def upsert_run_experiment(self, summary: Dict[str, Any]) -> None:
        self._index.upsert_run_experiment(summary)

    async def get_run_experiment(self, run_id: str) -> Optional[Dict[str, Any]]:
        return self._index.get_run_experiment(run_id)

    async def get_run_graph_id(self, run_id: str) -> Optional[str]:
        return self._index.get_run_graph_id(run_id)

    async def list_run_experiments(
        self,
        *,
        graph_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        return self._index.list_run_experiments(graph_id=graph_id, limit=limit, offset=offset)

    async def put_checkpoint_pins(self, graph_id: str, pins: List[Tuple[str, str]]) -> None:
        self._index.put_checkpoint_pins(graph_id, pins)

    async def get_checkpoint_pinned_artifact_ids(self) -> Set[str]:
        return self._index.get_checkpoint_pinned_artifact_ids()

    async def record_artifact_usage(self, run_id: str, artifact_id: str) -> None:
        self._index.record_artifact_usage(run_id, artifact_id)

    async def delete_run(self, run_id: str, mode: str = "soft", gc: str = "none") -> Dict[str, Any]:
        out = self._index.delete_run(run_id, mode=mode, gc=gc)
        for aid in list(out.get("artifactIdsRemoved") or []):
            self._meta_cache.pop(str(aid), None)
            self._blob_cache.pop(str(aid), None)
        return out

    async def record_consumers(
        self,
        *,
        input_artifact_ids: List[str],
        consumer_run_id: str,
        consumer_node_id: str,
        consumer_exec_key: Optional[str],
        output_artifact_id: str,
    ) -> None:
        self._index.record_consumers(
            input_artifact_ids=input_artifact_ids,
            consumer_run_id=consumer_run_id,
            consumer_node_id=consumer_node_id,
            consumer_exec_key=consumer_exec_key,
            output_artifact_id=output_artifact_id,
        )

    async def get_consumers(self, artifact_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        return self._index.get_consumers(artifact_id, limit=limit)

    async def gc_orphan_blobs(
        self, mode: str = "dry_run", limit: Optional[int] = None, max_seconds: Optional[int] = None
    ) -> Dict[str, Any]:
        return self._index.gc_orphan_blobs(mode=mode, limit=limit, max_seconds=max_seconds)

    async def delete_node_artifacts(self, *, graph_id: str, node_id: str) -> Dict[str, Any]:
        out = self._index.delete_node_artifacts(graph_id=graph_id, node_id=node_id)
        for aid in list(out.get("artifactIdsRemoved") or []):
            self._meta_cache.pop(str(aid), None)
            self._blob_cache.pop(str(aid), None)
        return out

    async def write_snapshot_from_file(
        self,
        *,
        snapshot_id: str,
        file_path: str | Path,
        metadata: Dict[str, Any],
        mime_type: Optional[str] = None,
    ) -> str:
        sid = str(snapshot_id or "").strip().lower()
        if not sid:
            raise ValueError("snapshot_id is required")
        src = Path(file_path)
        if not src.exists():
            raise FileNotFoundError(f"snapshot source file not found: {src}")
        blob_path = self._blob_path(sid)
        blob_path.parent.mkdir(parents=True, exist_ok=True)
        if not blob_path.exists():
            os.replace(str(src), str(blob_path))
        else:
            try:
                src.unlink(missing_ok=True)
            except Exception:
                pass
        size_bytes = blob_path.stat().st_size
        art = Artifact(
            artifact_id=sid,
            node_kind="snapshot",
            params_hash="snapshot",
            upstream_ids=[],
            created_at=datetime.now(timezone.utc),
            execution_version="snapshot_v1",
            mime_type=mime_type or str(metadata.get("mimeType") or "application/octet-stream"),
            payload_type="binary",
            size_bytes=int(size_bytes),
            storage_uri=str(blob_path),
            payload_schema={"schema_version": 1, "type": "binary", "snapshot": True},
            content_hash=sid,
            graph_id="__snapshots__",
            node_id=None,
            run_id=None,
            exec_key=None,
        )
        self._index.put(art)
        self._index.upsert_snapshot_metadata(sid, dict(metadata or {}))
        return sid

    async def get_snapshot_metadata(self, snapshot_id: str) -> Optional[Dict[str, Any]]:
        return self._index.get_snapshot_metadata(snapshot_id)

    async def get_latest_node_artifact(
        self,
        *,
        graph_id: str,
        node_id: str,
        exclude_artifact_id: Optional[str] = None,
    ) -> Optional[str]:
        return self._index.latest_node_artifact_id(
            graph_id=graph_id,
            node_id=node_id,
            exclude_artifact_id=exclude_artifact_id,
        )

    def get_memo_stats(self) -> Dict[str, int]:
        return {
            **self._memo_stats,
            "meta_entries": int(len(self._meta_cache)),
            "blob_entries": int(len(self._blob_cache)),
        }


# ----------------------------
# Run bindings (run_id + node_id -> artifact_id)
# ----------------------------

class RunBindings:
    """
    Minimal binding map for a single run.
    If you want cross-run bindings later, move to a repo/db.
    """
    def __init__(self, run_id: str, graph_id: str = "") -> None:
        self.run_id = run_id
        self.graph_id = str(graph_id or "")
        self._bindings: Dict[str, RunArtifactBinding] = {}

    def _normalize_handle(self, handle: Optional[str]) -> str:
        h = str(handle or "out").strip()
        return h or "out"

    def _key(self, node_id: str, handle: Optional[str] = "out") -> str:
        return f"{self.graph_id}:{node_id}:{self._normalize_handle(handle)}"

    def bind(
        self,
        node_id: str,
        artifact_id: str,
        status: str = "computed",
        handle: Optional[str] = "out",
    ) -> RunArtifactBinding:
        normalized_handle = self._normalize_handle(handle)
        logger.debug(
            "run_binding_bind run_id=%s node_id=%s handle=%s artifact_id=%s status=%s",
            self.run_id,
            node_id,
            normalized_handle,
            artifact_id,
            status,
        )
        b = RunArtifactBinding(
            run_id=self.run_id,
            graph_id=self.graph_id,
            node_id=node_id,
            handle=normalized_handle,
            artifact_id=artifact_id,
            status=status,
            bound_at=datetime.now(timezone.utc),
        )
        self._bindings[self._key(node_id, normalized_handle)] = b
        return b

    def get(self, node_id: str, handle: Optional[str] = "out") -> Optional[RunArtifactBinding]:
        normalized_handle = self._normalize_handle(handle)
        binding = self._bindings.get(self._key(node_id, normalized_handle))
        if binding is not None:
            return binding
        # Backward-compat fallback for legacy callers that ask for "out" when only
        # one handle was bound under a non-default key.
        if normalized_handle == "out":
            prefix = f"{self.graph_id}:{node_id}:"
            matches = [b for k, b in self._bindings.items() if k.startswith(prefix)]
            if len(matches) == 1:
                return matches[0]
        return None

    def artifact_id_for(self, node_id: str, handle: Optional[str] = "out") -> Optional[str]:
        b = self.get(node_id, handle=handle)
        return b.artifact_id if b else None

    def get_current_artifact(self, node_id: str, handle: Optional[str] = "out") -> Optional[str]:
        return self.artifact_id_for(node_id, handle=handle)

    def all(self) -> List[RunArtifactBinding]:
        return list(self._bindings.values())

