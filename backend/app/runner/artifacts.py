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
from typing import Any, AsyncIterator, Dict, List, Optional, Protocol, Tuple

from pydantic import BaseModel

logger = logging.getLogger(__name__)


_PORT_TYPES = {"table", "json", "text", "binary", "embeddings"}
_ARTIFACT_METADATA_VERSION = 1
_REQUIRED_ARTIFACT_META_KEYS = {
    "metadataVersion",
    "execKey",
    "nodeId",
    "nodeType",
    "nodeImplVersion",
    "paramsFingerprint",
    "upstreamArtifactIds",
    "contractFingerprint",
    "mimeType",
    "portType",
    "createdAt",
}


def _normalize_payload_schema(payload_schema: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(payload_schema, dict):
        return payload_schema
    out = dict(payload_schema)
    t = str(out.get("type") or "").lower()
    if t == "string":
        out["type"] = "text"
        out.setdefault("encoding", "utf-8")
    return out


def _infer_port_type(
    *,
    payload_schema: Optional[Dict[str, Any]],
    mime_type: Optional[str],
    node_kind: Optional[str] = None,
) -> str:
    ps = _normalize_payload_schema(payload_schema)
    if isinstance(ps, dict):
        t = str(ps.get("type") or "").lower()
        if t in _PORT_TYPES:
            return t
    mt = str(mime_type or "").lower()
    if "json" in mt:
        return "json"
    if "markdown" in mt or mt.startswith("text/"):
        return "text"
    if "csv" in mt or "tsv" in mt or "parquet" in mt:
        return "table"
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
    port_type: Optional[str] = None
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
        self._consumers: Dict[str, List[Dict[str, Any]]] = {}
        self._snapshots: Dict[str, Dict[str, Any]] = {}

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
        delete_set = set(to_delete)
        for aid in to_delete:
            self._meta.pop(aid, None)
            self._blob.pop(aid, None)
        for input_id, consumers in list(self._consumers.items()):
            self._consumers[input_id] = [c for c in consumers if c.get("outputArtifactId") not in delete_set]
            if not self._consumers[input_id]:
                self._consumers.pop(input_id, None)
        return to_delete

    async def exists(self, artifact_id: str) -> bool:
        return artifact_id in self._meta

    async def get(self, artifact_id: str) -> Artifact:
        if artifact_id not in self._meta:
            raise KeyError(f"Artifact not found: {artifact_id}")
        return self._meta[artifact_id]

    async def read(self, artifact_id: str) -> bytes:
        if artifact_id not in self._blob:
            raise KeyError(f"Artifact bytes not found: {artifact_id}")
        return self._blob[artifact_id]

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
        self._prune_node_artifacts(
            graph_id=str(artifact.graph_id or ""),
            node_id=str(artifact.node_id or ""),
            keep_last=5,
        )
        return artifact.artifact_id

    async def record_run(self, run_id: str, status: str) -> None:
        self._runs[run_id] = {
            "run_id": run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "deleted_at": None,
        }

    async def update_run_status(self, run_id: str, status: str) -> None:
        rec = self._runs.get(run_id)
        if not rec:
            await self.record_run(run_id, status)
            return
        rec["status"] = status

    async def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        return self._runs.get(run_id)

    async def list_runs(self, include_deleted: bool = False) -> List[Dict[str, Any]]:
        runs = list(self._runs.values())
        if not include_deleted:
            runs = [r for r in runs if r.get("status") != "deleted"]
        runs.sort(key=lambda r: r.get("created_at", ""), reverse=True)
        return runs

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
            return {"runDeleted": True, "mode": "soft", "artifactsRemoved": 0, "cacheRowsRemoved": 0, "blobsDeleted": 0, "artifactIdsRemoved": []}

        artifact_ids = []
        for aid, art in list(self._meta.items()):
            if art.run_id == run_id:
                artifact_ids.append(aid)
                self._meta.pop(aid, None)
                self._blob.pop(aid, None)
        for input_id, rows in list(self._consumers.items()):
            self._consumers[input_id] = [
                r
                for r in rows
                if r.get("consumerRunId") != run_id and r.get("outputArtifactId") not in artifact_ids
            ]
            if not self._consumers[input_id]:
                self._consumers.pop(input_id, None)
        run_deleted = (self._runs.pop(run_id, None) is not None) or bool(artifact_ids)
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
            port_type="binary",
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
                    port_type TEXT,
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
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_run_id ON artifacts(run_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_graph_id ON artifacts(graph_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_node_id ON artifacts(node_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_graph_node ON artifacts(graph_id, node_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_graph_node_created ON artifacts(graph_id, node_id, created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_exec_key ON artifacts(exec_key)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_content_hash ON artifacts(content_hash)")
            # Migration: older DBs won't have port_type.
            cols = [r[1] for r in cur.execute("PRAGMA table_info(artifacts)").fetchall()]
            if "port_type" not in cols:
                cur.execute("ALTER TABLE artifacts ADD COLUMN port_type TEXT")
            if "graph_id" not in cols:
                cur.execute("ALTER TABLE artifacts ADD COLUMN graph_id TEXT")
            if "node_id" not in cols:
                cur.execute("ALTER TABLE artifacts ADD COLUMN node_id TEXT")
            if "exec_key" not in cols:
                cur.execute("ALTER TABLE artifacts ADD COLUMN exec_key TEXT")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_port_type ON artifacts(port_type)")
            # Backfill legacy rows with null port_type.
            null_rows = cur.execute(
                """
                SELECT artifact_id, mime_type, payload_schema_json, node_kind
                FROM artifacts
                WHERE port_type IS NULL OR TRIM(port_type) = ''
                """
            ).fetchall()
            for aid, mime_type, payload_schema_json, node_kind in null_rows:
                payload_schema = None
                if payload_schema_json:
                    try:
                        payload_schema = json.loads(payload_schema_json)
                    except Exception:
                        payload_schema = None
                inferred = _infer_port_type(
                    payload_schema=payload_schema,
                    mime_type=mime_type,
                    node_kind=node_kind,
                )
                cur.execute(
                    "UPDATE artifacts SET port_type=? WHERE artifact_id=?",
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
                    status TEXT NOT NULL,
                    deleted_at TEXT
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at)")
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
                       created_at, execution_version, mime_type, port_type, size_bytes, storage_uri,
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
        inferred_port = _infer_port_type(payload_schema=payload_schema, mime_type=row[7], node_kind=row[2])

        return Artifact(
            artifact_id=row[0],
            content_hash=row[1],
            node_kind=row[2],
            params_hash=row[3],
            upstream_ids=json.loads(row[4]),
            created_at=created_at,
            execution_version=row[6],
            mime_type=row[7] or "application/octet-stream",
            port_type=row[8] or inferred_port,
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
                    created_at, execution_version, mime_type, port_type, size_bytes, storage_uri,
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
                    artifact.port_type,
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
            if artifact.graph_id and artifact.node_id:
                self._prune_node_artifacts_locked(
                    cur=cur,
                    graph_id=str(artifact.graph_id),
                    node_id=str(artifact.node_id),
                    keep_last=5,
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
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT INTO runs (run_id, created_at, status, deleted_at)
                VALUES (?, ?, ?, NULL)
                ON CONFLICT(run_id) DO UPDATE SET status=excluded.status
                """,
                (run_id, datetime.now(timezone.utc).isoformat(), status),
            )
            self._conn.commit()

    def update_run_status(self, run_id: str, status: str) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT INTO runs (run_id, created_at, status, deleted_at)
                VALUES (?, ?, ?, NULL)
                ON CONFLICT(run_id) DO UPDATE SET status=excluded.status
                """,
                (run_id, datetime.now(timezone.utc).isoformat(), status),
            )
            self._conn.commit()

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.cursor()
            row = cur.execute(
                "SELECT run_id, created_at, status, deleted_at FROM runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
        if not row:
            return None
        return {
            "run_id": row[0],
            "created_at": row[1],
            "status": row[2],
            "deleted_at": row[3],
        }

    def list_runs(self, include_deleted: bool = False) -> List[Dict[str, Any]]:
        where = "" if include_deleted else "WHERE status <> 'deleted'"
        with self._lock:
            cur = self._conn.cursor()
            rows = cur.execute(
                f"""
                SELECT run_id, created_at, status, deleted_at
                FROM runs
                {where}
                ORDER BY created_at DESC
                """
            ).fetchall()
        return [
            {"run_id": r[0], "created_at": r[1], "status": r[2], "deleted_at": r[3]}
            for r in rows
        ]

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
                cur.execute(
                    "UPDATE runs SET status='deleted', deleted_at=? WHERE run_id=?",
                    (datetime.now(timezone.utc).isoformat(), run_id),
                )
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
            artifacts_removed = cur.rowcount
            cur.execute("DELETE FROM runs WHERE run_id=?", (run_id,))
            run_deleted = (cur.rowcount > 0) or bool(artifacts_removed)
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
        return self._index.get(artifact_id)

    async def read(self, artifact_id: str) -> bytes:
        art = self._index.get(artifact_id)
        if not art.content_hash:
            raise KeyError(f"Artifact missing content hash: {artifact_id}")
        path = self._blob_path(art.content_hash)
        if not path.exists():
            raise KeyError(f"Artifact bytes not found: {artifact_id}")
        return path.read_bytes()

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

    async def delete_run(self, run_id: str, mode: str = "soft", gc: str = "none") -> Dict[str, Any]:
        return self._index.delete_run(run_id, mode=mode, gc=gc)

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
        return self._index.delete_node_artifacts(graph_id=graph_id, node_id=node_id)

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
            port_type="binary",
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

    def _key(self, node_id: str) -> str:
        return f"{self.graph_id}:{node_id}"

    def bind(self, node_id: str, artifact_id: str, status: str = "computed") -> RunArtifactBinding:
        logger.debug(
            "run_binding_bind run_id=%s node_id=%s artifact_id=%s status=%s",
            self.run_id,
            node_id,
            artifact_id,
            status,
        )
        b = RunArtifactBinding(
            run_id=self.run_id,
            graph_id=self.graph_id,
            node_id=node_id,
            artifact_id=artifact_id,
            status=status,
            bound_at=datetime.now(timezone.utc),
        )
        self._bindings[self._key(node_id)] = b
        return b

    def get(self, node_id: str) -> Optional[RunArtifactBinding]:
        return self._bindings.get(self._key(node_id))

    def artifact_id_for(self, node_id: str) -> Optional[str]:
        b = self._bindings.get(self._key(node_id))
        return b.artifact_id if b else None

    def get_current_artifact(self, node_id: str) -> Optional[str]:
        return self.artifact_id_for(node_id)

    def all(self) -> List[RunArtifactBinding]:
        return list(self._bindings.values())
