from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Protocol, Tuple

from pydantic import BaseModel


# ----------------------------
# Models
# ----------------------------

class Artifact(BaseModel):
    artifact_id: str  # content-addressed hash
    node_kind: str
    params_hash: str
    upstream_ids: List[str]
    created_at: datetime
    execution_version: str

    mime_type: str
    size_bytes: int

    storage_uri: str  # memory://<id>, file://..., s3://...

    payload_schema: Optional[Dict[str, Any]] = None
    content_hash: Optional[str] = None
    run_id: Optional[str] = None
    node_id: Optional[str] = None
    exec_key: Optional[str] = None


class RunArtifactBinding(BaseModel):
    run_id: str
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
    async def write(self, artifact: Artifact, data: bytes) -> None: ...
    async def record_run(self, run_id: str, status: str) -> None: ...
    async def update_run_status(self, run_id: str, status: str) -> None: ...
    async def get_run(self, run_id: str) -> Optional[Dict[str, Any]]: ...
    async def list_runs(self, include_deleted: bool = False) -> List[Dict[str, Any]]: ...
    async def delete_run(self, run_id: str, mode: str = "soft", gc: str = "none") -> Dict[str, Any]: ...
    async def gc_orphan_blobs(
        self, mode: str = "dry_run", limit: Optional[int] = None, max_seconds: Optional[int] = None
    ) -> Dict[str, Any]: ...


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

    async def write(self, artifact: Artifact, data: bytes) -> None:
        # Enforce immutability: don't overwrite
        if artifact.artifact_id in self._meta:
            return
        content_hash = hashlib.sha256(data).hexdigest()
        self._meta[artifact.artifact_id] = artifact.model_copy(
            update={"content_hash": content_hash, "size_bytes": len(data)}
        )
        self._blob[artifact.artifact_id] = data

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
        run_deleted = (self._runs.pop(run_id, None) is not None) or bool(artifact_ids)
        return {
            "runDeleted": run_deleted,
            "mode": "hard",
            "artifactsRemoved": len(artifact_ids),
            "cacheRowsRemoved": 0,
            "blobsDeleted": len(artifact_ids),
            "artifactIdsRemoved": artifact_ids,
        }

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


class _SqliteArtifactIndex:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

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
                    size_bytes INTEGER NOT NULL,
                    storage_uri TEXT NOT NULL,
                    payload_schema_json TEXT,
                    run_id TEXT,
                    node_id TEXT,
                    exec_key TEXT
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_run_id ON artifacts(run_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_node_id ON artifacts(node_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_exec_key ON artifacts(exec_key)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_content_hash ON artifacts(content_hash)")
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
                       created_at, execution_version, mime_type, size_bytes, storage_uri,
                       payload_schema_json, run_id, node_id, exec_key
                FROM artifacts
                WHERE artifact_id=?
                """,
                (artifact_id,),
            ).fetchone()
        if not row:
            raise KeyError(f"Artifact not found: {artifact_id}")

        payload_schema = json.loads(row[10]) if row[10] else None
        created_at = datetime.fromisoformat(row[5])
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)

        return Artifact(
            artifact_id=row[0],
            content_hash=row[1],
            node_kind=row[2],
            params_hash=row[3],
            upstream_ids=json.loads(row[4]),
            created_at=created_at,
            execution_version=row[6],
            mime_type=row[7] or "application/octet-stream",
            size_bytes=int(row[8]),
            storage_uri=row[9],
            payload_schema=payload_schema,
            run_id=row[11],
            node_id=row[12],
            exec_key=row[13],
        )

    def put(self, artifact: Artifact) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT OR IGNORE INTO artifacts (
                    artifact_id, content_hash, node_kind, params_hash, upstream_ids_json,
                    created_at, execution_version, mime_type, size_bytes, storage_uri,
                    payload_schema_json, run_id, node_id, exec_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    int(artifact.size_bytes),
                    artifact.storage_uri,
                    json.dumps(artifact.payload_schema, ensure_ascii=False)
                    if artifact.payload_schema is not None
                    else None,
                    artifact.run_id,
                    artifact.node_id,
                    artifact.exec_key,
                ),
            )
            self._conn.commit()

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
        self._index = _SqliteArtifactIndex(self._root / "meta" / "artifacts.sqlite")

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

    async def write(self, artifact: Artifact, data: bytes) -> None:
        if self._index.exists(artifact.artifact_id):
            return

        content_hash = hashlib.sha256(data).hexdigest()
        storage_uri = self._write_blob_atomic(content_hash, data)
        artifact_to_store = artifact.model_copy(
            update={
                "content_hash": content_hash,
                "storage_uri": storage_uri,
                "size_bytes": len(data),
            }
        )
        self._index.put(artifact_to_store)

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

    async def gc_orphan_blobs(
        self, mode: str = "dry_run", limit: Optional[int] = None, max_seconds: Optional[int] = None
    ) -> Dict[str, Any]:
        return self._index.gc_orphan_blobs(mode=mode, limit=limit, max_seconds=max_seconds)


# ----------------------------
# Run bindings (run_id + node_id -> artifact_id)
# ----------------------------

class RunBindings:
    """
    Minimal binding map for a single run.
    If you want cross-run bindings later, move to a repo/db.
    """
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self._bindings: Dict[str, RunArtifactBinding] = {}

    def bind(self, node_id: str, artifact_id: str, status: str = "computed") -> RunArtifactBinding:
        b = RunArtifactBinding(
            run_id=self.run_id,
            node_id=node_id,
            artifact_id=artifact_id,
            status=status,
            bound_at=datetime.now(timezone.utc),
        )
        self._bindings[node_id] = b
        return b

    def get(self, node_id: str) -> Optional[RunArtifactBinding]:
        return self._bindings.get(node_id)

    def artifact_id_for(self, node_id: str) -> Optional[str]:
        b = self._bindings.get(node_id)
        return b.artifact_id if b else None

    def all(self) -> List[RunArtifactBinding]:
        return list(self._bindings.values())
