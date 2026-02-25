# backend/app/runner/cache.py
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


def _canon_json(obj: Any) -> str:
    # Stable canonical JSON for hashing
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


class ExecutionCache:
    """
    Artifact-keyed cache index.

    This is NOT the ArtifactStore.
    It's an index that maps an execution key -> artifact_id
    so the scheduler can skip execution and just bind the artifact.

    You can keep it in-memory for now.
    Later this becomes a DB table.
    """

    def __init__(self) -> None:
        # execution_key -> artifact_id
        self._index: Dict[str, str] = {}

    def params_hash(self, params: Dict[str, Any]) -> str:
        return sha256_hex(_canon_json(params))

    def execution_key(
        self,
        node_kind: str,
        normalized_params: Dict[str, Any],
        upstream_artifact_ids: Optional[List[str]],
        execution_version: str,
        input_bindings: Optional[List[Tuple[str, str]]] = None,
        determinism_env: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Deterministic execution key for artifact reuse.

        Note: upstream ids must be sorted to avoid nondeterminism due to edge ordering.
        """
        upstream_sorted = sorted(upstream_artifact_ids or [])
        bindings_sorted = sorted(
            [(str(p), str(aid)) for (p, aid) in (input_bindings or [])],
            key=lambda x: (x[0], x[1]),
        )
        key_obj = {
            "build_version": execution_version,
            "node_kind": node_kind,
            "normalized_params": normalized_params,
            "input_artifact_ids": upstream_sorted,
            "input_bindings": [{"port": p, "artifact_id": aid} for p, aid in bindings_sorted],
            "determinism_env": determinism_env or {},
        }
        return sha256_hex(_canon_json(key_obj))

    async def get_artifact_id(self, execution_key: str) -> Optional[str]:
        return self._index.get(execution_key)

    async def store_artifact_id(self, execution_key: str, artifact_id: str) -> None:
        self._index[execution_key] = artifact_id

    async def delete_artifact_ids(self, artifact_ids: List[str]) -> int:
        if not artifact_ids:
            return 0
        artifact_set = set(artifact_ids)
        to_delete = [k for k, v in self._index.items() if v in artifact_set]
        for k in to_delete:
            self._index.pop(k, None)
        return len(to_delete)


class SqliteExecutionCache(ExecutionCache):
    """
    Persistent execution-key -> artifact_id cache using SQLite.
    """

    def __init__(self, db_path: str) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS execution_cache (
                    execution_key TEXT PRIMARY KEY,
                    artifact_id TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            self._conn.commit()

    async def get_artifact_id(self, execution_key: str) -> Optional[str]:
        with self._lock:
            cur = self._conn.cursor()
            row = cur.execute(
                "SELECT artifact_id FROM execution_cache WHERE execution_key=?",
                (execution_key,),
            ).fetchone()
            return row[0] if row else None

    async def store_artifact_id(self, execution_key: str, artifact_id: str) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT INTO execution_cache (execution_key, artifact_id, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(execution_key) DO UPDATE SET
                    artifact_id=excluded.artifact_id
                """,
                (
                    execution_key,
                    artifact_id,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            self._conn.commit()

    async def delete_artifact_ids(self, artifact_ids: List[str]) -> int:
        if not artifact_ids:
            return 0
        with self._lock:
            cur = self._conn.cursor()
            placeholders = ",".join(["?"] * len(artifact_ids))
            cur.execute(
                f"DELETE FROM execution_cache WHERE artifact_id IN ({placeholders})",
                tuple(artifact_ids),
            )
            deleted = cur.rowcount
            self._conn.commit()
            return int(deleted)
