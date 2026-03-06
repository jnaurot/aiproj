from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_json(value: Dict[str, Any]) -> str:
    s = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _stable_dump(value: Dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@dataclass
class GraphRevision:
    graph_id: str
    revision_id: str
    parent_revision_id: Optional[str]
    created_at: str
    message: Optional[str]
    schema_version: int
    checksum: str
    graph: Dict[str, Any]


class GraphRevisionStore:
    """
    Additive graph-revision store (Phase 1).
    Backed by sqlite, independent from run/artifact stores.
    """

    def __init__(self, db_path: str):
        self._db_path = str(db_path)
        db_parent = Path(self._db_path).resolve().parent
        db_parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._lock:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS graphs (
                        graph_id TEXT PRIMARY KEY,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        latest_revision_id TEXT
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS graph_revisions (
                        revision_id TEXT PRIMARY KEY,
                        graph_id TEXT NOT NULL,
                        parent_revision_id TEXT,
                        created_at TEXT NOT NULL,
                        message TEXT,
                        schema_version INTEGER NOT NULL,
                        checksum TEXT NOT NULL,
                        graph_json TEXT NOT NULL,
                        FOREIGN KEY(graph_id) REFERENCES graphs(graph_id)
                    )
                    """
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_graph_revisions_graph_created ON graph_revisions(graph_id, created_at DESC)"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_graph_revisions_graph_parent ON graph_revisions(graph_id, parent_revision_id)"
                )
                conn.commit()

    def _latest_revision_id(self, conn: sqlite3.Connection, graph_id: str) -> Optional[str]:
        row = conn.execute(
            "SELECT latest_revision_id FROM graphs WHERE graph_id = ?",
            (graph_id,),
        ).fetchone()
        if not row:
            return None
        latest = row["latest_revision_id"]
        return str(latest) if latest else None

    def create_revision(
        self,
        *,
        graph_id: Optional[str],
        graph: Dict[str, Any],
        message: Optional[str] = None,
        parent_revision_id: Optional[str] = None,
        revision_id: Optional[str] = None,
        schema_version: int = 1,
    ) -> GraphRevision:
        if not isinstance(graph, dict):
            raise ValueError("graph must be an object")
        if "nodes" not in graph or "edges" not in graph:
            raise ValueError("graph must include nodes and edges")

        gid = str(graph_id or "").strip() or f"graph_{uuid4()}"
        rid = str(revision_id or "").strip() or f"rev_{uuid4()}"
        created_at = _iso_now()
        msg = str(message).strip() if isinstance(message, str) and str(message).strip() else None
        checksum = _sha256_json(graph)
        graph_json = _stable_dump(graph)

        with self._lock:
            with self._connect() as conn:
                current_latest = self._latest_revision_id(conn, gid)
                parent = (
                    str(parent_revision_id).strip()
                    if isinstance(parent_revision_id, str) and str(parent_revision_id).strip()
                    else current_latest
                )

                conn.execute(
                    """
                    INSERT INTO graph_revisions (
                        revision_id, graph_id, parent_revision_id, created_at,
                        message, schema_version, checksum, graph_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        rid,
                        gid,
                        parent,
                        created_at,
                        msg,
                        int(schema_version),
                        checksum,
                        graph_json,
                    ),
                )

                existing = conn.execute(
                    "SELECT graph_id FROM graphs WHERE graph_id = ?",
                    (gid,),
                ).fetchone()
                if existing:
                    conn.execute(
                        """
                        UPDATE graphs
                        SET updated_at = ?, latest_revision_id = ?
                        WHERE graph_id = ?
                        """,
                        (created_at, rid, gid),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO graphs(graph_id, created_at, updated_at, latest_revision_id)
                        VALUES (?, ?, ?, ?)
                        """,
                        (gid, created_at, created_at, rid),
                    )
                conn.commit()

        return GraphRevision(
            graph_id=gid,
            revision_id=rid,
            parent_revision_id=parent,
            created_at=created_at,
            message=msg,
            schema_version=int(schema_version),
            checksum=checksum,
            graph=graph,
        )

    def get_latest(self, graph_id: str) -> Optional[GraphRevision]:
        gid = str(graph_id or "").strip()
        if not gid:
            return None
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT r.revision_id, r.graph_id, r.parent_revision_id, r.created_at,
                           r.message, r.schema_version, r.checksum, r.graph_json
                    FROM graph_revisions r
                    JOIN graphs g ON g.graph_id = r.graph_id
                    WHERE g.graph_id = ? AND g.latest_revision_id = r.revision_id
                    """,
                    (gid,),
                ).fetchone()
                if not row:
                    return None
                return self._row_to_revision(row)

    def get_revision(self, graph_id: str, revision_id: str) -> Optional[GraphRevision]:
        gid = str(graph_id or "").strip()
        rid = str(revision_id or "").strip()
        if not gid or not rid:
            return None
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT revision_id, graph_id, parent_revision_id, created_at,
                           message, schema_version, checksum, graph_json
                    FROM graph_revisions
                    WHERE graph_id = ? AND revision_id = ?
                    """,
                    (gid, rid),
                ).fetchone()
                if not row:
                    return None
                return self._row_to_revision(row)

    def list_revisions(self, graph_id: str, *, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        gid = str(graph_id or "").strip()
        if not gid:
            return []
        lim = max(1, min(int(limit), 500))
        off = max(0, int(offset))
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT revision_id, graph_id, parent_revision_id, created_at,
                           message, schema_version, checksum
                    FROM graph_revisions
                    WHERE graph_id = ?
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                    """,
                    (gid, lim, off),
                ).fetchall()
                return [
                    {
                        "revisionId": str(r["revision_id"]),
                        "graphId": str(r["graph_id"]),
                        "parentRevisionId": str(r["parent_revision_id"]) if r["parent_revision_id"] else None,
                        "createdAt": str(r["created_at"]),
                        "message": str(r["message"]) if r["message"] else None,
                        "schemaVersion": int(r["schema_version"]),
                        "checksum": str(r["checksum"]),
                    }
                    for r in rows
                ]

    def _row_to_revision(self, row: sqlite3.Row) -> GraphRevision:
        graph = json.loads(str(row["graph_json"]))
        return GraphRevision(
            graph_id=str(row["graph_id"]),
            revision_id=str(row["revision_id"]),
            parent_revision_id=str(row["parent_revision_id"]) if row["parent_revision_id"] else None,
            created_at=str(row["created_at"]),
            message=str(row["message"]) if row["message"] else None,
            schema_version=int(row["schema_version"]),
            checksum=str(row["checksum"]),
            graph=graph,
        )

