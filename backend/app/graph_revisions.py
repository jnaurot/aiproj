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
    graph_name: Optional[str]
    revision_id: str
    parent_revision_id: Optional[str]
    created_at: str
    message: Optional[str]
    version_name: Optional[str]
    revision_kind: str
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
                        graph_name TEXT,
                        graph_name_norm TEXT,
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
                        version_name TEXT,
                        version_name_norm TEXT,
                        revision_kind TEXT NOT NULL DEFAULT 'save_graph',
                        schema_version INTEGER NOT NULL,
                        checksum TEXT NOT NULL,
                        graph_json TEXT NOT NULL,
                        FOREIGN KEY(graph_id) REFERENCES graphs(graph_id)
                    )
                    """
                )
                cols_graphs = [r[1] for r in cur.execute("PRAGMA table_info(graphs)").fetchall()]
                if "graph_name" not in cols_graphs:
                    cur.execute("ALTER TABLE graphs ADD COLUMN graph_name TEXT")
                if "graph_name_norm" not in cols_graphs:
                    cur.execute("ALTER TABLE graphs ADD COLUMN graph_name_norm TEXT")

                cols_revisions = [r[1] for r in cur.execute("PRAGMA table_info(graph_revisions)").fetchall()]
                if "version_name" not in cols_revisions:
                    cur.execute("ALTER TABLE graph_revisions ADD COLUMN version_name TEXT")
                if "version_name_norm" not in cols_revisions:
                    cur.execute("ALTER TABLE graph_revisions ADD COLUMN version_name_norm TEXT")
                if "revision_kind" not in cols_revisions:
                    cur.execute("ALTER TABLE graph_revisions ADD COLUMN revision_kind TEXT NOT NULL DEFAULT 'save_graph'")
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_graph_revisions_graph_created ON graph_revisions(graph_id, created_at DESC)"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_graph_revisions_graph_parent ON graph_revisions(graph_id, parent_revision_id)"
                )
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_graphs_name_norm ON graphs(graph_name_norm)")
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_graph_revisions_graph_version_name_norm
                    ON graph_revisions(graph_id, version_name_norm)
                    WHERE version_name_norm IS NOT NULL
                    """
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
        graph_name: Optional[str] = None,
        version_name: Optional[str] = None,
        revision_kind: str = "save_graph",
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
        gname = str(graph_name).strip() if isinstance(graph_name, str) and str(graph_name).strip() else None
        gname_norm = gname.lower() if gname else None
        vname = str(version_name).strip() if isinstance(version_name, str) and str(version_name).strip() else None
        vname_norm = vname.lower() if vname else None
        rkind = str(revision_kind or "").strip().lower() or "save_graph"
        if rkind not in {"save_graph", "save_version", "save_graph_as", "import"}:
            rkind = "save_graph"
        checksum = _sha256_json(graph)
        graph_json = _stable_dump(graph)

        with self._lock:
            with self._connect() as conn:
                if gname_norm:
                    existing_name = conn.execute(
                        "SELECT graph_id FROM graphs WHERE graph_name_norm = ?",
                        (gname_norm,),
                    ).fetchone()
                    if existing_name and str(existing_name["graph_id"]) != gid:
                        raise ValueError(f"graph name already exists: {gname}")
                current_latest = self._latest_revision_id(conn, gid)
                parent = (
                    str(parent_revision_id).strip()
                    if isinstance(parent_revision_id, str) and str(parent_revision_id).strip()
                    else current_latest
                )
                if vname_norm:
                    existing_version = conn.execute(
                        """
                        SELECT revision_id
                        FROM graph_revisions
                        WHERE graph_id = ? AND version_name_norm = ?
                        LIMIT 1
                        """,
                        (gid, vname_norm),
                    ).fetchone()
                    if existing_version:
                        raise ValueError(f"version name already exists in graph: {vname}")

                conn.execute(
                    """
                    INSERT INTO graph_revisions (
                        revision_id, graph_id, parent_revision_id, created_at,
                        message, version_name, version_name_norm, revision_kind,
                        schema_version, checksum, graph_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        rid,
                        gid,
                        parent,
                        created_at,
                        msg,
                        vname,
                        vname_norm,
                        rkind,
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
                        SET graph_name = COALESCE(?, graph_name),
                            graph_name_norm = COALESCE(?, graph_name_norm),
                            updated_at = ?, latest_revision_id = ?
                        WHERE graph_id = ?
                        """,
                        (gname, gname_norm, created_at, rid, gid),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO graphs(graph_id, graph_name, graph_name_norm, created_at, updated_at, latest_revision_id)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (gid, gname, gname_norm, created_at, created_at, rid),
                    )
                stored_graph_name_row = conn.execute(
                    "SELECT graph_name FROM graphs WHERE graph_id = ?",
                    (gid,),
                ).fetchone()
                conn.commit()

        return GraphRevision(
            graph_id=gid,
            graph_name=(
                str(stored_graph_name_row["graph_name"])
                if stored_graph_name_row and stored_graph_name_row["graph_name"]
                else None
            ),
            revision_id=rid,
            parent_revision_id=parent,
            created_at=created_at,
            message=msg,
            version_name=vname,
            revision_kind=rkind,
            schema_version=int(schema_version),
            checksum=checksum,
            graph=graph,
        )

    def list_graphs(self, *, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        lim = max(1, min(int(limit), 500))
        off = max(0, int(offset))
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT graph_id, graph_name, created_at, updated_at, latest_revision_id
                    FROM graphs
                    ORDER BY updated_at DESC
                    LIMIT ? OFFSET ?
                    """,
                    (lim, off),
                ).fetchall()
                return [
                    {
                        "graphId": str(r["graph_id"]),
                        "graphName": str(r["graph_name"]) if r["graph_name"] else None,
                        "createdAt": str(r["created_at"]),
                        "updatedAt": str(r["updated_at"]),
                        "latestRevisionId": str(r["latest_revision_id"]) if r["latest_revision_id"] else None,
                    }
                    for r in rows
                ]

    def get_latest(self, graph_id: str) -> Optional[GraphRevision]:
        gid = str(graph_id or "").strip()
        if not gid:
            return None
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT r.revision_id, r.graph_id, g.graph_name, r.parent_revision_id, r.created_at,
                           r.message, r.version_name, r.revision_kind, r.schema_version, r.checksum, r.graph_json
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
                    SELECT r.revision_id, r.graph_id, g.graph_name, r.parent_revision_id, r.created_at,
                           r.message, r.version_name, r.revision_kind, r.schema_version, r.checksum, r.graph_json
                    FROM graph_revisions r
                    JOIN graphs g ON g.graph_id = r.graph_id
                    WHERE r.graph_id = ? AND r.revision_id = ?
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
                           message, version_name, revision_kind, schema_version, checksum
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
                        "versionName": str(r["version_name"]) if r["version_name"] else None,
                        "revisionKind": str(r["revision_kind"] or "save_graph"),
                        "schemaVersion": int(r["schema_version"]),
                        "checksum": str(r["checksum"]),
                    }
                    for r in rows
                ]

    def delete_graph(self, graph_id: str) -> Dict[str, Any]:
        gid = str(graph_id or "").strip()
        if not gid:
            return {"deleted": False, "reason": "missing_graph_id"}
        with self._lock:
            with self._connect() as conn:
                existing = conn.execute(
                    "SELECT graph_id FROM graphs WHERE graph_id = ?",
                    (gid,),
                ).fetchone()
                if not existing:
                    return {"deleted": False, "reason": "graph_not_found"}
                count_row = conn.execute(
                    "SELECT COUNT(1) AS c FROM graph_revisions WHERE graph_id = ?",
                    (gid,),
                ).fetchone()
                revision_count = int(count_row["c"]) if count_row else 0
                conn.execute("DELETE FROM graph_revisions WHERE graph_id = ?", (gid,))
                conn.execute("DELETE FROM graphs WHERE graph_id = ?", (gid,))
                conn.commit()
                return {
                    "deleted": True,
                    "graphId": gid,
                    "deletedRevisionCount": revision_count,
                }

    def delete_revision(self, graph_id: str, revision_id: str) -> Dict[str, Any]:
        gid = str(graph_id or "").strip()
        rid = str(revision_id or "").strip()
        if not gid:
            return {"deleted": False, "reason": "missing_graph_id"}
        if not rid:
            return {"deleted": False, "reason": "missing_revision_id"}
        with self._lock:
            with self._connect() as conn:
                existing_graph = conn.execute(
                    "SELECT graph_id FROM graphs WHERE graph_id = ?",
                    (gid,),
                ).fetchone()
                if not existing_graph:
                    return {"deleted": False, "reason": "graph_not_found"}
                existing_revision = conn.execute(
                    "SELECT revision_id FROM graph_revisions WHERE graph_id = ? AND revision_id = ?",
                    (gid, rid),
                ).fetchone()
                if not existing_revision:
                    return {"deleted": False, "reason": "revision_not_found"}

                conn.execute(
                    "DELETE FROM graph_revisions WHERE graph_id = ? AND revision_id = ?",
                    (gid, rid),
                )
                remaining_count_row = conn.execute(
                    "SELECT COUNT(1) AS c FROM graph_revisions WHERE graph_id = ?",
                    (gid,),
                ).fetchone()
                remaining_count = int(remaining_count_row["c"]) if remaining_count_row else 0
                if remaining_count <= 0:
                    conn.execute("DELETE FROM graphs WHERE graph_id = ?", (gid,))
                    conn.commit()
                    return {
                        "deleted": True,
                        "graphDeleted": True,
                        "graphId": gid,
                        "revisionId": rid,
                    }

                newest_row = conn.execute(
                    """
                    SELECT revision_id
                    FROM graph_revisions
                    WHERE graph_id = ?
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (gid,),
                ).fetchone()
                newest_revision_id = str(newest_row["revision_id"]) if newest_row else None
                conn.execute(
                    """
                    UPDATE graphs
                    SET latest_revision_id = ?, updated_at = ?
                    WHERE graph_id = ?
                    """,
                    (newest_revision_id, _iso_now(), gid),
                )
                conn.commit()
                return {
                    "deleted": True,
                    "graphDeleted": False,
                    "graphId": gid,
                    "revisionId": rid,
                    "latestRevisionId": newest_revision_id,
                }

    def _row_to_revision(self, row: sqlite3.Row) -> GraphRevision:
        graph = json.loads(str(row["graph_json"]))
        return GraphRevision(
            graph_id=str(row["graph_id"]),
            graph_name=str(row["graph_name"]) if "graph_name" in row.keys() and row["graph_name"] else None,
            revision_id=str(row["revision_id"]),
            parent_revision_id=str(row["parent_revision_id"]) if row["parent_revision_id"] else None,
            created_at=str(row["created_at"]),
            message=str(row["message"]) if row["message"] else None,
            version_name=str(row["version_name"]) if "version_name" in row.keys() and row["version_name"] else None,
            revision_kind=str(row["revision_kind"] if "revision_kind" in row.keys() else "save_graph"),
            schema_version=int(row["schema_version"]),
            checksum=str(row["checksum"]),
            graph=graph,
        )

