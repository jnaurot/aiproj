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


def _stable_dump(value: Dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256_json(value: Dict[str, Any]) -> str:
    return hashlib.sha256(_stable_dump(value).encode("utf-8")).hexdigest()


@dataclass
class ComponentRevision:
    component_id: str
    revision_id: str
    parent_revision_id: Optional[str]
    created_at: str
    message: Optional[str]
    schema_version: int
    checksum: str
    definition: Dict[str, Any]


class ComponentRevisionStore:
    """
    Component catalog and revision store.
    Follows the same additive sqlite pattern as GraphRevisionStore.
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
                    CREATE TABLE IF NOT EXISTS components (
                        component_id TEXT PRIMARY KEY,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        latest_revision_id TEXT
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS component_revisions (
                        revision_id TEXT PRIMARY KEY,
                        component_id TEXT NOT NULL,
                        parent_revision_id TEXT,
                        created_at TEXT NOT NULL,
                        message TEXT,
                        schema_version INTEGER NOT NULL,
                        checksum TEXT NOT NULL,
                        definition_json TEXT NOT NULL,
                        FOREIGN KEY(component_id) REFERENCES components(component_id)
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_component_revisions_component_created
                    ON component_revisions(component_id, created_at DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_component_revisions_component_parent
                    ON component_revisions(component_id, parent_revision_id)
                    """
                )
                conn.commit()

    def _latest_revision_id(self, conn: sqlite3.Connection, component_id: str) -> Optional[str]:
        row = conn.execute(
            "SELECT latest_revision_id FROM components WHERE component_id = ?",
            (component_id,),
        ).fetchone()
        if not row:
            return None
        latest = row["latest_revision_id"]
        return str(latest) if latest else None

    def create_revision(
        self,
        *,
        component_id: Optional[str],
        definition: Dict[str, Any],
        message: Optional[str] = None,
        parent_revision_id: Optional[str] = None,
        revision_id: Optional[str] = None,
        schema_version: int = 1,
    ) -> ComponentRevision:
        if not isinstance(definition, dict):
            raise ValueError("definition must be an object")
        graph = definition.get("graph")
        api = definition.get("api")
        if not isinstance(graph, dict) or "nodes" not in graph or "edges" not in graph:
            raise ValueError("definition.graph must include nodes and edges")
        if not isinstance(api, dict):
            raise ValueError("definition.api must be an object")
        if not isinstance(api.get("inputs"), list) or not isinstance(api.get("outputs"), list):
            raise ValueError("definition.api must include inputs[] and outputs[]")

        cid = str(component_id or "").strip() or f"component_{uuid4()}"
        rid = str(revision_id or "").strip() or f"crev_{uuid4()}"
        created_at = _iso_now()
        msg = str(message).strip() if isinstance(message, str) and str(message).strip() else None
        checksum = _sha256_json(definition)
        definition_json = _stable_dump(definition)

        with self._lock:
            with self._connect() as conn:
                current_latest = self._latest_revision_id(conn, cid)
                parent = (
                    str(parent_revision_id).strip()
                    if isinstance(parent_revision_id, str) and str(parent_revision_id).strip()
                    else current_latest
                )

                conn.execute(
                    """
                    INSERT INTO component_revisions (
                        revision_id, component_id, parent_revision_id, created_at,
                        message, schema_version, checksum, definition_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        rid,
                        cid,
                        parent,
                        created_at,
                        msg,
                        int(schema_version),
                        checksum,
                        definition_json,
                    ),
                )

                existing = conn.execute(
                    "SELECT component_id FROM components WHERE component_id = ?",
                    (cid,),
                ).fetchone()
                if existing:
                    conn.execute(
                        """
                        UPDATE components
                        SET updated_at = ?, latest_revision_id = ?
                        WHERE component_id = ?
                        """,
                        (created_at, rid, cid),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO components(component_id, created_at, updated_at, latest_revision_id)
                        VALUES (?, ?, ?, ?)
                        """,
                        (cid, created_at, created_at, rid),
                    )
                conn.commit()

        return ComponentRevision(
            component_id=cid,
            revision_id=rid,
            parent_revision_id=parent,
            created_at=created_at,
            message=msg,
            schema_version=int(schema_version),
            checksum=checksum,
            definition=definition,
        )

    def list_components(self, *, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        lim = max(1, min(int(limit), 500))
        off = max(0, int(offset))
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT component_id, created_at, updated_at, latest_revision_id
                    FROM components
                    ORDER BY updated_at DESC
                    LIMIT ? OFFSET ?
                    """,
                    (lim, off),
                ).fetchall()
                return [
                    {
                        "componentId": str(r["component_id"]),
                        "createdAt": str(r["created_at"]),
                        "updatedAt": str(r["updated_at"]),
                        "latestRevisionId": str(r["latest_revision_id"]) if r["latest_revision_id"] else None,
                    }
                    for r in rows
                ]

    def list_revisions(self, component_id: str, *, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        cid = str(component_id or "").strip()
        if not cid:
            return []
        lim = max(1, min(int(limit), 500))
        off = max(0, int(offset))
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT revision_id, component_id, parent_revision_id, created_at,
                           message, schema_version, checksum
                    FROM component_revisions
                    WHERE component_id = ?
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                    """,
                    (cid, lim, off),
                ).fetchall()
                return [
                    {
                        "revisionId": str(r["revision_id"]),
                        "componentId": str(r["component_id"]),
                        "parentRevisionId": str(r["parent_revision_id"]) if r["parent_revision_id"] else None,
                        "createdAt": str(r["created_at"]),
                        "message": str(r["message"]) if r["message"] else None,
                        "schemaVersion": int(r["schema_version"]),
                        "checksum": str(r["checksum"]),
                    }
                    for r in rows
                ]

    def get_revision(self, component_id: str, revision_id: str) -> Optional[ComponentRevision]:
        cid = str(component_id or "").strip()
        rid = str(revision_id or "").strip()
        if not cid or not rid:
            return None
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT revision_id, component_id, parent_revision_id, created_at,
                           message, schema_version, checksum, definition_json
                    FROM component_revisions
                    WHERE component_id = ? AND revision_id = ?
                    """,
                    (cid, rid),
                ).fetchone()
                if not row:
                    return None
                return self._row_to_revision(row)

    def _row_to_revision(self, row: sqlite3.Row) -> ComponentRevision:
        definition = json.loads(str(row["definition_json"]))
        return ComponentRevision(
            component_id=str(row["component_id"]),
            revision_id=str(row["revision_id"]),
            parent_revision_id=str(row["parent_revision_id"]) if row["parent_revision_id"] else None,
            created_at=str(row["created_at"]),
            message=str(row["message"]) if row["message"] else None,
            schema_version=int(row["schema_version"]),
            checksum=str(row["checksum"]),
            definition=definition,
        )

