import json
import sqlite3
import asyncio
from datetime import datetime, timezone
from pathlib import Path

from app.runner.artifacts import DiskArtifactStore


def _init_legacy_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE artifacts (
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
    cur.execute(
        """
        INSERT INTO artifacts (
            artifact_id, content_hash, node_kind, params_hash, upstream_ids_json,
            created_at, execution_version, mime_type, size_bytes, storage_uri,
            payload_schema_json, run_id, node_id, exec_key
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "legacy-artifact-1",
            "a" * 64,
            "source",
            "p" * 64,
            "[]",
            datetime.now(timezone.utc).isoformat(),
            "v1",
            "text/plain; charset=utf-8",
            12,
            "artifact://legacy-artifact-1",
            json.dumps({"type": "string"}),
            "run-legacy",
            "node-legacy",
            None,
        ),
    )
    conn.commit()
    conn.close()


def test_payload_type_backfill_and_legacy_payload_schema_normalization(tmp_path):
    root = tmp_path / "artifact-root"
    db_path = root / "meta" / "artifacts.sqlite"
    _init_legacy_db(db_path)

    # Triggers schema migration + backfill in index init.
    store = DiskArtifactStore(root)

    conn = sqlite3.connect(str(db_path))
    row = conn.execute("SELECT payload_type FROM artifacts WHERE artifact_id=?", ("legacy-artifact-1",)).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "text"

    art = asyncio.run(store.get("legacy-artifact-1"))
    assert art.payload_type == "text"
    assert isinstance(art.payload_schema, dict)
    assert art.payload_schema.get("type") == "text"

