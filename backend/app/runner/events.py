import asyncio
import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Protocol


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class EventStore(Protocol):
    async def append_event(self, evt: Dict[str, Any]) -> int: ...
    async def list_events(self, run_id: str, *, after_id: int = 0, limit: int = 500) -> List[Dict[str, Any]]: ...
    async def delete_run_events(self, run_id: str) -> int: ...
    async def prune_events(
        self,
        *,
        keep_last: int,
        dry_run: bool = True,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]: ...


class MemoryEventStore:
    def __init__(self) -> None:
        self._rows: List[Dict[str, Any]] = []
        self._next_id = 1

    async def append_event(self, evt: Dict[str, Any]) -> int:
        row_id = self._next_id
        self._next_id += 1
        self._rows.append(
            {
                "id": row_id,
                "runId": str(evt.get("runId") or ""),
                "ts": str(evt.get("at") or _iso_now()),
                "seq": int(evt.get("seq") or 0),
                "type": str(evt.get("type") or "unknown"),
                "payload": dict(evt),
            }
        )
        return row_id

    async def list_events(self, run_id: str, *, after_id: int = 0, limit: int = 500) -> List[Dict[str, Any]]:
        lim = max(1, int(limit))
        return [
            r for r in self._rows if r["runId"] == run_id and int(r["id"]) > int(after_id)
        ][:lim]

    async def delete_run_events(self, run_id: str) -> int:
        before = len(self._rows)
        self._rows = [r for r in self._rows if r["runId"] != run_id]
        return before - len(self._rows)

    async def prune_events(
        self,
        *,
        keep_last: int,
        dry_run: bool = True,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        keep = max(0, int(keep_last))
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for row in self._rows:
            rid = str(row.get("runId") or "")
            if run_id and rid != run_id:
                continue
            grouped.setdefault(rid, []).append(row)

        rows_deleted = 0
        runs_affected = 0
        oldest_remaining: Optional[int] = None
        keep_ids: set[int] = set()
        for rid, rows in grouped.items():
            rows_sorted = sorted(rows, key=lambda r: int(r["id"]))
            if len(rows_sorted) <= keep:
                if rows_sorted:
                    oldest = int(rows_sorted[0]["id"])
                    oldest_remaining = oldest if oldest_remaining is None else min(oldest_remaining, oldest)
                keep_ids.update(int(r["id"]) for r in rows_sorted)
                continue
            runs_affected += 1
            delete_count = len(rows_sorted) - keep
            rows_deleted += delete_count
            kept = rows_sorted[-keep:] if keep > 0 else []
            keep_ids.update(int(r["id"]) for r in kept)
            if kept:
                oldest = int(kept[0]["id"])
                oldest_remaining = oldest if oldest_remaining is None else min(oldest_remaining, oldest)

        if not dry_run:
            if run_id:
                self._rows = [
                    r for r in self._rows if str(r.get("runId") or "") != run_id or int(r["id"]) in keep_ids
                ]
            else:
                self._rows = [r for r in self._rows if int(r["id"]) in keep_ids or str(r.get("runId") or "") not in grouped]

        return {
            "keep_last": keep,
            "dry_run": bool(dry_run),
            "rows_deleted": int(rows_deleted),
            "runs_affected": int(runs_affected),
            "oldest_remaining_event_id": oldest_remaining,
            "run_id": run_id,
        }


class SqliteEventStore:
    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS run_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    seq INTEGER,
                    type TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_run_events_run_id_id ON run_events(run_id, id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_run_events_run_id_type ON run_events(run_id, type)")
            self._conn.commit()

    async def append_event(self, evt: Dict[str, Any]) -> int:
        run_id = str(evt.get("runId") or "")
        ts = str(evt.get("at") or _iso_now())
        seq_raw = evt.get("seq")
        seq = int(seq_raw) if isinstance(seq_raw, int) else None
        etype = str(evt.get("type") or "unknown")
        payload_json = json.dumps(evt, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT INTO run_events (run_id, ts, seq, type, payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (run_id, ts, seq, etype, payload_json),
            )
            row_id = int(cur.lastrowid)
            self._conn.commit()
            return row_id

    async def list_events(self, run_id: str, *, after_id: int = 0, limit: int = 500) -> List[Dict[str, Any]]:
        lim = max(1, min(int(limit), 5000))
        with self._lock:
            cur = self._conn.cursor()
            rows = cur.execute(
                """
                SELECT id, run_id, ts, seq, type, payload_json
                FROM run_events
                WHERE run_id=? AND id>?
                ORDER BY id ASC
                LIMIT ?
                """,
                (run_id, int(after_id), lim),
            ).fetchall()

        out: List[Dict[str, Any]] = []
        for rid, rr, ts, seq, rtype, payload_json in rows:
            payload: Dict[str, Any]
            try:
                payload = json.loads(payload_json)
            except Exception:
                payload = {"type": rtype, "runId": rr, "at": ts}
            out.append(
                {
                    "id": int(rid),
                    "runId": rr,
                    "ts": ts,
                    "seq": int(seq) if seq is not None else None,
                    "type": rtype,
                    "payload": payload,
                }
            )
        return out

    async def delete_run_events(self, run_id: str) -> int:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("DELETE FROM run_events WHERE run_id=?", (run_id,))
            deleted = int(cur.rowcount)
            self._conn.commit()
            return deleted

    async def prune_events(
        self,
        *,
        keep_last: int,
        dry_run: bool = True,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        keep = max(0, int(keep_last))
        with self._lock:
            cur = self._conn.cursor()
            if run_id:
                runs = cur.execute(
                    "SELECT run_id, COUNT(*) FROM run_events WHERE run_id=? GROUP BY run_id",
                    (run_id,),
                ).fetchall()
            else:
                runs = cur.execute(
                    "SELECT run_id, COUNT(*) FROM run_events GROUP BY run_id"
                ).fetchall()

            rows_deleted = 0
            runs_affected = 0
            for rid, count in runs:
                count_i = int(count or 0)
                if count_i <= keep:
                    continue
                runs_affected += 1
                rows_deleted += (count_i - keep)
                if not dry_run:
                    if keep <= 0:
                        cur.execute("DELETE FROM run_events WHERE run_id=?", (rid,))
                    else:
                        cutoff_row = cur.execute(
                            """
                            SELECT id
                            FROM run_events
                            WHERE run_id=?
                            ORDER BY id DESC
                            LIMIT 1 OFFSET ?
                            """,
                            (rid, keep - 1),
                        ).fetchone()
                        if cutoff_row:
                            cutoff_id = int(cutoff_row[0])
                            cur.execute(
                                "DELETE FROM run_events WHERE run_id=? AND id<?",
                                (rid, cutoff_id),
                            )

            oldest_row = cur.execute("SELECT MIN(id) FROM run_events").fetchone()
            oldest_remaining = int(oldest_row[0]) if oldest_row and oldest_row[0] is not None else None

            if not dry_run:
                self._conn.commit()

        return {
            "keep_last": keep,
            "dry_run": bool(dry_run),
            "rows_deleted": int(rows_deleted),
            "runs_affected": int(runs_affected),
            "oldest_remaining_event_id": oldest_remaining,
            "run_id": run_id,
        }

class RunEventBus:
    def __init__(
        self,
        run_id: str,
        graph_id: Optional[str] = None,
        on_emit: Optional[Callable[[Dict[str, Any]], None]] = None,
        persist_event: Optional[Callable[[Dict[str, Any]], Awaitable[int] | int | None]] = None,
    ):
        self.run_id = run_id
        self.graph_id = str(graph_id or "")
        self._seq = 0
        self.q = asyncio.Queue()
        self._on_emit = on_emit
        self._persist_event = persist_event

    async def emit(self, evt: dict):
        if "runId" not in evt:
            evt["runId"] = self.run_id
        if "graphId" not in evt and self.graph_id:
            evt["graphId"] = self.graph_id
        self._seq += 1
        evt["seq"] = self._seq
        if self._persist_event:
            try:
                maybe_awaitable = self._persist_event(evt)
                if asyncio.iscoroutine(maybe_awaitable):
                    event_id = await maybe_awaitable
                else:
                    event_id = maybe_awaitable
                if event_id is not None:
                    evt["eventId"] = int(event_id)
            except Exception as e:
                # persistence should not break live execution
                print("[RunEventBus] persist_event error:", e, "evt=", evt)
        if self._on_emit:
            try:
                self._on_emit(evt)   # sync hook
            except Exception as e:
                # NEVER let state projection kill the runtime
                print("[RunEventBus] on_emit error:", e, "evt=", evt)
        await self.q.put(evt)


    async def next_event(self) -> Dict[str, Any]:
        return await self.q.get()
