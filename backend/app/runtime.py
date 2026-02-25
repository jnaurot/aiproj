import asyncio, time
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime, timezone

from .runner.events import EventStore, MemoryEventStore, RunEventBus, SqliteEventStore
from .runner.artifacts import ArtifactStore, DiskArtifactStore, MemoryArtifactStore
from .runner.cache import ExecutionCache, SqliteExecutionCache
from .runner.run import run_graph


def datetime_from_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


@dataclass
class RunHandle:
    run_id: str
    bus: RunEventBus
    artifact_store: ArtifactStore
    cache: ExecutionCache
    task: Optional[asyncio.Task] = None

    created_at: float = field(default_factory=lambda: time.time())
    status: str = "pending"  # pending|running|finished|failed|canceled
    error: Optional[str] = None
    cancel_requested_at: Optional[float] = None
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)

    node_status: Dict[str, str] = field(default_factory=dict)   # idle|active|done|error|skipped|blocked|paused
    node_outputs: Dict[str, str] = field(default_factory=dict)  # node_id -> artifact_id
    
class RuntimeManager:
    def __init__(self):
        print("RuntimeManager init from:", __file__)

        self.runs: Dict[str, RunHandle] = {}
        self._artifact_owner: dict[str, str] = {}
        self.artifact_store, self.cache, self.event_store = self._build_storage()

    def _build_storage(self):
        store_kind = (os.getenv("ARTIFACT_STORE") or "disk").strip().lower()
        if store_kind == "memory":
            return MemoryArtifactStore(), ExecutionCache(), MemoryEventStore()

        artifact_dir = Path(os.getenv("ARTIFACT_DIR") or "./data/artifacts").resolve()
        store = DiskArtifactStore(artifact_dir)
        cache_db = str((artifact_dir / "meta" / "artifacts.sqlite"))
        cache = SqliteExecutionCache(cache_db)
        event_store: EventStore = SqliteEventStore(cache_db)
        return store, cache, event_store

    # ---------- creation ----------

    def create_run(self, run_id: str) -> RunHandle:
        handle = RunHandle(
            run_id=run_id,
            bus=None,
            artifact_store=self.artifact_store,
            cache=self.cache,
        )
        bus = RunEventBus(
            run_id,
            on_emit=lambda ev: self._apply_event_to_state(handle, ev),
            persist_event=lambda ev: self.event_store.append_event(ev),
        )
        handle.bus = bus
        
        self.runs[run_id] = handle
        print("BUS INIT OK:", bus, "has on_emit:", hasattr(bus, "_on_emit"))
        asyncio.create_task(self.artifact_store.record_run(run_id, "pending"))

        return handle

    def get_run(self, run_id: str) -> Optional[RunHandle]:
        return self.runs.get(run_id)

    async def list_runs(self, include_deleted: bool = False) -> list[Dict[str, Any]]:
        persisted = await self.artifact_store.list_runs(include_deleted=include_deleted)
        out: Dict[str, Dict[str, Any]] = {r["run_id"]: dict(r) for r in persisted}
        for rid, h in self.runs.items():
            if h.status == "deleted" and not include_deleted:
                continue
            out[rid] = {
                "run_id": rid,
                "created_at": datetime_from_ts(h.created_at),
                "status": h.status,
                "deleted_at": None,
            }
        rows = list(out.values())
        rows.sort(key=lambda r: str(r.get("created_at") or ""), reverse=True)
        return rows

    async def delete_run(self, run_id: str, mode: str = "soft", gc: str = "none") -> Dict[str, Any]:
        handle = self.runs.get(run_id)
        if handle and handle.task and not handle.task.done():
            await self.request_cancel(run_id)

        result = await self.artifact_store.delete_run(run_id, mode=mode, gc=gc)
        removed_ids = result.get("artifactIdsRemoved", []) or []
        cache_removed = await self.cache.delete_artifact_ids(removed_ids)
        result["cacheRowsRemoved"] = cache_removed

        for aid in removed_ids:
            self._artifact_owner.pop(aid, None)

        if mode == "hard":
            self.runs.pop(run_id, None)
            await self.event_store.delete_run_events(run_id)
        else:
            h = self.runs.get(run_id)
            if h:
                h.status = "deleted"
        return result

    async def list_run_events(self, run_id: str, *, after_id: int = 0, limit: int = 500) -> list[Dict[str, Any]]:
        return await self.event_store.list_events(run_id, after_id=after_id, limit=limit)

    async def _list_all_run_events(self, run_id: str) -> list[Dict[str, Any]]:
        out: list[Dict[str, Any]] = []
        after_id = 0
        while True:
            rows = await self.event_store.list_events(run_id, after_id=after_id, limit=2000)
            if not rows:
                break
            out.extend(rows)
            after_id = int(rows[-1].get("id") or after_id)
            if len(rows) < 2000:
                break
        return out

    async def recover_unfinished_runs(self) -> Dict[str, Any]:
        terminal = {"succeeded", "failed", "cancelled", "deleted"}
        unfinished = {"pending", "running", "cancel_requested"}
        recs = await self.artifact_store.list_runs(include_deleted=True)
        recovered = 0
        scanned = 0

        for rec in recs:
            run_id = str(rec.get("run_id") or "")
            status = str(rec.get("status") or "").strip().lower()
            if not run_id or status in terminal or status not in unfinished:
                continue
            scanned += 1
            rows = await self._list_all_run_events(run_id)
            payloads = [dict(r.get("payload") or {}) for r in rows]
            has_run_finished = any(p.get("type") == "run_finished" for p in payloads)

            # If persisted events already contain terminal status, normalize run table and continue.
            if has_run_finished:
                last_finished = [p for p in payloads if p.get("type") == "run_finished"][-1]
                await self.artifact_store.update_run_status(run_id, str(last_finished.get("status") or "failed"))
                continue

            decisions = [p for p in payloads if p.get("type") == "cache_decision"]
            has_cache_summary = any(p.get("type") == "cache_summary" for p in payloads)
            if not has_cache_summary:
                cache_hit = sum(1 for p in decisions if p.get("decision") == "cache_hit")
                cache_miss = sum(1 for p in decisions if p.get("decision") == "cache_miss")
                cache_hit_contract_mismatch = sum(
                    1 for p in decisions if p.get("decision") == "cache_hit_contract_mismatch"
                )
                await self.event_store.append_event(
                    {
                        "type": "cache_summary",
                        "schema_version": 1,
                        "runId": run_id,
                        "at": datetime_from_ts(time.time()),
                        "cache_hit": int(cache_hit),
                        "cache_miss": int(cache_miss),
                        "cache_hit_contract_mismatch": int(cache_hit_contract_mismatch),
                    }
                )

            if status == "cancel_requested":
                recovered_status = "cancelled"
                reason = "RECOVERED_CANCEL_REQUESTED_ON_STARTUP"
                await self.event_store.append_event(
                    {
                        "type": "run_cancelled",
                        "runId": run_id,
                        "at": datetime_from_ts(time.time()),
                        "reason": reason,
                    }
                )
            else:
                recovered_status = "failed"
                reason = "RECOVERED_UNFINISHED_RUN_ON_STARTUP"

            await self.event_store.append_event(
                {
                    "type": "run_finished",
                    "runId": run_id,
                    "at": datetime_from_ts(time.time()),
                    "status": recovered_status,
                    "error": reason,
                    "recovered": True,
                }
            )
            await self.artifact_store.update_run_status(run_id, recovered_status)
            recovered += 1

        return {"scanned": scanned, "recovered": recovered}

    async def prune_events(
        self,
        *,
        keep_last: int,
        dry_run: bool = True,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        return await self.event_store.prune_events(keep_last=keep_last, dry_run=dry_run, run_id=run_id)

    async def request_cancel(self, run_id: str) -> Dict[str, Any]:
        handle = self.runs.get(run_id)
        if not handle:
            return {"runId": run_id, "found": False, "cancelRequested": False, "status": "unknown"}

        terminal = {"succeeded", "failed", "cancelled", "deleted"}
        if handle.status in terminal:
            return {"runId": run_id, "found": True, "cancelRequested": False, "status": handle.status}

        if handle.cancel_event.is_set() or handle.status == "cancel_requested":
            return {"runId": run_id, "found": True, "cancelRequested": True, "status": "cancel_requested"}

        handle.cancel_requested_at = time.time()
        handle.status = "cancel_requested"
        handle.cancel_event.set()
        await handle.bus.emit(
            {
                "type": "run_cancel_requested",
                "runId": run_id,
                "at": datetime_from_ts(handle.cancel_requested_at),
            }
        )
        return {"runId": run_id, "found": True, "cancelRequested": True, "status": "cancel_requested"}
    
    # ----------------------artifacts------------------------

    async def resolve_artifact_owner(self, artifact_id: str) -> str | None:
        owner = self._artifact_owner.get(artifact_id)
        if owner:
            return owner
        if not await self.artifact_store.exists(artifact_id):
            return None
        try:
            art = await self.artifact_store.get(artifact_id)
        except Exception:
            return None
        if art.run_id:
            self._artifact_owner[artifact_id] = art.run_id
            return art.run_id
        return None

    # ---------- execution ----------

    async def start_run(self, run_id: str, graph, run_from, run_mode: Optional[str] = None):
        handle = self.runs[run_id]
        print("Scheduling run task:", run_id, "loop:", asyncio.get_running_loop())

        handle.task = asyncio.create_task(
            run_graph(
                run_id,
                graph,
                run_from,
                handle.bus,
                run_mode=run_mode,
                artifact_store=handle.artifact_store,
                cache=handle.cache,
                cancel_event=handle.cancel_event,
            )
        )

    def _apply_event_to_state(self, handle, ev: dict) -> None:
        t = ev.get("type")
        if handle.status == "deleted":
            return

        # run lifecycle
        if t == "run_started":
            handle.status = "running"
            asyncio.create_task(self.artifact_store.update_run_status(handle.run_id, "running"))
            return

        if t == "run_cancel_requested":
            handle.status = "cancel_requested"
            asyncio.create_task(self.artifact_store.update_run_status(handle.run_id, "cancel_requested"))
            return

        if t == "run_cancelled":
            handle.status = "cancelled"
            asyncio.create_task(self.artifact_store.update_run_status(handle.run_id, "cancelled"))
            return

        if t == "run_finished":
            handle.status = ev.get("status", "finished")
            asyncio.create_task(self.artifact_store.update_run_status(handle.run_id, handle.status))
            return

        # node lifecycle
        if t == "node_started":
            nid = ev.get("nodeId")
            if nid:
                handle.node_status[nid] = "running"
            return

        if t == "node_finished":
            nid = ev.get("nodeId")
            if nid:
                handle.node_status[nid] = ev.get("status", "succeeded")
            return

        if t == "node_cancelled":
            nid = ev.get("nodeId")
            if nid:
                handle.node_status[nid] = "cancelled"
            return

        # artifacts
        if t == "node_output":
            nid = ev.get("nodeId")
            aid = ev.get("artifactId")
            if nid and aid:
                handle.node_outputs[nid] = aid
                # Option B registry (artifact → runId)
                self._artifact_owner[aid] = handle.run_id
            return

        # optional: edge exec
        if t == "edge_exec":
            # you can store per-edge exec if you want (optional)
            return

        if t == "node_blocked":
            handle.node_status[nid] = "blocked"
            return

        if t == "node_paused":
            handle.node_status[nid] = "paused"
            return

        if t == "node_resumed":
            handle.node_status[nid] = "active"
            return


