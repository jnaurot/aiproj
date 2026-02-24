import asyncio, time
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime, timezone

from .runner.events import RunEventBus
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

    node_status: Dict[str, str] = field(default_factory=dict)   # idle|active|done|error|skipped|blocked|paused
    node_outputs: Dict[str, str] = field(default_factory=dict)  # node_id -> artifact_id
    
class RuntimeManager:
    def __init__(self):
        print("RuntimeManager init from:", __file__)

        self.runs: Dict[str, RunHandle] = {}
        self._artifact_owner: dict[str, str] = {}
        self.artifact_store, self.cache = self._build_storage()

    def _build_storage(self):
        store_kind = (os.getenv("ARTIFACT_STORE") or "disk").strip().lower()
        if store_kind == "memory":
            return MemoryArtifactStore(), ExecutionCache()

        artifact_dir = Path(os.getenv("ARTIFACT_DIR") or "./data/artifacts").resolve()
        store = DiskArtifactStore(artifact_dir)
        cache_db = str((artifact_dir / "meta" / "artifacts.sqlite"))
        cache = SqliteExecutionCache(cache_db)
        return store, cache

    # ---------- creation ----------

    def create_run(self, run_id: str) -> RunHandle:
        handle = RunHandle(
            run_id=run_id,
            bus=None,
            artifact_store=self.artifact_store,
            cache=self.cache,
        )
        bus = RunEventBus(run_id, on_emit=lambda ev: self._apply_event_to_state(handle, ev))
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
            handle.task.cancel()

        result = await self.artifact_store.delete_run(run_id, mode=mode, gc=gc)
        removed_ids = result.get("artifactIdsRemoved", []) or []
        cache_removed = await self.cache.delete_artifact_ids(removed_ids)
        result["cacheRowsRemoved"] = cache_removed

        for aid in removed_ids:
            self._artifact_owner.pop(aid, None)

        if mode == "hard":
            self.runs.pop(run_id, None)
        else:
            h = self.runs.get(run_id)
            if h:
                h.status = "deleted"
        return result
    
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

    async def start_run(self, run_id: str, graph, run_from):
        handle = self.runs[run_id]
        print("Scheduling run task:", run_id, "loop:", asyncio.get_running_loop())

        handle.task = asyncio.create_task(
            run_graph(
                run_id,
                graph,
                run_from,
                handle.bus,
                artifact_store=handle.artifact_store,
                cache=handle.cache,
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


