import asyncio, time
import os
import traceback
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime, timezone

from .runner.events import EventStore, MemoryEventStore, RunEventBus, SqliteEventStore
from .runner.artifacts import ArtifactStore, DiskArtifactStore, MemoryArtifactStore
from .runner.cache import ExecutionCache, SqliteExecutionCache
from .runner.run import run_graph

logger = logging.getLogger(__name__)


def datetime_from_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()



@dataclass
class RunHandle:
    run_id: str
    graph_id: str
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
    node_bindings: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # node_id -> ui binding state
    active_run_planned: set[str] = field(default_factory=set)
    graph: Optional[Dict[str, Any]] = None
    
class RuntimeManager:
    def __init__(self):
        print("RuntimeManager init from:", __file__)

        self.runs: Dict[str, RunHandle] = {}
        self._artifact_owner: dict[str, str] = {}
        self.global_cache_mode: str = "default_on"
        self.artifact_store, self.cache, self.event_store = self._build_storage()

    def set_global_cache_enabled(self, enabled: bool) -> None:
        self.set_global_cache_mode("default_on" if bool(enabled) else "force_off")

    def get_global_cache_enabled(self) -> bool:
        return self.get_global_cache_mode() != "force_off"

    def set_global_cache_mode(self, mode: str) -> None:
        m = str(mode or "").strip().lower()
        if m not in {"default_on", "force_off", "force_on"}:
            m = "default_on"
        self.global_cache_mode = m

    def get_global_cache_mode(self) -> str:
        m = str(getattr(self, "global_cache_mode", "default_on")).strip().lower()
        if m not in {"default_on", "force_off", "force_on"}:
            return "default_on"
        return m

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
            graph_id="",
            bus=None,
            artifact_store=self.artifact_store,
            cache=self.cache,
        )
        bus = RunEventBus(
            run_id,
            graph_id="",
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

    def _binding_for(self, handle: RunHandle, node_id: str) -> Dict[str, Any]:
        b = handle.node_bindings.get(node_id)
        if b is None:
            b = {
                "graphId": handle.graph_id,
                "status": "idle",
                "lastArtifactId": None,
                "lastRunId": None,
                "lastExecKey": None,
                "currentExecKey": None,
                "currentArtifactId": None,
                "currentRunId": None,
                "isUpToDate": False,
                "cacheValid": False,
                "staleReason": None,
            }
            handle.node_bindings[node_id] = b
        return b

    def _log_stale_regression(
        self,
        *,
        handle: RunHandle,
        node_id: str,
        prev: Dict[str, Any],
        nxt: Dict[str, Any],
        ev: Dict[str, Any],
    ) -> None:
        prev_status = str(prev.get("status") or "")
        prev_up_to_date = prev.get("isUpToDate")
        next_status = str(nxt.get("status") or "")
        next_up_to_date = nxt.get("isUpToDate")
        was_succeeded = (prev_status == "succeeded_up_to_date") or (prev_up_to_date is True)
        became_stale = (next_status == "stale") or (next_up_to_date is False)
        if not (was_succeeded and became_stale):
            return
        payload = {
            "type": "SUCCEEDED_TO_STALE",
            "runId": handle.run_id,
            "eventType": ev.get("type"),
            "event": dict(ev),
            "nodeId": node_id,
            "previousBinding": dict(prev),
            "nextBinding": dict(nxt),
            "nodeInPlannedSet": (node_id in handle.active_run_planned) if handle.active_run_planned else None,
            "plannedNodeCount": len(handle.active_run_planned),
            "stack": "".join(traceback.format_stack(limit=12)),
        }
        print("[binding-regression]", payload)
        strict = os.getenv("RUNTIME_STRICT_STALE_TRANSITIONS", "").strip().lower()
        if strict in {"1", "true", "yes", "on"}:
            raise RuntimeError(f"SUCCEEDED_TO_STALE node={node_id} event={ev.get('type')}")

    def _log_binding_update(
        self,
        *,
        handle: RunHandle,
        node_id: str,
        event_type: str,
        prev: Dict[str, Any],
        nxt: Dict[str, Any],
    ) -> None:
        logger.debug(
            "binding_update run_id=%s node_id=%s event=%s prev_status=%s next_status=%s prev_isUpToDate=%s next_isUpToDate=%s",
            handle.run_id,
            node_id,
            event_type,
            prev.get("status"),
            nxt.get("status"),
            prev.get("isUpToDate"),
            nxt.get("isUpToDate"),
        )

    def _debug_assert_sibling_status_unchanged(
        self,
        *,
        handle: RunHandle,
        before_status_by_node: Dict[str, Any],
        excluded_node_ids: set[str],
        reason: str,
    ) -> None:
        regressions: list[Dict[str, Any]] = []
        for nid, prev_status in before_status_by_node.items():
            if nid in excluded_node_ids:
                continue
            current_binding = handle.node_bindings.get(nid) or {}
            next_status = current_binding.get("status")
            if next_status != prev_status:
                regressions.append(
                    {
                        "nodeId": nid,
                        "previousStatus": prev_status,
                        "nextStatus": next_status,
                    }
                )
        if not regressions:
            return
        payload = {
            "type": "SIBLING_STATUS_CHANGED_DURING_INVALIDATION",
            "runId": handle.run_id,
            "reason": reason,
            "regressions": regressions,
            "count": len(regressions),
            "stack": "".join(traceback.format_stack(limit=12)),
        }
        print("[invalidation-regression]", payload)
        strict = os.getenv("RUNTIME_STRICT_INVALIDATION_ASSERTS", "").strip().lower()
        if strict in {"1", "true", "yes", "on"}:
            raise RuntimeError("SIBLING_STATUS_CHANGED_DURING_INVALIDATION")

    def _downstream_nodes(self, graph: Dict[str, Any], node_id: str) -> set[str]:
        edges = graph.get("edges", []) if isinstance(graph, dict) else []
        adj: Dict[str, list[str]] = {}
        for e in edges:
            s = e.get("source")
            t = e.get("target")
            if isinstance(s, str) and isinstance(t, str):
                adj.setdefault(s, []).append(t)
        seen: set[str] = set()
        q = [node_id]
        while q:
            cur = q.pop(0)
            for nxt in adj.get(cur, []):
                if nxt not in seen:
                    seen.add(nxt)
                    q.append(nxt)
        return seen

    def invalidate_node(
        self,
        handle: RunHandle,
        node_id: str,
        *,
        reason: str,
        graph: Optional[Dict[str, Any]] = None,
    ) -> set[str]:
        graph_ref = graph if isinstance(graph, dict) else (handle.graph or {})
        before_status_by_node = {
            nid: (binding.get("status") if isinstance(binding, dict) else None)
            for nid, binding in handle.node_bindings.items()
        }
        candidate_ids = {node_id} | self._downstream_nodes(graph_ref, node_id)
        invalidated: set[str] = set()
        for nid in sorted(candidate_ids):
            b = handle.node_bindings.get(nid)
            if not isinstance(b, dict):
                continue
            had_artifact = bool(b.get("currentArtifactId") or b.get("lastArtifactId"))
            if not had_artifact:
                continue
            b["status"] = "stale"
            b["isUpToDate"] = False
            b["cacheValid"] = False
            b["currentArtifactId"] = None
            b["currentRunId"] = None
            b["currentExecKey"] = None
            b["staleReason"] = reason if nid == node_id else "UPSTREAM_CHANGED"
            handle.node_status[nid] = "stale"
            handle.node_outputs.pop(nid, None)
            invalidated.add(nid)
        self._debug_assert_sibling_status_unchanged(
            handle=handle,
            before_status_by_node=before_status_by_node,
            excluded_node_ids=candidate_ids,
            reason=reason,
        )
        return invalidated

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

    async def accept_node_params(
        self,
        *,
        run_id: str,
        graph: Dict[str, Any],
        node_id: str,
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        handle = self.runs.get(run_id)
        if not handle:
            raise KeyError(run_id)
        if handle.status == "running":
            raise RuntimeError("Cannot accept params while run is active")

        g = graph if isinstance(graph, dict) else (handle.graph or {})
        nodes = g.get("nodes", []) if isinstance(g, dict) else []
        target = None
        for n in nodes:
            if n.get("id") == node_id:
                target = n
                break
        if target is None:
            raise ValueError(f"Unknown node_id: {node_id}")

        data = target.setdefault("data", {})
        data["params"] = dict(params or {})
        handle.graph = g

        affected = self.invalidate_node(handle, node_id, reason="PARAMS_CHANGED", graph=g)

        return {
            "runId": run_id,
            "nodeId": node_id,
            "affectedNodeIds": sorted(affected),
            "status": "accepted",
        }
    
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

    async def delete_node_artifacts(
        self,
        *,
        run_id: str,
        node_id: str,
        graph: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        handle = self.runs.get(run_id)
        if not handle:
            raise KeyError(run_id)
        graph_ref = graph if isinstance(graph, dict) else (handle.graph or {})
        result = await self.artifact_store.delete_node_artifacts(
            graph_id=handle.graph_id,
            node_id=node_id,
        )
        removed_ids = list(result.get("artifactIdsRemoved") or [])
        if removed_ids:
            await self.cache.delete_artifact_ids(removed_ids)
            for aid in removed_ids:
                self._artifact_owner.pop(aid, None)
        affected = self.invalidate_node(handle, node_id, reason="NODE_DELETED", graph=graph_ref)
        # Node deletion semantics: deleted node no longer has binding/output/status state.
        handle.node_bindings.pop(node_id, None)
        handle.node_outputs.pop(node_id, None)
        handle.node_status.pop(node_id, None)
        affected.discard(node_id)
        result["affectedNodeIds"] = sorted(affected)
        return result

    # ---------- execution ----------

    async def start_run(self, run_id: str, graph, run_from, run_mode: Optional[str] = None, graph_id: Optional[str] = None):
        handle = self.runs[run_id]
        if not str(graph_id or "").strip():
            raise ValueError("graph_id is required")
        handle.graph_id = str(graph_id)
        handle.bus.graph_id = handle.graph_id
        handle.graph = graph
        for n in (graph.get("nodes", []) if isinstance(graph, dict) else []):
            nid = n.get("id")
            if not isinstance(nid, str) or not nid:
                continue
            handle.node_status.setdefault(nid, "idle")
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
                runtime_ref=self,
                graph_id=handle.graph_id,
            )
        )

    def _apply_event_to_state(self, handle, ev: dict) -> None:
        t = ev.get("type")
        if handle.status == "deleted":
            return

        # run lifecycle
        if t == "run_started":
            handle.status = "running"
            planned = ev.get("plannedNodeIds") or []
            if isinstance(planned, list):
                handle.active_run_planned = {str(x) for x in planned if isinstance(x, str) and x}
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
            handle.active_run_planned = set()
            asyncio.create_task(self.artifact_store.update_run_status(handle.run_id, handle.status))
            return

        # node lifecycle
        if t == "node_started":
            nid = ev.get("nodeId")
            if nid:
                b = self._binding_for(handle, nid)
                prev = dict(b)
                b["status"] = "running"
                handle.node_status[nid] = "running"
                self._log_binding_update(handle=handle, node_id=nid, event_type=t, prev=prev, nxt=b)
                self._log_stale_regression(handle=handle, node_id=nid, prev=prev, nxt=b, ev=ev)
            return

        if t == "node_finished":
            nid = ev.get("nodeId")
            if nid:
                status = str(ev.get("status", "succeeded"))
                b = self._binding_for(handle, nid)
                prev = dict(b)
                if status == "succeeded":
                    b["status"] = "succeeded_up_to_date"
                    # Container/alias nodes (e.g. component parent nodes) may not emit
                    # node_output directly, but a succeeded finish still means their
                    # current state is up-to-date for this run.
                    b["isUpToDate"] = True
                    b["cacheValid"] = bool(b.get("currentArtifactId")) and bool(b.get("currentExecKey"))
                    b["staleReason"] = None
                    handle.node_status[nid] = "succeeded_up_to_date"
                elif status == "cancelled":
                    b["status"] = "cancelled"
                    b["isUpToDate"] = False
                    handle.node_status[nid] = "cancelled"
                else:
                    b["status"] = "failed"
                    b["isUpToDate"] = False
                    handle.node_status[nid] = "failed"
                self._log_binding_update(handle=handle, node_id=nid, event_type=t, prev=prev, nxt=b)
                self._log_stale_regression(handle=handle, node_id=nid, prev=prev, nxt=b, ev=ev)
            return

        if t == "node_cancelled":
            nid = ev.get("nodeId")
            if nid:
                b = self._binding_for(handle, nid)
                prev = dict(b)
                b["status"] = "cancelled"
                b["isUpToDate"] = False
                handle.node_status[nid] = "cancelled"
                self._log_binding_update(handle=handle, node_id=nid, event_type=t, prev=prev, nxt=b)
                self._log_stale_regression(handle=handle, node_id=nid, prev=prev, nxt=b, ev=ev)
            return

        if t == "cache_decision":
            nid = ev.get("nodeId")
            if nid:
                b = self._binding_for(handle, nid)
                prev = dict(b)
                exec_key = ev.get("execKey")
                decision = ev.get("decision")
                if isinstance(exec_key, str) and exec_key:
                    b["currentExecKey"] = exec_key
                if decision == "cache_hit":
                    b["cacheValid"] = True
                    b["isUpToDate"] = True
                    aid = ev.get("artifactId")
                    if isinstance(aid, str) and aid:
                        b["currentArtifactId"] = aid
                        b["currentRunId"] = handle.run_id
                elif decision == "cache_hit_contract_mismatch":
                    self.invalidate_node(
                        handle,
                        str(nid),
                        reason="CONTRACT_MISMATCH",
                        graph=handle.graph,
                    )
                    b = self._binding_for(handle, nid)
                elif decision == "cache_miss":
                    b["cacheValid"] = False
                    # cache_miss means compute required; do not force staleness.
                self._log_binding_update(handle=handle, node_id=nid, event_type=t, prev=prev, nxt=b)
                self._log_stale_regression(handle=handle, node_id=nid, prev=prev, nxt=b, ev=ev)
            return

        # artifacts
        if t == "node_output":
            nid = ev.get("nodeId")
            aid = ev.get("artifactId")
            if nid and aid:
                b = self._binding_for(handle, nid)
                prev = dict(b)
                b["currentArtifactId"] = aid
                b["currentRunId"] = handle.run_id
                b["lastArtifactId"] = aid
                b["lastRunId"] = handle.run_id
                if b.get("currentExecKey"):
                    b["lastExecKey"] = b.get("currentExecKey")
                handle.node_outputs[nid] = aid
                # Option B registry (artifact → runId)
                self._artifact_owner[aid] = handle.run_id
                self._log_binding_update(handle=handle, node_id=nid, event_type=t, prev=prev, nxt=b)
                self._log_stale_regression(handle=handle, node_id=nid, prev=prev, nxt=b, ev=ev)
            return

        # optional: edge exec
        if t == "edge_exec":
            # you can store per-edge exec if you want (optional)
            return

        if t == "node_blocked":
            nid = ev.get("nodeId")
            if nid:
                handle.node_status[nid] = "blocked"
            return

        if t == "node_paused":
            nid = ev.get("nodeId")
            if nid:
                handle.node_status[nid] = "paused"
            return

        if t == "node_resumed":
            nid = ev.get("nodeId")
            if nid:
                handle.node_status[nid] = "active"
            return


