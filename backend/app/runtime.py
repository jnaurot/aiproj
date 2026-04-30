import asyncio, time
import json
import traceback
import logging
import hashlib
import re
from uuid import uuid4
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime, timezone

from .runner.events import EventStore, MemoryEventStore, RunEventBus, SqliteEventStore
from .runner.artifacts import ArtifactStore, DiskArtifactStore, MemoryArtifactStore
from .runner.cache import ExecutionCache, SqliteExecutionCache
from .runner.run import run_graph, _build_frontier_identity_basis
from .runner.pause_resume import (
    validate_pause_snapshot_schema,
)
from .runner.execution_contract import (
    validate_execution_contract,
    compare_execution_contracts,
    EXECUTION_CONTRACT_VERSION,
)
from .runner.execution_state import can_transition_node, can_transition_run
from .runner.invariants import evaluate_runtime_invariants
from .runner.state_machine_migration import (
    canonicalize_event_payload,
    canonicalize_run_status,
    summarize_migration_report,
    validate_migrated_event_payload,
)
from .feature_flags import get_feature_flags
from .services.runtime_env import get_env

logger = logging.getLogger(__name__)


_EXPERIMENT_SENSITIVE_KEYS = {
    "authorization",
    "api_key",
    "apikey",
    "token",
    "password",
    "secret",
    "access_token",
    "refresh_token",
    "credentials",
    "connection_string",
}


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
    pause_requested_at: Optional[float] = None
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    pause_event: asyncio.Event = field(default_factory=asyncio.Event)

    node_status: Dict[str, str] = field(default_factory=dict)   # idle|active|done|error|skipped|blocked|paused
    node_outputs: Dict[str, str] = field(default_factory=dict)  # node_id -> artifact_id
    node_bindings: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # node_id -> ui binding state
    checkpoint_outcomes: Dict[str, str] = field(default_factory=dict)
    active_run_planned: set[str] = field(default_factory=set)
    graph: Optional[Dict[str, Any]] = None
    run_telemetry: Dict[str, Any] = field(default_factory=dict)
    pause_snapshot: Dict[str, Any] = field(default_factory=dict)
    execution_contract: Dict[str, Any] = field(default_factory=dict)
    invariant_violations_seen: set[str] = field(default_factory=set)
    invariant_violations_count: int = 0
    
class RuntimeManager:
    def __init__(self):
        print("RuntimeManager init from:", __file__)

        self.runs: Dict[str, RunHandle] = {}
        self._artifact_owner: dict[str, str] = {}
        self.global_cache_mode: str = "default_on"
        self.artifact_store, self.cache, self.event_store = self._build_storage()
        self.rollout_metrics: Dict[str, Any] = {
            "schemaFailures": 0,
            "coercionApplied": 0,
            "componentBindingFailures": 0,
            "lastUpdatedAt": None,
        }

    def _bump_rollout_metric(self, key: str, inc: int = 1) -> None:
        cur = int(self.rollout_metrics.get(key, 0) or 0)
        self.rollout_metrics[key] = max(0, cur + int(inc))
        self.rollout_metrics["lastUpdatedAt"] = datetime_from_ts(time.time())

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
        store_kind = str(get_env("ARTIFACT_STORE", "disk") or "disk").strip().lower()
        if store_kind == "memory":
            return MemoryArtifactStore(), ExecutionCache(), MemoryEventStore()

        artifact_dir = Path(str(get_env("ARTIFACT_DIR", "./data/artifacts") or "./data/artifacts")).resolve()
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

    def _emit_state_transition_violation(
        self,
        *,
        handle: RunHandle,
        entity: str,
        entity_id: str | None,
        source: str,
        target: str,
        reason: str,
        code: str,
    ) -> None:
        payload = {
            "type": "state_transition_violation",
            "runId": str(handle.run_id),
            "entity": str(entity),
            "entityId": str(entity_id or ""),
            "source": str(source),
            "target": str(target),
            "reason": str(reason),
            "code": str(code),
            "message": (
                f"STATE_TRANSITION_VIOLATION entity={entity} entity_id={entity_id or ''} "
                f"source={source} target={target} reason={reason} code={code}"
            ),
            "at": datetime_from_ts(time.time()),
        }
        logger.error(payload["message"])
        try:
            asyncio.create_task(handle.bus.emit(payload))
        except Exception:
            logger.exception("failed_to_emit_state_transition_violation")

    def _run_invariants(self, handle: RunHandle, *, trigger: str) -> None:
        violations = evaluate_runtime_invariants(
            run_status=handle.status,
            node_status=handle.node_status,
            run_telemetry=handle.run_telemetry,
        )
        if not violations:
            return
        strict = str(get_env("RUNTIME_INVARIANTS_STRICT", "") or "").strip().lower() in {"1", "true", "yes", "on"}
        for violation in violations:
            key = f"{violation.code}|{','.join(violation.node_ids)}|{handle.status}"
            if key in handle.invariant_violations_seen:
                continue
            handle.invariant_violations_seen.add(key)
            handle.invariant_violations_count = int(handle.invariant_violations_count or 0) + 1
            payload = {
                "type": "state_invariant_violation",
                "runId": str(handle.run_id),
                "code": str(violation.code),
                "message": str(violation.message),
                "severity": str(violation.severity),
                "nodeIds": list(violation.node_ids),
                "trigger": str(trigger),
                "at": datetime_from_ts(time.time()),
            }
            # RUN_TERMINAL_HAS_ACTIVE_NODES is a transient/expected condition when
            # non-cancellable async tasks (e.g. Ollama HTTP calls) outlive a fatal
            # run termination by a brief window.  Log at warning, not error, and
            # never trigger strict-mode failures for this code.
            is_transient = violation.code == "RUN_TERMINAL_HAS_ACTIVE_NODES"
            if is_transient:
                logger.warning(
                    "STATE_INVARIANT_VIOLATION run_id=%s code=%s trigger=%s nodes=%s",
                    handle.run_id,
                    violation.code,
                    trigger,
                    ",".join(violation.node_ids),
                )
            else:
                logger.error(
                    "STATE_INVARIANT_VIOLATION run_id=%s code=%s trigger=%s nodes=%s",
                    handle.run_id,
                    violation.code,
                    trigger,
                    ",".join(violation.node_ids),
                )
            try:
                asyncio.create_task(handle.bus.emit(payload))
            except Exception:
                logger.exception("failed_to_emit_state_invariant_violation")
            if strict and not is_transient:
                raise RuntimeError(
                    f"STATE_INVARIANT_VIOLATION run_id={handle.run_id} code={violation.code} trigger={trigger}"
                )

    def _set_run_status(self, handle: RunHandle, target: str, *, reason: str) -> bool:
        source = str(handle.status or "").strip().lower() or "pending"
        decision = can_transition_run(source, target)
        if not decision.ok:
            self._emit_state_transition_violation(
                handle=handle,
                entity="run",
                entity_id=handle.run_id,
                source=source,
                target=target,
                reason=reason,
                code=decision.reason,
            )
            return False
        handle.status = decision.target
        try:
            asyncio.create_task(
                handle.bus.emit(
                    {
                        "type": "state_transition",
                        "runId": str(handle.run_id),
                        "entity": "run",
                        "entityId": str(handle.run_id),
                        "source": source,
                        "target": decision.target,
                        "reason": str(reason),
                        "at": datetime_from_ts(time.time()),
                    }
                )
            )
        except Exception:
            logger.exception("failed_to_emit_state_transition")
        self._run_invariants(handle, trigger=f"run_status:{reason}")
        return True

    def _set_node_status(self, handle: RunHandle, node_id: str, target: str, *, reason: str) -> bool:
        source = str(handle.node_status.get(node_id) or "idle").strip().lower() or "idle"
        decision = can_transition_node(source, target)
        if not decision.ok:
            self._emit_state_transition_violation(
                handle=handle,
                entity="node",
                entity_id=node_id,
                source=source,
                target=target,
                reason=reason,
                code=decision.reason,
            )
            return False
        handle.node_status[node_id] = decision.target
        try:
            asyncio.create_task(
                handle.bus.emit(
                    {
                        "type": "state_transition",
                        "runId": str(handle.run_id),
                        "entity": "node",
                        "entityId": str(node_id),
                        "source": source,
                        "target": decision.target,
                        "reason": str(reason),
                        "at": datetime_from_ts(time.time()),
                    }
                )
            )
        except Exception:
            logger.exception("failed_to_emit_state_transition")
        self._run_invariants(handle, trigger=f"node_status:{reason}")
        return True

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
        strict = str(get_env("RUNTIME_STRICT_STALE_TRANSITIONS", "") or "").strip().lower()
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
        strict = str(get_env("RUNTIME_STRICT_INVALIDATION_ASSERTS", "") or "").strip().lower()
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
            self._set_node_status(handle, nid, "stale", reason=f"invalidate_node:{reason}")
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
        rows = await self.event_store.list_events(run_id, after_id=after_id, limit=limit)
        for row in rows:
            payload = row.get("payload")
            if not isinstance(payload, dict):
                continue
            migrated, changed, _notes = canonicalize_event_payload(payload)
            if not changed:
                continue
            valid, _reason = validate_migrated_event_payload(migrated)
            if not valid:
                continue
            row["payload"] = migrated
            row["type"] = str(migrated.get("type") or row.get("type") or "unknown")
        return rows

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

    async def migrate_legacy_state_machine_data(
        self,
        *,
        run_id: Optional[str] = None,
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        report: Dict[str, Any] = {
            "scope": {"runId": run_id},
            "dryRun": bool(dry_run),
            "runs": {"scanned": 0, "fixed": 0, "skipped": 0, "details": []},
            "events": {"scanned": 0, "fixed": 0, "skipped": 0, "details": []},
            "snapshots": {"scanned": 0, "invalid": 0, "details": []},
        }

        run_rows = await self.artifact_store.list_runs(include_deleted=True)
        for rec in run_rows:
            rid = str(rec.get("run_id") or "")
            if not rid:
                continue
            if run_id and rid != run_id:
                continue
            report["runs"]["scanned"] = int(report["runs"]["scanned"]) + 1
            current_status = str(rec.get("status") or "")
            outcome = canonicalize_run_status(current_status)
            if outcome.reason != "ok":
                report["runs"]["skipped"] = int(report["runs"]["skipped"]) + 1
                report["runs"]["details"].append(
                    {"runId": rid, "status": current_status, "action": "skipped", "reason": outcome.reason}
                )
                continue
            if outcome.changed:
                if not dry_run:
                    await self.artifact_store.update_run_status(rid, outcome.value)
                report["runs"]["fixed"] = int(report["runs"]["fixed"]) + 1
                report["runs"]["details"].append(
                    {
                        "runId": rid,
                        "status": current_status,
                        "newStatus": outcome.value,
                        "action": "fixed",
                    }
                )

            snap = await self.artifact_store.get_run_pause_snapshot(rid)
            if isinstance(snap, dict):
                report["snapshots"]["scanned"] = int(report["snapshots"]["scanned"]) + 1
                schema_ok, schema_errors = validate_pause_snapshot_schema(snap)
                if not schema_ok:
                    report["snapshots"]["invalid"] = int(report["snapshots"]["invalid"]) + 1
                    report["snapshots"]["details"].append(
                        {"runId": rid, "action": "invalid_schema", "errors": list(schema_errors or [])}
                    )

            rows = await self._list_all_run_events(rid)
            for row in rows:
                report["events"]["scanned"] = int(report["events"]["scanned"]) + 1
                event_id = int(row.get("id") or 0)
                payload = row.get("payload")
                if not isinstance(payload, dict):
                    report["events"]["skipped"] = int(report["events"]["skipped"]) + 1
                    report["events"]["details"].append(
                        {"runId": rid, "eventId": event_id, "action": "skipped", "reason": "invalid_payload_type"}
                    )
                    continue
                migrated, changed, notes = canonicalize_event_payload(payload)
                if not changed:
                    continue
                valid, reason = validate_migrated_event_payload(migrated)
                if not valid:
                    report["events"]["skipped"] = int(report["events"]["skipped"]) + 1
                    report["events"]["details"].append(
                        {
                            "runId": rid,
                            "eventId": event_id,
                            "action": "skipped",
                            "reason": reason,
                            "notes": notes,
                        }
                    )
                    continue
                changed_row = True
                if not dry_run:
                    changed_row = await self.event_store.update_event(rid, event_id, migrated)
                if changed_row:
                    report["events"]["fixed"] = int(report["events"]["fixed"]) + 1
                    report["events"]["details"].append(
                        {"runId": rid, "eventId": event_id, "action": "fixed", "notes": notes}
                    )
                else:
                    report["events"]["skipped"] = int(report["events"]["skipped"]) + 1
                    report["events"]["details"].append(
                        {
                            "runId": rid,
                            "eventId": event_id,
                            "action": "skipped",
                            "reason": "event_row_not_found",
                            "notes": notes,
                        }
                    )

        report["summary"] = summarize_migration_report(report)
        return report

    async def recover_unfinished_runs(self) -> Dict[str, Any]:
        terminal = {"succeeded", "failed", "canceled", "deleted", "paused"}
        unfinished = {"pending", "running", "cancel_requested", "pausing", "resuming"}
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
                recovered_status = "canceled"
                reason = "RECOVERED_CANCEL_REQUESTED_ON_STARTUP"
                await self.event_store.append_event(
                    {
                        "type": "run_canceled",
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

    def get_diagnostics(self) -> Dict[str, Any]:
        memo_stats_fn = getattr(self.artifact_store, "get_memo_stats", None)
        artifact_memo = memo_stats_fn() if callable(memo_stats_fn) else {}
        runs: Dict[str, Any] = {}
        for run_id, handle in self.runs.items():
            if handle.status == "deleted":
                continue
            runs[run_id] = {
                "status": handle.status,
                "graphId": handle.graph_id,
                "runTelemetry": dict(handle.run_telemetry or {}),
                "plannedNodeCount": int(len(handle.active_run_planned or set())),
            }
        return {
            "schemaVersion": 1,
            "artifactMemo": artifact_memo,
            "globalCacheMode": self.get_global_cache_mode()
            if hasattr(self, "get_global_cache_mode")
            else ("default_on" if bool(getattr(self, "global_cache_enabled", True)) else "force_off"),
            "activeRuns": runs,
            "sourceJsonItemExtraction": {
                "pathSyntax": ["jobs", "$.jobs", "$.jobs[]", "jobs[0].id", '$["jobs"][0]'],
                "strictErrors": [
                    "JSON_ITEM_PATH_NOT_FOUND",
                    "JSON_ITEM_PATH_TYPE_MISMATCH",
                    "JSON_ITEM_PATH_INVALID",
                ],
            },
            "featureFlags": {
                "STRICT_SCHEMA_EDGE_CHECKS": bool(get_feature_flags().get("STRICT_SCHEMA_EDGE_CHECKS", True)),
                "STRICT_SCHEMA_EDGE_CHECKS_V2": bool(get_feature_flags().get("STRICT_SCHEMA_EDGE_CHECKS_V2", True)),
                "STRICT_COERCION_POLICY": bool(get_feature_flags().get("STRICT_COERCION_POLICY", True)),
            },
            "rolloutMetrics": dict(self.rollout_metrics),
        }

    async def request_cancel(self, run_id: str) -> Dict[str, Any]:
        handle = self.runs.get(run_id)
        if not handle:
            return {"runId": run_id, "found": False, "cancelRequested": False, "status": "unknown"}

        terminal = {"succeeded", "failed", "canceled", "deleted"}
        if handle.status in terminal:
            return {"runId": run_id, "found": True, "cancelRequested": False, "status": handle.status}

        if handle.cancel_event.is_set() or handle.status == "cancel_requested":
            return {"runId": run_id, "found": True, "cancelRequested": True, "status": "cancel_requested"}

        handle.cancel_requested_at = time.time()
        self._set_run_status(handle, "cancel_requested", reason="request_cancel")
        handle.cancel_event.set()
        await handle.bus.emit(
            {
                "type": "run_cancel_requested",
                "runId": run_id,
                "at": datetime_from_ts(handle.cancel_requested_at),
            }
        )
        return {"runId": run_id, "found": True, "cancelRequested": True, "status": "cancel_requested"}

    async def request_cancel_many(self, *, graph_id: Optional[str] = None, hard: bool = False) -> Dict[str, Any]:
        target_graph_id = str(graph_id or "").strip()
        terminal = {"succeeded", "failed", "canceled", "deleted"}
        matched: list[str] = []
        requested: list[str] = []
        hard_canceled: list[str] = []
        already_terminal: list[str] = []
        already_requested: list[str] = []
        for run_id, handle in self.runs.items():
            if not handle:
                continue
            if target_graph_id and str(handle.graph_id or "").strip() != target_graph_id:
                continue
            matched.append(run_id)
            if handle.status in terminal:
                already_terminal.append(run_id)
                continue
            if handle.cancel_event.is_set() or handle.status == "cancel_requested":
                already_requested.append(run_id)
            else:
                handle.cancel_requested_at = time.time()
                self._set_run_status(handle, "cancel_requested", reason="request_cancel_many")
                handle.cancel_event.set()
                await handle.bus.emit(
                    {
                        "type": "run_cancel_requested",
                        "runId": run_id,
                        "at": datetime_from_ts(handle.cancel_requested_at),
                    }
                )
                requested.append(run_id)
            if hard:
                task = handle.task
                if task is not None and not task.done():
                    task.cancel()
                    hard_canceled.append(run_id)
        return {
            "graphId": target_graph_id or None,
            "hard": bool(hard),
            "matchedRunIds": matched,
            "cancelRequestedRunIds": requested,
            "alreadyRequestedRunIds": already_requested,
            "alreadyTerminalRunIds": already_terminal,
            "hardCancelledRunIds": hard_canceled,
        }

    async def request_pause(self, run_id: str) -> Dict[str, Any]:
        handle = self.runs.get(run_id)
        if not handle:
            return {"runId": run_id, "found": False, "pauseRequested": False, "status": "unknown"}

        terminal = {"succeeded", "failed", "canceled", "deleted"}
        if handle.status in terminal:
            return {"runId": run_id, "found": True, "pauseRequested": False, "status": handle.status}
        if handle.status == "paused":
            return {"runId": run_id, "found": True, "pauseRequested": False, "status": "paused"}
        if handle.status in {"pausing"} or handle.pause_event.is_set():
            return {"runId": run_id, "found": True, "pauseRequested": True, "status": "pausing"}

        handle.pause_requested_at = time.time()
        self._set_run_status(handle, "pausing", reason="request_pause")
        handle.pause_event.set()
        await handle.bus.emit(
            {
                "type": "run_pause_requested",
                "runId": run_id,
                "at": datetime_from_ts(handle.pause_requested_at),
            }
        )
        return {"runId": run_id, "found": True, "pauseRequested": True, "status": "pausing"}

    def _node_bindings_from_frontier_basis(self, basis: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        basis_nodes = basis.get("nodes") if isinstance(basis.get("nodes"), dict) else {}
        node_bindings: Dict[str, Dict[str, Any]] = {}

        def _binding_payload_from_pair(pair: Dict[str, Any], *, graph_id: str) -> Dict[str, Any]:
            exec_key = str(pair.get("currentExecKey") or "").strip()
            artifact_id = str(pair.get("currentArtifactId") or "").strip()
            has_artifact = bool(exec_key) and bool(artifact_id)
            return {
                "graphId": graph_id,
                "status": "succeeded_up_to_date" if has_artifact else "idle",
                "lastArtifactId": artifact_id or None,
                "lastRunId": None,
                "lastExecKey": exec_key or None,
                "currentExecKey": exec_key or None,
                "currentArtifactId": artifact_id or None,
                "currentRunId": None,
                "isUpToDate": bool(has_artifact),
                "cacheValid": bool(has_artifact),
                "staleReason": None,
            }

        for node_id, node_basis in basis_nodes.items():
            if not isinstance(node_basis, dict):
                continue
            pair = node_basis.get("binding") if isinstance(node_basis.get("binding"), dict) else {}
            node_key = str(node_id or "").strip()
            if not node_key:
                continue
            graph_id = str(basis.get("graphId") or "").strip()
            node_bindings[node_key] = _binding_payload_from_pair(pair, graph_id=graph_id)
            upstream = (
                node_basis.get("upstreamBindings")
                if isinstance(node_basis.get("upstreamBindings"), dict)
                else {}
            )
            for upstream_node_id, upstream_pair in upstream.items():
                if not isinstance(upstream_pair, dict):
                    continue
                upstream_key = str(upstream_node_id or "").strip()
                if not upstream_key:
                    continue
                node_bindings.setdefault(
                    upstream_key,
                    _binding_payload_from_pair(
                        {
                            "currentExecKey": str(upstream_pair.get("currentExecKey") or "").strip(),
                            "currentArtifactId": str(upstream_pair.get("currentArtifactId") or "").strip(),
                        },
                        graph_id=str(basis.get("graphId") or "").strip(),
                    ),
                )
        return node_bindings

    def _build_current_resume_identity_basis(
        self,
        *,
        handle: RunHandle,
        snapshot: Dict[str, Any],
    ) -> Dict[str, Any]:
        basis = (
            snapshot.get("frontierValidationBasis")
            if isinstance(snapshot.get("frontierValidationBasis"), dict)
            else {}
        )
        expected_nodes = basis.get("nodes") if isinstance(basis.get("nodes"), dict) else {}
        graph = handle.graph if isinstance(handle.graph, dict) else {}
        execution_version = str(
            basis.get("executionVersion")
            or snapshot.get("executionVersion")
            or "v1"
        )
        node_ids = [str(node_id).strip() for node_id in expected_nodes.keys() if str(node_id).strip()]
        return _build_frontier_identity_basis(
            graph=graph,
            graph_id=str(handle.graph_id or snapshot.get("graphId") or ""),
            node_ids=node_ids,
            node_bindings=handle.node_bindings if isinstance(handle.node_bindings, dict) else {},
            execution_version=execution_version,
        )

    async def request_resume(self, run_id: str) -> Dict[str, Any]:
        handle = self.runs.get(run_id)
        if not handle:
            snapshot_only = await self.artifact_store.get_run_pause_snapshot(run_id)
            persisted = await self.artifact_store.get_run(run_id)
            persisted_status = str((persisted or {}).get("status") or "").strip().lower()
            if persisted_status != "paused" or not isinstance(snapshot_only, dict):
                return {"runId": run_id, "found": False, "resumed": False, "status": "unknown"}
            schema_ok, _schema_errors = validate_pause_snapshot_schema(snapshot_only)
            if not schema_ok:
                return {
                    "runId": run_id,
                    "found": True,
                    "resumed": False,
                    "status": "paused",
                    "errorCode": "PAUSE_SNAPSHOT_SCHEMA_INVALID",
                    "details": {"errors": _schema_errors},
                }
            handle = self.create_run(run_id)
            self._set_run_status(handle, "paused", reason="request_resume:rehydrate_snapshot")
            handle.graph_id = str(snapshot_only.get("graphId") or "")
            snapshot_graph = (
                snapshot_only.get("graph")
                if isinstance(snapshot_only.get("graph"), dict)
                else None
            )
            if isinstance(snapshot_graph, dict) and isinstance(snapshot_graph.get("nodes"), list) and len(snapshot_graph.get("nodes") or []) > 0:
                handle.graph = snapshot_graph
            else:
                experiment_fn = getattr(self.artifact_store, "get_run_experiment", None)
                experiment = await experiment_fn(run_id) if callable(experiment_fn) else None
                experiment_graph = None
                if isinstance(experiment, dict):
                    if isinstance(experiment.get("graph"), dict):
                        experiment_graph = experiment.get("graph")
                    else:
                        params = experiment.get("params") if isinstance(experiment.get("params"), dict) else {}
                        if isinstance(params.get("graph"), dict):
                            experiment_graph = params.get("graph")
                handle.graph = experiment_graph if isinstance(experiment_graph, dict) else {"nodes": [], "edges": []}
            basis = (
                snapshot_only.get("frontierValidationBasis")
                if isinstance(snapshot_only.get("frontierValidationBasis"), dict)
                else {}
            )
            handle.node_bindings = self._node_bindings_from_frontier_basis(basis)
            self.runs[run_id] = handle
        if handle.status != "paused":
            return {"runId": run_id, "found": True, "resumed": False, "status": handle.status}

        snapshot = await self.artifact_store.get_run_pause_snapshot(run_id)
        if not isinstance(snapshot, dict):
            await handle.bus.emit(
                {
                    "type": "run_resume_failed",
                    "runId": run_id,
                    "at": datetime_from_ts(time.time()),
                    "errorCode": "PAUSE_SNAPSHOT_MISSING",
                    "error": "No durable pause snapshot available",
                }
            )
            return {
                "runId": run_id,
                "found": True,
                "resumed": False,
                "status": "paused",
                "errorCode": "PAUSE_SNAPSHOT_MISSING",
            }

        schema_ok, schema_errors = validate_pause_snapshot_schema(snapshot)
        if not schema_ok:
            details = {"errors": schema_errors}
            await handle.bus.emit(
                {
                    "type": "run_resume_failed",
                    "runId": run_id,
                    "at": datetime_from_ts(time.time()),
                    "errorCode": "PAUSE_SNAPSHOT_SCHEMA_INVALID",
                    "error": "Pause snapshot schema invalid",
                    "details": details,
                }
            )
            return {
                "runId": run_id,
                "found": True,
                "resumed": False,
                "status": "paused",
                "errorCode": "PAUSE_SNAPSHOT_SCHEMA_INVALID",
                "details": details,
            }

        resumability_by_node = (
            snapshot.get("resumabilityByNode")
            if isinstance(snapshot.get("resumabilityByNode"), dict)
            else {}
        )
        basis_nodes = (
            (snapshot.get("frontierValidationBasis") or {}).get("nodes")
            if isinstance((snapshot.get("frontierValidationBasis") or {}).get("nodes"), dict)
            else {}
        )
        non_safe_nodes = sorted(
            {
                str(node_id)
                for node_id in basis_nodes.keys()
                if str((resumability_by_node or {}).get(str(node_id), "")).strip()
                not in {"safe_boundary_resumable", ""}
            }
        )
        if non_safe_nodes:
            details = {
                "reasonCodes": ["non_resumable_frontier"],
                "nodeIds": non_safe_nodes,
                "mismatches": [
                    {
                        "nodeId": node_id,
                        "reasonCode": "non_resumable_frontier",
                        "changedFields": ["resumability"],
                    }
                    for node_id in non_safe_nodes
                ],
            }
            await handle.bus.emit(
                {
                    "type": "run_resume_failed",
                    "runId": run_id,
                    "at": datetime_from_ts(time.time()),
                    "errorCode": "RESUME_NON_RESUMABLE_FRONTIER",
                    "error": "Paused frontier includes non-resumable nodes",
                    "details": details,
                }
            )
            return {
                "runId": run_id,
                "found": True,
                "resumed": False,
                "status": "paused",
                "errorCode": "RESUME_NON_RESUMABLE_FRONTIER",
                "details": details,
            }

        current_basis = self._build_current_resume_identity_basis(handle=handle, snapshot=snapshot)
        expected_contract = (
            snapshot.get("executionContract")
            if isinstance(snapshot.get("executionContract"), dict)
            else None
        )
        expected_basis = (
            snapshot.get("frontierValidationBasis")
            if isinstance(snapshot.get("frontierValidationBasis"), dict)
            else {}
        )
        expected_contract_obj = (
            dict(expected_contract)
            if isinstance(expected_contract, dict)
            else {
                "contractVersion": int(EXECUTION_CONTRACT_VERSION),
                "graphId": str(expected_basis.get("graphId") or snapshot.get("graphId") or ""),
                "basis": expected_basis,
            }
        )
        current_contract = {
            "contractVersion": int(expected_contract_obj.get("contractVersion") or EXECUTION_CONTRACT_VERSION),
            "graphId": str(expected_contract_obj.get("graphId") or current_basis.get("graphId") or ""),
            "basis": current_basis,
        }
        contract_diff = compare_execution_contracts(
            expected_contract=expected_contract_obj,
            current_contract=current_contract,
        )
        if not bool(contract_diff.get("ok")):
            details = {
                "reasonCodes": list(contract_diff.get("reasonCodes") or []),
                "nodeIds": list(contract_diff.get("nodeIds") or []),
                "mismatches": list(contract_diff.get("mismatches") or []),
                "contractDiff": contract_diff,
            }
            await handle.bus.emit(
                {
                    "type": "run_resume_failed",
                    "runId": run_id,
                    "at": datetime_from_ts(time.time()),
                    "errorCode": "RESUME_FRONTIER_VALIDATION_FAILED",
                    "error": "Paused frontier validation failed",
                    "details": details,
                }
            )
            return {
                "runId": run_id,
                "found": True,
                "resumed": False,
                "status": "paused",
                "errorCode": "RESUME_FRONTIER_VALIDATION_FAILED",
                "details": details,
            }

        self._set_run_status(handle, "resuming", reason="request_resume")
        await handle.bus.emit(
            {
                "type": "run_resume_requested",
                "runId": run_id,
                "at": datetime_from_ts(time.time()),
            }
        )
        await handle.bus.emit(
            {
                "type": "run_resuming",
                "runId": run_id,
                "at": datetime_from_ts(time.time()),
            }
        )
        snapshot_graph = snapshot.get("graph") if isinstance(snapshot.get("graph"), dict) else None
        handle_graph = handle.graph if isinstance(handle.graph, dict) else None
        handle_pause_graph = (
            handle.pause_snapshot.get("graph")
            if isinstance(getattr(handle, "pause_snapshot", None), dict)
            and isinstance(handle.pause_snapshot.get("graph"), dict)
            else None
        )
        selected_graph_source = ""
        graph: Dict[str, Any] = {}
        if isinstance(handle_graph, dict) and isinstance(handle_graph.get("nodes"), list) and len(handle_graph.get("nodes") or []) > 0:
            graph = handle_graph
            selected_graph_source = "live_handle"
        elif isinstance(snapshot_graph, dict) and isinstance(snapshot_graph.get("nodes"), list) and len(snapshot_graph.get("nodes") or []) > 0:
            graph = snapshot_graph
            selected_graph_source = "pause_snapshot_store"
        elif isinstance(handle_pause_graph, dict) and isinstance(handle_pause_graph.get("nodes"), list) and len(handle_pause_graph.get("nodes") or []) > 0:
            graph = handle_pause_graph
            selected_graph_source = "pause_snapshot_memory"
        if not graph:
            experiment_fn = getattr(self.artifact_store, "get_run_experiment", None)
            experiment = await experiment_fn(run_id) if callable(experiment_fn) else None
            experiment_graph = None
            if isinstance(experiment, dict):
                if isinstance(experiment.get("graph"), dict):
                    experiment_graph = experiment.get("graph")
                else:
                    params = experiment.get("params") if isinstance(experiment.get("params"), dict) else {}
                    if isinstance(params.get("graph"), dict):
                        experiment_graph = params.get("graph")
            if isinstance(experiment_graph, dict):
                graph = experiment_graph
                selected_graph_source = "run_experiment"
            else:
                graph = {}
                selected_graph_source = "missing"
        graph_node_count = (
            len(graph.get("nodes") or [])
            if isinstance(graph, dict) and isinstance(graph.get("nodes"), list)
            else 0
        )
        await handle.bus.emit(
            {
                "type": "log",
                "runId": run_id,
                "at": datetime_from_ts(time.time()),
                "level": "info",
                "message": (
                    "[resume] graph_rehydrate "
                    f"source={selected_graph_source} "
                    f"nodes={graph_node_count}"
                ),
            }
        )
        if graph_node_count <= 0:
            await handle.bus.emit(
                {
                    "type": "run_resume_failed",
                    "runId": run_id,
                    "at": datetime_from_ts(time.time()),
                    "errorCode": "RESUME_GRAPH_REHYDRATION_FAILED",
                    "error": "Unable to rehydrate paused graph for resume",
                    "details": {
                        "source": selected_graph_source,
                        "nodeCount": graph_node_count,
                    },
                }
            )
            self._set_run_status(handle, "paused", reason="request_resume:graph_missing")
            return {
                "runId": run_id,
                "found": True,
                "resumed": False,
                "status": "paused",
                "errorCode": "RESUME_GRAPH_REHYDRATION_FAILED",
                "details": {"source": selected_graph_source, "nodeCount": graph_node_count},
            }
        run_from = snapshot.get("runFrom")
        run_mode = str(snapshot.get("runMode") or "").strip() or None
        await self.start_run(
            run_id,
            graph,
            run_from,
            run_mode=run_mode,
            graph_id=handle.graph_id,
            resume_snapshot=snapshot,
        )
        return {"runId": run_id, "found": True, "resumed": True, "status": "resuming"}

    async def _load_run_execution_contract(
        self,
        *,
        run_id: str,
        handle: Optional[RunHandle],
    ) -> tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
        context: Dict[str, Any] = {}
        if handle and isinstance(handle.graph, dict):
            context["graph"] = handle.graph
        if handle and str(handle.graph_id or "").strip():
            context["graphId"] = str(handle.graph_id or "").strip()
        if handle and isinstance(handle.execution_contract, dict) and handle.execution_contract:
            return dict(handle.execution_contract), context

        snapshot = await self.artifact_store.get_run_pause_snapshot(run_id)
        if isinstance(snapshot, dict):
            if isinstance(snapshot.get("graph"), dict):
                context["graph"] = snapshot.get("graph")
            if str(snapshot.get("graphId") or "").strip():
                context["graphId"] = str(snapshot.get("graphId") or "").strip()
            if "runFrom" in snapshot:
                context["runFrom"] = snapshot.get("runFrom")
            if str(snapshot.get("runMode") or "").strip():
                context["runMode"] = str(snapshot.get("runMode") or "").strip()
            snapshot_contract = snapshot.get("executionContract")
            if isinstance(snapshot_contract, dict) and snapshot_contract:
                return dict(snapshot_contract), context

        experiment_fn = getattr(self.artifact_store, "get_run_experiment", None)
        if callable(experiment_fn):
            experiment = await experiment_fn(run_id)
            if isinstance(experiment, dict):
                if isinstance(experiment.get("graph"), dict):
                    context["graph"] = experiment.get("graph")
                if str(experiment.get("graphId") or "").strip() and "graphId" not in context:
                    context["graphId"] = str(experiment.get("graphId") or "").strip()
                if "runFrom" in experiment and "runFrom" not in context:
                    context["runFrom"] = experiment.get("runFrom")
                if str(experiment.get("runMode") or "").strip() and "runMode" not in context:
                    context["runMode"] = str(experiment.get("runMode") or "").strip()
                experiment_contract = experiment.get("executionContract")
                if isinstance(experiment_contract, dict) and experiment_contract:
                    return dict(experiment_contract), context

        rows = await self._list_all_run_events(run_id)
        for row in reversed(rows):
            payload = row.get("payload")
            if not isinstance(payload, dict):
                continue
            t = str(payload.get("type") or "")
            if t not in {"run_started", "run_resumed"}:
                continue
            evt_contract = payload.get("executionContract")
            if isinstance(evt_contract, dict) and evt_contract:
                if str(payload.get("graphId") or "").strip() and "graphId" not in context:
                    context["graphId"] = str(payload.get("graphId") or "").strip()
                if "runFrom" in payload and "runFrom" not in context:
                    context["runFrom"] = payload.get("runFrom")
                if str(payload.get("runMode") or "").strip() and "runMode" not in context:
                    context["runMode"] = str(payload.get("runMode") or "").strip()
                return dict(evt_contract), context
        return None, context

    def _build_current_contract_for_expected(
        self,
        *,
        graph: Dict[str, Any],
        graph_id: str,
        expected_contract: Dict[str, Any],
        node_bindings: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        expected_basis = expected_contract.get("basis") if isinstance(expected_contract.get("basis"), dict) else {}
        expected_nodes = expected_basis.get("nodes") if isinstance(expected_basis.get("nodes"), dict) else {}
        node_ids = [str(node_id).strip() for node_id in expected_nodes.keys() if str(node_id).strip()]
        execution_version = str(get_env("RUNNER_EXECUTION_VERSION", "v1") or "v1").strip() or "v1"
        basis = _build_frontier_identity_basis(
            graph=graph,
            graph_id=graph_id,
            node_ids=node_ids,
            node_bindings=node_bindings if isinstance(node_bindings, dict) else {},
            execution_version=execution_version,
        )
        return {
            "contractVersion": int(EXECUTION_CONTRACT_VERSION),
            "graphId": str(graph_id or ""),
            "basis": basis,
        }

    def _bindings_from_expected_contract(self, contract: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
        basis = contract.get("basis") if isinstance(contract.get("basis"), dict) else {}
        nodes = basis.get("nodes") if isinstance(basis.get("nodes"), dict) else {}
        out: Dict[str, Dict[str, str]] = {}
        for node_id, raw_node in nodes.items():
            if not isinstance(raw_node, dict):
                continue
            node_key = str(node_id or "").strip()
            if not node_key:
                continue
            binding = raw_node.get("binding") if isinstance(raw_node.get("binding"), dict) else {}
            out[node_key] = {
                "currentExecKey": str(binding.get("currentExecKey") or "").strip(),
                "currentArtifactId": str(binding.get("currentArtifactId") or "").strip(),
            }
            upstream = raw_node.get("upstreamBindings") if isinstance(raw_node.get("upstreamBindings"), dict) else {}
            for upstream_node_id, pair in upstream.items():
                if not isinstance(pair, dict):
                    continue
                upstream_key = str(upstream_node_id or "").strip()
                if not upstream_key:
                    continue
                out.setdefault(
                    upstream_key,
                    {
                        "currentExecKey": str(pair.get("currentExecKey") or "").strip(),
                        "currentArtifactId": str(pair.get("currentArtifactId") or "").strip(),
                    },
                )
        return out

    async def request_replay(
        self,
        *,
        source_run_id: str,
        graph: Optional[Dict[str, Any]] = None,
        run_from: Any = None,
        run_mode: Optional[str] = None,
        graph_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        source_handle = self.runs.get(source_run_id)
        source_contract, source_context = await self._load_run_execution_contract(
            run_id=source_run_id,
            handle=source_handle,
        )
        if not isinstance(source_contract, dict):
            persisted = await self.artifact_store.get_run(source_run_id)
            return {
                "sourceRunId": source_run_id,
                "found": bool(persisted or source_handle),
                "replayed": False,
                "status": "unknown",
                "errorCode": "REPLAY_CONTRACT_MISSING",
            }
        contract_ok, contract_errors = validate_execution_contract(source_contract)
        if not contract_ok:
            return {
                "sourceRunId": source_run_id,
                "found": True,
                "replayed": False,
                "status": "unknown",
                "errorCode": "REPLAY_CONTRACT_INVALID",
                "details": {"errors": contract_errors},
            }

        replay_graph = graph if isinstance(graph, dict) else source_context.get("graph")
        if not isinstance(replay_graph, dict):
            return {
                "sourceRunId": source_run_id,
                "found": True,
                "replayed": False,
                "status": "unknown",
                "errorCode": "REPLAY_GRAPH_MISSING",
            }
        expected_graph_id = str(source_contract.get("graphId") or "").strip()
        resolved_graph_id = (
            str(graph_id or "").strip()
            or str(source_context.get("graphId") or "").strip()
            or expected_graph_id
        )
        if not resolved_graph_id:
            resolved_graph_id = f"graph:replay:{source_run_id}"

        node_bindings: Dict[str, Dict[str, Any]]
        if source_handle and isinstance(source_handle.node_bindings, dict):
            node_bindings = dict(source_handle.node_bindings)
        else:
            node_bindings = self._bindings_from_expected_contract(source_contract)

        current_contract = self._build_current_contract_for_expected(
            graph=replay_graph,
            graph_id=resolved_graph_id,
            expected_contract=source_contract,
            node_bindings=node_bindings,
        )
        validation = compare_execution_contracts(
            expected_contract=source_contract,
            current_contract=current_contract,
        )
        if not bool(validation.get("ok")):
            return {
                "sourceRunId": source_run_id,
                "found": True,
                "replayed": False,
                "status": "unknown",
                "errorCode": "REPLAY_CONTRACT_VALIDATION_FAILED",
                "details": {
                    "reasonCodes": list(validation.get("reasonCodes") or []),
                    "nodeIds": list(validation.get("nodeIds") or []),
                    "mismatches": list(validation.get("mismatches") or []),
                    "contractDiff": validation,
                },
            }

        replay_run_id = str(uuid4())
        replay_handle = self.create_run(replay_run_id)
        replay_handle.execution_contract = dict(current_contract)
        resolved_run_mode = str(run_mode or source_context.get("runMode") or "").strip() or None
        resolved_run_from = run_from if run_from is not None else source_context.get("runFrom")
        await self.start_run(
            replay_run_id,
            replay_graph,
            resolved_run_from,
            run_mode=resolved_run_mode,
            graph_id=resolved_graph_id,
        )
        return {
            "sourceRunId": source_run_id,
            "runId": replay_run_id,
            "found": True,
            "replayed": True,
            "status": "running",
        }

    def _canonical_json(self, value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)

    def _run_package_checksum(self, package_without_integrity: Dict[str, Any]) -> str:
        payload = self._canonical_json(package_without_integrity)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    async def export_run_package(self, run_id: str) -> Dict[str, Any]:
        handle = self.runs.get(run_id)
        rec = await self.artifact_store.get_run(run_id)
        if not handle and not rec:
            raise KeyError(run_id)

        contract, context = await self._load_run_execution_contract(run_id=run_id, handle=handle)
        events_rows = await self._list_all_run_events(run_id)
        events = [row.get("payload") for row in events_rows if isinstance(row.get("payload"), dict)]
        experiment: Dict[str, Any] = {}
        experiment_fn = getattr(self.artifact_store, "get_run_experiment", None)
        if callable(experiment_fn):
            loaded = await experiment_fn(run_id)
            if isinstance(loaded, dict):
                experiment = dict(loaded)

        graph = (
            context.get("graph")
            if isinstance(context.get("graph"), dict)
            else (handle.graph if handle and isinstance(handle.graph, dict) else (experiment.get("graph") if isinstance(experiment.get("graph"), dict) else None))
        )
        graph_id = (
            str(context.get("graphId") or "").strip()
            or (str(handle.graph_id or "").strip() if handle else "")
            or str((rec or {}).get("graph_id") or "").strip()
            or str(experiment.get("graphId") or "").strip()
        )
        status = str((handle.status if handle else (rec or {}).get("status")) or "unknown")
        created_at = (
            datetime_from_ts(handle.created_at)
            if handle
            else str((rec or {}).get("created_at") or datetime_from_ts(time.time()))
        )
        artifact_refs = (
            list(experiment.get("artifacts") or [])
            if isinstance(experiment.get("artifacts"), list)
            else []
        )
        package_core: Dict[str, Any] = {
            "schemaVersion": 1,
            "runId": str(run_id),
            "graphId": graph_id,
            "status": status,
            "createdAt": created_at,
            "runFrom": context.get("runFrom"),
            "runMode": context.get("runMode"),
            "graph": graph if isinstance(graph, dict) else None,
            "executionContract": contract if isinstance(contract, dict) else {},
            "events": events,
            "artifactRefs": artifact_refs,
        }
        checksum = self._run_package_checksum(package_core)
        return {
            **package_core,
            "integrity": {
                "algorithm": "sha256",
                "checksum": checksum,
            },
        }

    async def import_run_package(
        self,
        *,
        package: Dict[str, Any],
        run_id_override: Optional[str] = None,
        overwrite: bool = False,
    ) -> Dict[str, Any]:
        if not isinstance(package, dict):
            return {"imported": False, "errorCode": "RUN_PACKAGE_INVALID", "details": {"reason": "package_not_object"}}
        integrity = package.get("integrity") if isinstance(package.get("integrity"), dict) else {}
        expected_checksum = str(integrity.get("checksum") or "").strip().lower()
        algo = str(integrity.get("algorithm") or "sha256").strip().lower()
        package_core = dict(package)
        package_core.pop("integrity", None)
        actual_checksum = self._run_package_checksum(package_core)
        if algo != "sha256" or not expected_checksum or expected_checksum != actual_checksum:
            return {
                "imported": False,
                "errorCode": "RUN_PACKAGE_INTEGRITY_FAILED",
                "details": {
                    "algorithm": algo,
                    "expectedChecksum": expected_checksum,
                    "actualChecksum": actual_checksum,
                },
            }

        source_run_id = str(package.get("runId") or "").strip()
        if not source_run_id:
            return {"imported": False, "errorCode": "RUN_PACKAGE_INVALID", "details": {"reason": "missing_run_id"}}
        target_run_id = str(run_id_override or source_run_id).strip()
        existing = await self.artifact_store.get_run(target_run_id)
        if existing and not overwrite:
            return {"imported": False, "errorCode": "RUN_PACKAGE_CONFLICT", "details": {"runId": target_run_id}}

        status = str(package.get("status") or "unknown")
        await self.artifact_store.record_run(target_run_id, "pending")
        await self.artifact_store.update_run_status(target_run_id, status)

        events = package.get("events") if isinstance(package.get("events"), list) else []
        for event in events:
            if not isinstance(event, dict):
                continue
            evt = dict(event)
            evt["runId"] = target_run_id
            await self.event_store.append_event(evt)

        experiment_fn = getattr(self.artifact_store, "upsert_run_experiment", None)
        if callable(experiment_fn):
            summary = {
                "runId": target_run_id,
                "graphId": str(package.get("graphId") or ""),
                "createdAt": str(package.get("createdAt") or datetime_from_ts(time.time())),
                "status": status,
                "executionContract": (
                    dict(package.get("executionContract"))
                    if isinstance(package.get("executionContract"), dict)
                    else {}
                ),
                "graph": package.get("graph") if isinstance(package.get("graph"), dict) else None,
                "runFrom": package.get("runFrom"),
                "runMode": package.get("runMode"),
                "artifacts": list(package.get("artifactRefs") or []),
            }
            await experiment_fn(summary)
        snapshot_fn = getattr(self.artifact_store, "upsert_run_pause_snapshot", None)
        if callable(snapshot_fn):
            await snapshot_fn(
                target_run_id,
                {
                    "runId": target_run_id,
                    "graphId": str(package.get("graphId") or ""),
                    "graph": package.get("graph") if isinstance(package.get("graph"), dict) else None,
                    "runFrom": package.get("runFrom"),
                    "runMode": package.get("runMode"),
                    "executionContract": (
                        dict(package.get("executionContract"))
                        if isinstance(package.get("executionContract"), dict)
                        else {}
                    ),
                },
            )

        return {
            "imported": True,
            "runId": target_run_id,
            "status": status,
            "sourceRunId": source_run_id,
        }

    async def diff_run_execution_contracts(
        self,
        *,
        run_id: str,
        against_run_id: str,
    ) -> Dict[str, Any]:
        left_handle = self.runs.get(run_id)
        right_handle = self.runs.get(against_run_id)
        left_contract, _left_context = await self._load_run_execution_contract(run_id=run_id, handle=left_handle)
        right_contract, _right_context = await self._load_run_execution_contract(
            run_id=against_run_id,
            handle=right_handle,
        )
        left_found = bool(left_contract)
        right_found = bool(right_contract)
        if not left_found or not right_found:
            return {
                "found": left_found and right_found,
                "errorCode": "CONTRACT_MISSING",
                "runId": run_id,
                "againstRunId": against_run_id,
                "missing": {
                    "runId": not left_found,
                    "againstRunId": not right_found,
                },
            }
        diff = compare_execution_contracts(
            expected_contract=dict(left_contract or {}),
            current_contract=dict(right_contract or {}),
        )
        return {
            "found": True,
            "runId": run_id,
            "againstRunId": against_run_id,
            "contractDiff": diff,
        }

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

    async def start_run(
        self,
        run_id: str,
        graph,
        run_from,
        run_mode: Optional[str] = None,
        graph_id: Optional[str] = None,
        *,
        resume_snapshot: Optional[Dict[str, Any]] = None,
        adaptive_override: Optional[Dict[str, Any]] = None,
    ):
        handle = self.runs[run_id]
        resolved_graph_id = str(graph_id or "").strip()
        if not resolved_graph_id and isinstance(graph, dict):
            resolved_graph_id = str(graph.get("graphId") or "").strip()
        if not resolved_graph_id:
            resolved_graph_id = str(handle.graph_id or "").strip()
        if not resolved_graph_id:
            # Keep runtime-compatible behavior for tests/direct callers that
            # execute without a top-level graph id.
            resolved_graph_id = f"graph:{run_id}"
        handle.graph_id = resolved_graph_id
        handle.bus.graph_id = handle.graph_id
        handle.graph = graph
        handle.cancel_event = asyncio.Event()
        handle.pause_event = asyncio.Event()
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
                pause_event=handle.pause_event,
                runtime_ref=self,
                graph_id=handle.graph_id,
                resume_snapshot=resume_snapshot,
                adaptive_override=adaptive_override,
            )
        )

    def _sanitize_experiment_value(self, value: Any) -> Any:
        if isinstance(value, dict):
            out: Dict[str, Any] = {}
            for key, raw in value.items():
                k = str(key or "")
                lower = k.lower()
                if any(s in lower for s in _EXPERIMENT_SENSITIVE_KEYS):
                    out[k] = "***REDACTED***"
                else:
                    out[k] = self._sanitize_experiment_value(raw)
            return out
        if isinstance(value, list):
            return [self._sanitize_experiment_value(v) for v in value]
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        try:
            return json.loads(json.dumps(value, ensure_ascii=False))
        except Exception:
            return str(value)

    def _flatten_numeric_metrics(self, value: Any, *, prefix: str = "") -> Dict[str, float]:
        out: Dict[str, float] = {}
        if isinstance(value, dict):
            for key, raw in value.items():
                k = str(key or "").strip()
                if not k:
                    continue
                next_prefix = f"{prefix}.{k}" if prefix else k
                out.update(self._flatten_numeric_metrics(raw, prefix=next_prefix))
            return out
        if isinstance(value, list):
            if value and all(isinstance(v, (int, float)) for v in value):
                key = prefix or "value"
                out[key] = float(sum(float(v) for v in value) / len(value))
            return out
        if isinstance(value, (int, float)):
            key = prefix or "value"
            out[key] = float(value)
        return out

    def _percentile(self, values: list[float], p: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(float(v) for v in values)
        if len(ordered) == 1:
            return float(ordered[0])
        idx = max(0.0, min(float(p), 100.0)) / 100.0 * float(len(ordered) - 1)
        lo = int(idx)
        hi = min(len(ordered) - 1, lo + 1)
        frac = idx - float(lo)
        return float(ordered[lo] * (1.0 - frac) + ordered[hi] * frac)

    async def _capture_run_experiment_summary(self, handle: RunHandle) -> None:
        upsert_fn = getattr(self.artifact_store, "upsert_run_experiment", None)
        if not callable(upsert_fn):
            return
        graph = handle.graph if isinstance(handle.graph, dict) else {}
        nodes = graph.get("nodes", []) if isinstance(graph.get("nodes"), list) else []
        params_by_node: Dict[str, Dict[str, Any]] = {}
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_id = str(node.get("id") or "").strip()
            if not node_id:
                continue
            data = node.get("data") if isinstance(node.get("data"), dict) else {}
            params = data.get("params") if isinstance(data.get("params"), dict) else {}
            params_by_node[node_id] = {
                "kind": str(data.get("kind") or ""),
                "params": self._sanitize_experiment_value(params),
            }

        artifact_refs: list[Dict[str, Any]] = []
        artifact_ids = sorted(
            {
                str(aid).strip()
                for aid in (handle.node_outputs or {}).values()
                if isinstance(aid, str) and str(aid).strip()
            }
        )
        metrics_by_node: Dict[str, Dict[str, Any]] = {}
        metrics_flat: Dict[str, float] = {}
        profile_ids: set[str] = set()
        locks: set[str] = set()
        node_latencies_ms: Dict[str, list[float]] = {}
        queue_depth_trend: list[Dict[str, Any]] = []
        failure_categories: Dict[str, int] = {}

        for artifact_id in artifact_ids:
            try:
                art = await self.artifact_store.get(artifact_id)
            except Exception:
                continue
            payload_schema = art.payload_schema if isinstance(art.payload_schema, dict) else {}
            builtin_env = payload_schema.get("builtin_environment") if isinstance(payload_schema.get("builtin_environment"), dict) else {}
            profile_id = str(builtin_env.get("profileId") or "").strip()
            if profile_id:
                profile_ids.add(profile_id)
            lock_value = str(builtin_env.get("locked") or "").strip()
            if lock_value:
                locks.add(lock_value)
            artifact_refs.append(
                {
                    "artifactId": str(art.artifact_id),
                    "nodeId": str(art.node_id or ""),
                    "nodeKind": str(art.node_kind or ""),
                    "payloadType": str(getattr(art, "payload_type", "") or (art.payload_schema or {}).get("type") or ""),
                    "mimeType": str(art.mime_type or ""),
                }
            )

            node_id = str(art.node_id or "")
            if not node_id:
                continue
            metrics_payload: Dict[str, Any] = {}
            if "json" in str(art.mime_type or "").lower():
                try:
                    raw = await self.artifact_store.read(artifact_id)
                    parsed = json.loads(raw.decode("utf-8", errors="replace"))
                except Exception:
                    parsed = None
                if isinstance(parsed, dict):
                    body = parsed.get("payload") if isinstance(parsed.get("payload"), dict) else parsed
                    if isinstance(body, dict):
                        for metric_key in ("metrics", "metrics_train", "metrics_cv"):
                            if isinstance(body.get(metric_key), dict):
                                metrics_payload[metric_key] = body.get(metric_key)
                        if not metrics_payload:
                            # fallback: capture any numeric structure in payload root
                            fallback = self._flatten_numeric_metrics(body)
                            if fallback:
                                metrics_payload["payload"] = body
            flattened = self._flatten_numeric_metrics(metrics_payload)
            if flattened:
                metrics_by_node[node_id] = {
                    "artifactId": artifact_id,
                    "metrics": flattened,
                }
                for metric_key, metric_value in flattened.items():
                    metrics_flat[f"{node_id}.{metric_key}"] = float(metric_value)

        events = await self._list_all_run_events(str(handle.run_id))
        queue_re = re.compile(r"depth=(\d+)")
        for row in events:
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            evt_type = str(payload.get("type") or "")
            if evt_type == "node_finished":
                node_id = str(payload.get("nodeId") or "").strip()
                if node_id:
                    try:
                        ms = float(payload.get("execution_time_ms") or payload.get("executionTimeMs") or 0.0)
                    except Exception:
                        ms = 0.0
                    node_latencies_ms.setdefault(node_id, []).append(max(0.0, ms))
                if str(payload.get("status") or "").strip().lower() == "failed":
                    code = str(payload.get("errorCode") or "node_failed").strip() or "node_failed"
                    failure_categories[code] = int(failure_categories.get(code, 0)) + 1
            elif evt_type == "run_finished":
                if str(payload.get("status") or "").strip().lower() == "failed":
                    code = str(payload.get("errorCode") or "run_failed").strip() or "run_failed"
                    failure_categories[code] = int(failure_categories.get(code, 0)) + 1
            elif evt_type == "log":
                msg = str(payload.get("message") or "")
                if "[queue]" in msg and "depth=" in msg:
                    match = queue_re.search(msg)
                    if match:
                        try:
                            depth = int(match.group(1))
                        except Exception:
                            depth = 0
                        queue_depth_trend.append(
                            {
                                "at": str(payload.get("at") or ""),
                                "depth": int(max(0, depth)),
                            }
                        )

        node_latency_summary: Dict[str, Dict[str, float]] = {}
        for node_id, values in node_latencies_ms.items():
            if not values:
                continue
            vals = [float(v) for v in values]
            node_latency_summary[str(node_id)] = {
                "count": float(len(vals)),
                "avgMs": float(sum(vals) / len(vals)),
                "p50Ms": float(self._percentile(vals, 50)),
                "p95Ms": float(self._percentile(vals, 95)),
                "maxMs": float(max(vals)),
            }

        summary = {
            "runId": str(handle.run_id),
            "graphId": str(handle.graph_id or ""),
            "createdAt": datetime_from_ts(handle.created_at),
            "status": str(handle.status or "unknown"),
            "executionContract": (
                dict(handle.execution_contract)
                if isinstance(handle.execution_contract, dict)
                else {}
            ),
            "params": {"nodes": params_by_node},
            "metrics": {"byNode": metrics_by_node, "flat": metrics_flat},
            "environment": {
                "builtinProfiles": sorted(profile_ids),
                "locks": sorted(locks),
            },
            "analytics": {
                "runTelemetry": dict(handle.run_telemetry or {}),
                "nodeLatencyMs": node_latency_summary,
                "queueDepthTrend": queue_depth_trend[-500:],
                "failureCategories": dict(failure_categories),
            },
            "artifacts": artifact_refs,
            "artifactIds": artifact_ids,
        }
        await upsert_fn(summary)

    def _apply_event_to_state(self, handle, ev: dict) -> None:
        t = ev.get("type")
        if handle.status == "deleted":
            return

        if t == "log":
            msg = str(ev.get("message") or "")
            upper = msg.upper()
            if "[COERCION_APPLIED]" in upper:
                self._bump_rollout_metric("coercionApplied")
            return

        # run lifecycle
        if t == "run_started":
            changed = self._set_run_status(handle, "running", reason="event:run_started")
            planned = ev.get("plannedNodeIds") or []
            if isinstance(planned, list):
                handle.active_run_planned = {str(x) for x in planned if isinstance(x, str) and x}
            contract = ev.get("executionContract")
            if isinstance(contract, dict):
                handle.execution_contract = dict(contract)
            if changed:
                asyncio.create_task(self.artifact_store.update_run_status(handle.run_id, "running"))
            return

        if t == "run_cancel_requested":
            if self._set_run_status(handle, "cancel_requested", reason="event:run_cancel_requested"):
                asyncio.create_task(self.artifact_store.update_run_status(handle.run_id, "cancel_requested"))
            return

        if t == "run_pause_requested":
            if str(handle.status or "").strip().lower() == "pausing":
                return
            if self._set_run_status(handle, "pausing", reason="event:run_pause_requested"):
                asyncio.create_task(self.artifact_store.update_run_status(handle.run_id, "pausing"))
            return

        if t == "run_pausing":
            if str(handle.status or "").strip().lower() == "pausing":
                return
            if self._set_run_status(handle, "pausing", reason="event:run_pausing"):
                asyncio.create_task(self.artifact_store.update_run_status(handle.run_id, "pausing"))
            return

        if t == "run_paused":
            changed = self._set_run_status(handle, "paused", reason="event:run_paused")
            snapshot = ev.get("snapshot") if isinstance(ev.get("snapshot"), dict) else {}
            handle.pause_snapshot = dict(snapshot or {})
            snapshot_graph = snapshot.get("graph") if isinstance(snapshot.get("graph"), dict) else {}
            if snapshot_graph:
                handle.graph = snapshot_graph
            snapshot_basis = (
                snapshot.get("frontierValidationBasis")
                if isinstance(snapshot.get("frontierValidationBasis"), dict)
                else {}
            )
            snapshot_bindings = self._node_bindings_from_frontier_basis(snapshot_basis)
            if snapshot_bindings:
                merged_bindings: Dict[str, Dict[str, Any]] = {}
                existing_bindings = handle.node_bindings if isinstance(handle.node_bindings, dict) else {}
                for node_id, binding in existing_bindings.items():
                    key = str(node_id or "").strip()
                    if not key:
                        continue
                    merged_bindings[key] = dict(binding) if isinstance(binding, dict) else {}
                for node_id, pair in snapshot_bindings.items():
                    key = str(node_id or "").strip()
                    if not key:
                        continue
                    default_binding = {
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
                    base = (
                        dict(merged_bindings.get(key))
                        if isinstance(merged_bindings.get(key), dict)
                        else dict(default_binding)
                    )
                    base["currentExecKey"] = str(pair.get("currentExecKey") or "").strip() or None
                    base["currentArtifactId"] = str(pair.get("currentArtifactId") or "").strip() or None
                    merged_bindings[key] = base
                handle.node_bindings = merged_bindings
            snapshot_contract = snapshot.get("executionContract") if isinstance(snapshot, dict) else None
            if isinstance(snapshot_contract, dict):
                handle.execution_contract = dict(snapshot_contract)
            if changed:
                asyncio.create_task(self.artifact_store.update_run_status(handle.run_id, "paused"))
                if isinstance(snapshot, dict) and snapshot:
                    asyncio.create_task(self.artifact_store.upsert_run_pause_snapshot(handle.run_id, snapshot))
            return

        if t == "run_resume_requested":
            if str(handle.status or "").strip().lower() == "resuming":
                return
            if self._set_run_status(handle, "resuming", reason="event:run_resume_requested"):
                asyncio.create_task(self.artifact_store.update_run_status(handle.run_id, "resuming"))
            return

        if t == "run_resuming":
            if str(handle.status or "").strip().lower() == "resuming":
                return
            if self._set_run_status(handle, "resuming", reason="event:run_resuming"):
                asyncio.create_task(self.artifact_store.update_run_status(handle.run_id, "resuming"))
            return

        if t == "run_resumed":
            changed = self._set_run_status(handle, "running", reason="event:run_resumed")
            planned = ev.get("plannedNodeIds") or []
            if isinstance(planned, list):
                handle.active_run_planned = {str(x) for x in planned if isinstance(x, str) and x}
            contract = ev.get("executionContract")
            if isinstance(contract, dict):
                handle.execution_contract = dict(contract)
            if changed:
                asyncio.create_task(self.artifact_store.update_run_status(handle.run_id, "running"))
            return

        if t == "run_resume_failed":
            if self._set_run_status(handle, "paused", reason="event:run_resume_failed"):
                asyncio.create_task(self.artifact_store.update_run_status(handle.run_id, "paused"))
            return

        if t == "run_canceled":
            if self._set_run_status(handle, "canceled", reason="event:run_canceled"):
                asyncio.create_task(self.artifact_store.update_run_status(handle.run_id, "canceled"))
            return

        if t == "run_finished":
            next_status = str(ev.get("status", "failed") or "failed").strip().lower()
            contract = ev.get("executionContract")
            if isinstance(contract, dict):
                handle.execution_contract = dict(contract)
            checkpoint_outcomes = ev.get("checkpoint_outcomes")
            if isinstance(checkpoint_outcomes, dict):
                handle.checkpoint_outcomes = {
                    str(node_id): str(status)
                    for node_id, status in checkpoint_outcomes.items()
                    if str(node_id).strip()
                }
            elif isinstance(ev.get("checkpointOutcomes"), dict):
                handle.checkpoint_outcomes = {
                    str(node_id): str(status)
                    for node_id, status in dict(ev.get("checkpointOutcomes") or {}).items()
                    if str(node_id).strip()
                }
            if self._set_run_status(handle, next_status, reason="event:run_finished"):
                handle.active_run_planned = set()
                asyncio.create_task(self.artifact_store.update_run_status(handle.run_id, handle.status))
                asyncio.create_task(self.artifact_store.delete_run_pause_snapshot(handle.run_id))
                asyncio.create_task(self._capture_run_experiment_summary(handle))
                asyncio.create_task(
                    handle.bus.emit(
                        {
                            "type": "invariant_summary",
                            "runId": str(handle.run_id),
                            "status": str(handle.status),
                            "violations": int(handle.invariant_violations_count or 0),
                            "at": datetime_from_ts(time.time()),
                        }
                    )
                )
            return
        
        if t == "run_telemetry":
            handle.run_telemetry = dict(ev)
            self._run_invariants(handle, trigger="run_telemetry")
            return

        # node lifecycle
        if t == "node_started":
            nid = ev.get("nodeId")
            if nid:
                b = self._binding_for(handle, nid)
                prev = dict(b)
                if self._set_node_status(handle, nid, "running", reason="event:node_started"):
                    b["status"] = "running"
                    self._log_binding_update(handle=handle, node_id=nid, event_type=t, prev=prev, nxt=b)
                    self._log_stale_regression(handle=handle, node_id=nid, prev=prev, nxt=b, ev=ev)
            return

        if t == "node_finished":
            nid = ev.get("nodeId")
            if nid:
                status = str(ev.get("status", "succeeded"))
                error_code = str(ev.get("errorCode") or "").strip().upper()
                if status != "succeeded":
                    if error_code in {
                        "PAYLOAD_SCHEMA_MISMATCH",
                        "CONTRACT_EDGE_TYPED_SCHEMA_MISMATCH",
                        "CONTRACT_EDGE_PORT_TYPE_MISMATCH",
                        "CONTRACT_MISMATCH",
                        "MISSING_COLUMN",
                        "DUPLICATE_COLUMN",
                        "COLUMN_SELECTION_REQUIRED",
                        "EXPR_INVALID",
                    }:
                        self._bump_rollout_metric("schemaFailures")
                    if error_code.startswith("COMPONENT_OUTPUT_"):
                        self._bump_rollout_metric("componentBindingFailures")
                b = self._binding_for(handle, nid)
                prev = dict(b)
                if status == "succeeded":
                    if self._set_node_status(handle, nid, "succeeded_up_to_date", reason="event:node_finished"):
                        b["status"] = "succeeded_up_to_date"
                        # Container/alias nodes (e.g. component parent nodes) may not emit
                        # node_output directly, but a succeeded finish still means their
                        # current state is up-to-date for this run.
                        b["isUpToDate"] = True
                        b["cacheValid"] = bool(b.get("currentArtifactId")) and bool(b.get("currentExecKey"))
                        b["staleReason"] = None
                elif status == "canceled":
                    if self._set_node_status(handle, nid, "canceled", reason="event:node_finished"):
                        b["status"] = "canceled"
                        b["isUpToDate"] = False
                else:
                    if self._set_node_status(handle, nid, "failed", reason="event:node_finished"):
                        b["status"] = "failed"
                        b["isUpToDate"] = False
                self._log_binding_update(handle=handle, node_id=nid, event_type=t, prev=prev, nxt=b)
                self._log_stale_regression(handle=handle, node_id=nid, prev=prev, nxt=b, ev=ev)
            return

        if t == "node_canceled":
            nid = ev.get("nodeId")
            if nid:
                b = self._binding_for(handle, nid)
                prev = dict(b)
                if self._set_node_status(handle, nid, "canceled", reason="event:node_canceled"):
                    b["status"] = "canceled"
                    b["isUpToDate"] = False
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
                handle_name = str(ev.get("handle") or "").strip() or "out"
                output_lineage_raw = b.get("outputLineage")
                output_lineage = (
                    dict(output_lineage_raw)
                    if isinstance(output_lineage_raw, dict)
                    else {}
                )
                output_exec_key = str(
                    ev.get("execKey")
                    or b.get("currentExecKey")
                    or b.get("lastExecKey")
                    or ""
                ).strip()
                output_lineage[handle_name] = {
                    "artifactId": str(aid),
                    "execKey": output_exec_key or None,
                }
                b["outputLineage"] = output_lineage
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
                current_status = str(handle.node_status.get(nid) or "idle").strip().lower()
                # node_blocked from the scheduler means "can't queue more work for
                # this node right now" (e.g. MAX_INFLIGHT_REACHED).  When the node
                # is already executing (running / active), this is a queue-level
                # signal only — the in-flight execution WILL complete and emit
                # node_finished.  If we let running → blocked happen here, the state
                # machine is left in "blocked" when node_finished arrives, which
                # causes an illegal blocked → succeeded_up_to_date violation.
                # Only apply the blocked transition when the node is not executing.
                if current_status not in {"running", "active"}:
                    self._set_node_status(handle, nid, "blocked", reason="event:node_blocked")
            return

        if t == "node_paused":
            nid = ev.get("nodeId")
            if nid:
                self._set_node_status(handle, nid, "paused", reason="event:node_paused")
            return

        if t == "node_resumed":
            nid = ev.get("nodeId")
            if nid:
                self._set_node_status(handle, nid, "active", reason="event:node_resumed")
            return



