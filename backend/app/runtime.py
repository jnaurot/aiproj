import asyncio, time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from .runner.events import RunEventBus
from .runner.artifacts import MemoryArtifactStore
from .runner.cache import ExecutionCache
from .runner.run import run_graph


@dataclass
class RunHandle:
    run_id: str
    bus: RunEventBus
    artifact_store: MemoryArtifactStore
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

    # ---------- creation ----------

    def create_run(self, run_id: str) -> RunHandle:
        store = MemoryArtifactStore()
        cache = ExecutionCache()

        handle = RunHandle(
            run_id=run_id,
            bus=None,
            artifact_store=store,
            cache=cache,
        )
        bus = RunEventBus(run_id, on_emit=lambda ev: self._apply_event_to_state(handle, ev))
        handle.bus = bus
        
        self.runs[run_id] = handle
        print("BUS INIT OK:", bus, "has on_emit:", hasattr(bus, "_on_emit"))

        return handle

    def get_run(self, run_id: str) -> Optional[RunHandle]:
        return self.runs.get(run_id)
    
    # ----------------------artifacts------------------------

    async def resolve_artifact_owner(self, artifact_id: str) -> str | None:
        return self._artifact_owner.get(artifact_id)

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

        # run lifecycle
        if t == "run_started":
            handle.status = "running"
            return

        if t == "run_finished":
            handle.status = ev.get("status", "finished")
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


