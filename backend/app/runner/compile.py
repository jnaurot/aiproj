from dataclasses import dataclass
import logging
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

@dataclass
class RunPlan:
    order: List[str]                 # topo order of nodeIds
    subgraph: Set[str]               # nodes included
    execute_nodes: Set[str]          # nodes that may execute (cache misses allowed)
    cache_only_nodes: Set[str]       # nodes that must resolve from cache only
    incoming_edges: Dict[str, List[str]]  # nodeId -> edgeIds
    run_mode: str                    # full | from_selected_onward | selected_only

def _downstream(start_id: str, edges: List[Dict[str, Any]]) -> Set[str]:
    adj: Dict[str, List[str]] = {}
    for e in edges:
        adj.setdefault(e["source"], []).append(e["target"])
    seen: Set[str] = set()
    q = [start_id]
    while q:
        cur = q.pop(0)
        for nxt in adj.get(cur, []):
            if nxt not in seen:
                seen.add(nxt)
                q.append(nxt)
    return seen


def _upstream(start_id: str, edges: List[Dict[str, Any]]) -> Set[str]:
    rev: Dict[str, List[str]] = {}
    for e in edges:
        rev.setdefault(e["target"], []).append(e["source"])
    seen: Set[str] = set()
    q = [start_id]
    while q:
        cur = q.pop(0)
        for prev in rev.get(cur, []):
            if prev not in seen:
                seen.add(prev)
                q.append(prev)
    return seen


def _required_upstream_with_checkpoint_cuts(
    target_ids: Set[str],
    edges: List[Dict[str, Any]],
    checkpoint_node_ids: Set[str],
) -> Set[str]:
    rev: Dict[str, List[str]] = {}
    for e in edges:
        rev.setdefault(e["target"], []).append(e["source"])
    out: Set[str] = set()
    q = list(target_ids)
    while q:
        cur = q.pop(0)
        if cur in out:
            continue
        out.add(cur)
        # Checkpoint nodes are hard upstream cuts for planning.
        if cur in checkpoint_node_ids:
            continue
        for prev in rev.get(cur, []):
            if prev not in out:
                q.append(prev)
    return out


def _expand_dirty_subgraph(dirty_ids: Set[str], edges: List[Dict[str, Any]]) -> Set[str]:
    if not dirty_ids:
        return set()
    out: Set[str] = set()
    for nid in dirty_ids:
        out.add(nid)
        out |= _upstream(nid, edges)
        out |= _downstream(nid, edges)
    return out


def compile_plan(
    graph: Dict[str, Any],
    run_from: Optional[str],
    run_mode: Optional[str] = None,
    dirty_node_ids: Optional[Set[str]] = None,
    checkpoint_node_ids: Optional[Set[str]] = None,
) -> RunPlan:
    logger.debug("compile_plan_start")
    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])
    node_ids = [n["id"] for n in nodes]
    mode = str(run_mode or "from_selected_onward").strip().lower()
    if mode not in {"full", "from_selected_onward", "selected_only"}:
        mode = "from_selected_onward"

    # Build adjacency + indegree
    adj = {nid: [] for nid in node_ids}
    indeg = {nid: 0 for nid in node_ids}

    for e in edges:
        s, t = e["source"], e["target"]
        if s not in adj or t not in indeg:
            continue
        adj[s].append(t)
        indeg[t] += 1

    # Determine subgraph set
    sub: Set[str] = set()
    execute_nodes: Set[str] = set()
    cache_only_nodes: Set[str] = set()
    pinned = {nid for nid in (checkpoint_node_ids or set()) if isinstance(nid, str) and nid in adj}
    if run_from:
        targets: Set[str] = {run_from}
        if mode == "selected_only":
            # selected_only still resolves upstream dependencies, but planning
            # obeys checkpoint hard cuts.
            sub = _required_upstream_with_checkpoint_cuts(targets, edges, pinned)
            execute_nodes = {run_from}
        else:
            # Preserve selected-onward semantics by targeting selected + downstream,
            # then resolve required upstream dependencies with checkpoint hard cuts.
            targets |= _downstream(run_from, edges)
            sub = _required_upstream_with_checkpoint_cuts(targets, edges, pinned)
            execute_nodes = set(sub)
    else:
        mode = "full"
        requested_dirty = {
            nid for nid in (dirty_node_ids or set()) if isinstance(nid, str) and nid in adj
        }
        if requested_dirty:
            sub = _expand_dirty_subgraph(requested_dirty, edges)
        else:
            roots = [nid for nid, d in indeg.items() if d == 0]
            for r in roots:
                sub.add(r)
                sub |= _downstream(r, edges)
        execute_nodes = set(sub)
    # Recompute indegree restricted to subgraph
    indeg2 = {nid: 0 for nid in sub}
    for e in edges:
        s, t = e["source"], e["target"]
        if s in sub and t in sub:
            indeg2[t] += 1

    # Kahn topo
    q = [nid for nid, d in indeg2.items() if d == 0]
    order: List[str] = []
    while q:
        cur = q.pop(0)
        order.append(cur)
        for nxt in adj.get(cur, []):
            if nxt not in sub:
                continue
            indeg2[nxt] -= 1
            if indeg2[nxt] == 0:
                q.append(nxt)

    if len(order) != len(sub):
        # cycle or disconnected weirdness
        raise ValueError("Graph is not a DAG (cycle detected)")

    incoming: Dict[str, List[str]] = {nid: [] for nid in sub}
    for e in edges:
        if e["target"] in incoming and e.get("id"):
            incoming[e["target"]].append(e["id"])

    # Nodes with a live checkpoint hint are frozen: they must resolve from the
    # saved artifact and must not re-execute.  Move them out of execute_nodes
    # into cache_only_nodes so the execution loop can honour the pin.
    pinned = {nid for nid in pinned if nid in sub}
    execute_nodes -= pinned
    cache_only_nodes |= pinned

    return RunPlan(
        order=order,
        subgraph=sub,
        execute_nodes=execute_nodes,
        cache_only_nodes=cache_only_nodes,
        incoming_edges=incoming,
        run_mode=mode,
    )
