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


def _upstream_until_boundaries(
    start_id: str,
    edges: List[Dict[str, Any]],
    boundary_ids: Set[str],
) -> Set[str]:
    """
    Walk upstream and include ancestors, but stop traversal beyond any boundary node.
    Boundary nodes are included in the returned set.
    """
    rev: Dict[str, List[str]] = {}
    for e in edges:
        rev.setdefault(e["target"], []).append(e["source"])
    seen: Set[str] = set()
    q = [start_id]
    while q:
        cur = q.pop(0)
        for prev in rev.get(cur, []):
            if prev in seen:
                continue
            seen.add(prev)
            if prev in boundary_ids:
                # Include boundary ancestor but do not walk beyond it.
                continue
            q.append(prev)
    return seen

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
    pinned_node_ids: Optional[Set[str]] = None,
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
    requested_pins = {
        nid
        for nid in (pinned_node_ids or set())
        if isinstance(nid, str) and nid in adj
    }
    if run_from:
        run_from_is_pinned = run_from in requested_pins
        ancestors = _upstream_until_boundaries(run_from, edges, requested_pins)
        if mode == "selected_only":
            # Pinned selected node is treated as a checkpoint; ancestors are not revalidated.
            sub = {run_from} if run_from_is_pinned else (ancestors | {run_from})
            execute_nodes = {run_from}
            cache_only_nodes = sub - execute_nodes
        else:
            # Include ancestors to resolve deterministic inputs, and downstream
            # to preserve "run from here forward" semantics.
            # If run_from is pinned, treat it as a trusted checkpoint and skip ancestors.
            base = {run_from} if run_from_is_pinned else (ancestors | {run_from})
            sub = base | _downstream(run_from, edges)
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
    # DEPRECATED: legacy pin boundary logic
    pinned = {
        nid
        for nid in requested_pins
        if isinstance(nid, str) and nid in sub
    }
    if pinned:
        execute_nodes -= pinned
        cache_only_nodes |= pinned

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

    return RunPlan(
        order=order,
        subgraph=sub,
        execute_nodes=execute_nodes,
        cache_only_nodes=cache_only_nodes,
        incoming_edges=incoming,
        run_mode=mode,
    )
