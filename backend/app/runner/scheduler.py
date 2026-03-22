from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Set, Tuple


@dataclass(frozen=True)
class QueueSchedulePlan:
	ready: List[str]
	adj: Dict[str, List[str]]
	indeg: Dict[str, int]
	order_index: Dict[str, int]


def build_queue_schedule(
	*,
	nodes: Iterable[str],
	edges: Iterable[Tuple[str, str]],
	order: List[str],
) -> QueueSchedulePlan:
	sub: Set[str] = {str(n) for n in nodes}
	adj: Dict[str, List[str]] = {nid: [] for nid in sub}
	indeg: Dict[str, int] = {nid: 0 for nid in sub}
	for src, dst in edges:
		s = str(src)
		d = str(dst)
		if s in sub and d in sub:
			adj[s].append(d)
			indeg[d] += 1
	order_index = {str(nid): idx for idx, nid in enumerate(order)}
	for nid in adj:
		adj[nid].sort(key=lambda v: order_index.get(v, 10**9))
	ready = sorted([nid for nid, d in indeg.items() if d == 0], key=lambda v: order_index.get(v, 10**9))
	return QueueSchedulePlan(
		ready=ready,
		adj=adj,
		indeg=indeg,
		order_index=order_index,
	)
