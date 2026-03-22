from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from time import monotonic
from typing import Any, Deque, Dict, Optional, Tuple


@dataclass(frozen=True)
class QueueLimits:
	per_edge_max: int = 1000
	global_max: int = 50000


class QueueFullError(RuntimeError):
	pass


class InMemoryEdgeQueue:
	def __init__(self, *, max_size: int):
		self.max_size = max(1, int(max_size))
		self._items: Deque[tuple[float, Any]] = deque()
		self._cond = asyncio.Condition()
		self._enqueued = 0
		self._dequeued = 0
		self._blocked_waiters = 0

	@property
	def depth(self) -> int:
		return len(self._items)

	@property
	def enqueued(self) -> int:
		return self._enqueued

	@property
	def dequeued(self) -> int:
		return self._dequeued

	@property
	def full(self) -> bool:
		return len(self._items) >= self.max_size

	@property
	def blocked(self) -> bool:
		return self._blocked_waiters > 0

	def oldest_age_sec(self) -> Optional[float]:
		if not self._items:
			return None
		return max(0.0, monotonic() - float(self._items[0][0]))

	async def enqueue(
		self,
		item: Any,
		*,
		block: bool = True,
		spill: bool = False,
		timeout_sec: Optional[float] = None,
	) -> bool:
		deadline = None if timeout_sec is None else (monotonic() + max(0.0, float(timeout_sec)))
		async with self._cond:
			while self.full:
				if spill:
					return False
				if not block:
					raise QueueFullError("edge queue is full")
				if deadline is not None and monotonic() >= deadline:
					raise QueueFullError("edge queue enqueue timed out while blocked")
				self._blocked_waiters += 1
				try:
					if deadline is None:
						await self._cond.wait()
					else:
						await asyncio.wait_for(self._cond.wait(), timeout=max(0.001, deadline - monotonic()))
				finally:
					self._blocked_waiters = max(0, self._blocked_waiters - 1)
			self._items.append((monotonic(), item))
			self._enqueued += 1
			self._cond.notify_all()
			return True

	async def dequeue(self, *, timeout_sec: Optional[float] = None) -> Any | None:
		deadline = None if timeout_sec is None else (monotonic() + max(0.0, float(timeout_sec)))
		async with self._cond:
			while not self._items:
				if deadline is not None and monotonic() >= deadline:
					return None
				if deadline is None:
					await self._cond.wait()
				else:
					try:
						await asyncio.wait_for(self._cond.wait(), timeout=max(0.001, deadline - monotonic()))
					except asyncio.TimeoutError:
						return None
			_ts, value = self._items.popleft()
			self._dequeued += 1
			self._cond.notify_all()
			return value


class QueueRegistry:
	def __init__(self, *, limits: QueueLimits | None = None):
		self._limits = limits or QueueLimits()
		self._queues: Dict[Tuple[str, str], InMemoryEdgeQueue] = {}
		self._global_cond = asyncio.Condition()
		self._global_depth = 0

	def _key(self, edge_id: str, input_handle: str) -> Tuple[str, str]:
		return (str(edge_id or "").strip(), str(input_handle or "in").strip() or "in")

	def get_queue(self, edge_id: str, input_handle: str = "in") -> InMemoryEdgeQueue:
		key = self._key(edge_id, input_handle)
		queue = self._queues.get(key)
		if queue is None:
			queue = InMemoryEdgeQueue(max_size=self._limits.per_edge_max)
			self._queues[key] = queue
		return queue

	async def enqueue(
		self,
		edge_id: str,
		input_handle: str,
		item: Any,
		*,
		overflow: str = "block",
		timeout_sec: Optional[float] = None,
	) -> bool:
		mode = str(overflow or "block").strip().lower()
		spill = mode == "spill"
		block = mode != "error"
		q = self.get_queue(edge_id, input_handle)
		deadline = None if timeout_sec is None else (monotonic() + max(0.0, float(timeout_sec)))
		async with self._global_cond:
			while self._global_depth >= self._limits.global_max:
				if spill:
					return False
				if not block:
					raise QueueFullError("global queue cap reached")
				if deadline is not None and monotonic() >= deadline:
					raise QueueFullError("global queue cap wait timed out")
				if deadline is None:
					await self._global_cond.wait()
				else:
					await asyncio.wait_for(
						self._global_cond.wait(),
						timeout=max(0.001, deadline - monotonic()),
					)
			ok = await q.enqueue(
				item,
				block=block,
				spill=spill,
				timeout_sec=(None if deadline is None else max(0.0, deadline - monotonic())),
			)
			if ok:
				self._global_depth += 1
				self._global_cond.notify_all()
			return ok

	async def dequeue(self, edge_id: str, input_handle: str, *, timeout_sec: Optional[float] = None) -> Any | None:
		q = self.get_queue(edge_id, input_handle)
		value = await q.dequeue(timeout_sec=timeout_sec)
		if value is not None:
			async with self._global_cond:
				self._global_depth = max(0, self._global_depth - 1)
				self._global_cond.notify_all()
		return value

	def metrics(self) -> Dict[str, Any]:
		edge_metrics: Dict[str, Any] = {}
		for (edge_id, input_handle), q in self._queues.items():
			key = f"{edge_id}:{input_handle}"
			edge_metrics[key] = {
				"edgeId": edge_id,
				"inputHandle": input_handle,
				"depth": q.depth,
				"enqueued": q.enqueued,
				"dequeued": q.dequeued,
				"oldestAgeSec": q.oldest_age_sec(),
				"blocked": q.blocked,
				"full": q.full,
			}
		return {
			"globalDepth": self._global_depth,
			"globalMax": self._limits.global_max,
			"perEdgeMax": self._limits.per_edge_max,
			"edges": edge_metrics,
		}
