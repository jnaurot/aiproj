"""
Memoization cache: maps MemoKey -> artifact references.

This is an in-process LRU/TTL cache for development and tests.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

MEMO_CACHE_TTL_SECONDS = 7 * 24 * 3600
MEMO_CACHE_MAX_ENTRIES = 10_000


@dataclass
class MemoCacheEntry:
	memo_key: str
	artifact_id: str
	exec_key: str
	graph_id: str
	node_id: str
	created_at: float


class InProcessMemoCache:
	def __init__(self, max_entries: int = MEMO_CACHE_MAX_ENTRIES, ttl: float = MEMO_CACHE_TTL_SECONDS):
		self._store: dict[str, MemoCacheEntry] = {}
		self._max = max_entries
		self._ttl = ttl

	def get(self, memo_key: str) -> Optional[MemoCacheEntry]:
		entry = self._store.get(memo_key)
		if entry is None:
			return None
		if time.time() - entry.created_at > self._ttl:
			self._store.pop(memo_key, None)
			return None
		# LRU touch
		self._store.pop(memo_key, None)
		self._store[memo_key] = entry
		return entry

	def put(self, entry: MemoCacheEntry) -> None:
		if not entry.memo_key:
			return
		self._store.pop(entry.memo_key, None)
		if len(self._store) >= self._max:
			oldest = next(iter(self._store))
			self._store.pop(oldest, None)
		self._store[entry.memo_key] = entry

	def invalidate(self, graph_id: str) -> int:
		keys = [k for k, v in self._store.items() if v.graph_id == graph_id]
		for key in keys:
			self._store.pop(key, None)
		return len(keys)

	def clear(self) -> None:
		self._store.clear()

	def __len__(self) -> int:
		return len(self._store)


_dev_cache = InProcessMemoCache()


def get_dev_cache() -> InProcessMemoCache:
	return _dev_cache

