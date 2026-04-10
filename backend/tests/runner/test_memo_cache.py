import time

from app.runner.memo_cache import InProcessMemoCache, MemoCacheEntry


def test_put_and_get_returns_entry():
	cache = InProcessMemoCache()
	entry = MemoCacheEntry("k1", "art1", "exec1", "graph1", "node1", time.time())
	cache.put(entry)
	result = cache.get("k1")
	assert result is not None
	assert result.artifact_id == "art1"


def test_get_returns_none_for_missing_key():
	cache = InProcessMemoCache()
	assert cache.get("missing") is None


def test_expired_entry_returns_none():
	cache = InProcessMemoCache(ttl=0.001)
	cache.put(MemoCacheEntry("k1", "art1", "exec1", "graph1", "node1", time.time()))
	time.sleep(0.01)
	assert cache.get("k1") is None


def test_lru_eviction_removes_oldest():
	cache = InProcessMemoCache(max_entries=2)
	cache.put(MemoCacheEntry("k1", "a1", "e1", "g", "n", time.time()))
	cache.put(MemoCacheEntry("k2", "a2", "e2", "g", "n", time.time()))
	cache.put(MemoCacheEntry("k3", "a3", "e3", "g", "n", time.time()))
	assert cache.get("k1") is None
	assert cache.get("k2") is not None
	assert cache.get("k3") is not None


def test_invalidate_removes_graph_entries():
	cache = InProcessMemoCache()
	cache.put(MemoCacheEntry("k1", "a1", "e1", "graph-a", "n1", time.time()))
	cache.put(MemoCacheEntry("k2", "a2", "e2", "graph-b", "n2", time.time()))
	removed = cache.invalidate("graph-a")
	assert removed == 1
	assert cache.get("k1") is None
	assert cache.get("k2") is not None

