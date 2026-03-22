from __future__ import annotations

import pytest

from app.runner.queues import QueueFullError, QueueLimits, QueueRegistry


@pytest.mark.asyncio
async def test_global_queue_cap_enforced() -> None:
	registry = QueueRegistry(limits=QueueLimits(per_edge_max=10, global_max=2))
	assert await registry.enqueue("e1", "in", {"i": 1}) is True
	assert await registry.enqueue("e2", "in", {"i": 2}) is True
	with pytest.raises(QueueFullError):
		await registry.enqueue("e3", "in", {"i": 3}, overflow="error")
