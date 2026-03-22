from __future__ import annotations

import asyncio

import pytest

from app.runner.queues import QueueFullError, QueueLimits, QueueRegistry


@pytest.mark.asyncio
async def test_backpressure_blocks_by_default_until_consumer_dequeues() -> None:
	registry = QueueRegistry(limits=QueueLimits(per_edge_max=1, global_max=10))
	await registry.enqueue("e1", "in", {"id": 1})

	task = asyncio.create_task(registry.enqueue("e1", "in", {"id": 2}, overflow="block", timeout_sec=1.0))
	await asyncio.sleep(0.05)
	# still blocked until dequeue
	assert not task.done()
	first = await registry.dequeue("e1", "in")
	assert first == {"id": 1}
	assert await task is True


@pytest.mark.asyncio
async def test_spill_mode_drops_when_queue_full() -> None:
	registry = QueueRegistry(limits=QueueLimits(per_edge_max=1, global_max=10))
	assert await registry.enqueue("e1", "in", {"id": 1}) is True
	assert await registry.enqueue("e1", "in", {"id": 2}, overflow="spill") is False


@pytest.mark.asyncio
async def test_error_mode_raises_when_queue_full() -> None:
	registry = QueueRegistry(limits=QueueLimits(per_edge_max=1, global_max=10))
	await registry.enqueue("e1", "in", {"id": 1})
	with pytest.raises(QueueFullError):
		await registry.enqueue("e1", "in", {"id": 2}, overflow="error")
