from __future__ import annotations

import asyncio

import pytest

from app.runner.queues import QueueLimits, QueueRegistry


@pytest.mark.asyncio
async def test_queue_load_blocking_is_lossless_under_pressure() -> None:
	registry = QueueRegistry(limits=QueueLimits(per_edge_max=8, global_max=128))
	total_items = 200
	produced: list[int] = []
	consumed: list[int] = []

	async def producer() -> None:
		for i in range(total_items):
			ok = await registry.enqueue("e_load", "in", i, overflow="block", timeout_sec=2.0)
			assert ok is True
			produced.append(i)

	async def consumer() -> None:
		while len(consumed) < total_items:
			item = await registry.dequeue("e_load", "in", timeout_sec=2.0)
			if item is None:
				continue
			consumed.append(int(item))

	await asyncio.gather(producer(), consumer())
	assert len(produced) == total_items
	assert len(consumed) == total_items
	assert consumed == list(range(total_items))


@pytest.mark.asyncio
async def test_queue_chaos_cancellation_does_not_corrupt_metrics() -> None:
	registry = QueueRegistry(limits=QueueLimits(per_edge_max=2, global_max=4))
	await registry.enqueue("e1", "in", 1)
	await registry.enqueue("e1", "in", 2)

	blocked_task = asyncio.create_task(registry.enqueue("e1", "in", 3, overflow="block", timeout_sec=5.0))
	await asyncio.sleep(0.05)
	assert not blocked_task.done()
	blocked_task.cancel()
	with pytest.raises(asyncio.CancelledError):
		await blocked_task

	# Queue should still be consistent and drainable after cancellation.
	assert await registry.dequeue("e1", "in", timeout_sec=1.0) == 1
	assert await registry.dequeue("e1", "in", timeout_sec=1.0) == 2
	metrics = registry.metrics()
	assert int(metrics["globalDepth"]) == 0
