from __future__ import annotations

import asyncio

import pytest

from app.runner.queues import QueueFullError, QueueLimits, QueueRegistry


@pytest.mark.asyncio
async def test_global_queue_cap_errors_when_overflow_mode_is_error() -> None:
	registry = QueueRegistry(limits=QueueLimits(per_edge_max=10, global_max=1))
	ok = await registry.enqueue("e1", "in", {"item": 1}, overflow="block")
	assert ok is True
	with pytest.raises(QueueFullError):
		await registry.enqueue("e2", "in", {"item": 2}, overflow="error", timeout_sec=0.01)


@pytest.mark.asyncio
async def test_global_queue_cap_spills_when_overflow_mode_is_spill() -> None:
	registry = QueueRegistry(limits=QueueLimits(per_edge_max=10, global_max=1))
	ok = await registry.enqueue("e1", "in", {"item": 1}, overflow="block")
	assert ok is True
	second = await registry.enqueue("e2", "in", {"item": 2}, overflow="spill", timeout_sec=0.01)
	assert second is False


@pytest.mark.asyncio
async def test_global_queue_cap_blocks_then_resumes_after_dequeue() -> None:
	registry = QueueRegistry(limits=QueueLimits(per_edge_max=10, global_max=1))
	ok = await registry.enqueue("e1", "in", {"item": 1}, overflow="block")
	assert ok is True

	async def _dequeue_later() -> None:
		await asyncio.sleep(0.05)
		value = await registry.dequeue("e1", "in", timeout_sec=0.1)
		assert value is not None

	drain_task = asyncio.create_task(_dequeue_later())
	second = await registry.enqueue("e2", "in", {"item": 2}, overflow="block", timeout_sec=0.5)
	await drain_task
	assert second is True

