from __future__ import annotations

import asyncio

from app.runner.queues import QueueLimits, QueueRegistry


async def test_queue_metrics_include_per_port_entries() -> None:
	registry = QueueRegistry(limits=QueueLimits(per_edge_max=10, global_max=100))
	await registry.enqueue("e1", "in", {"id": 1})
	await registry.enqueue("e1", "param_profile", {"id": 2})
	metrics = registry.metrics()
	edges = metrics.get("edges", {})
	assert "e1:in" in edges
	assert "e1:param_profile" in edges
	assert edges["e1:in"]["inputHandle"] == "in"
	assert edges["e1:param_profile"]["inputHandle"] == "param_profile"


async def test_queue_metrics_report_blocked_when_waiter_is_backpressured() -> None:
	registry = QueueRegistry(limits=QueueLimits(per_edge_max=1, global_max=10))
	await registry.enqueue("e2", "in", {"id": 1})
	blocked_task = asyncio.create_task(registry.enqueue("e2", "in", {"id": 2}, overflow="block"))
	await asyncio.sleep(0.02)
	metrics_while_blocked = registry.metrics()
	assert metrics_while_blocked["edges"]["e2:in"]["blocked"] is True
	await registry.dequeue("e2", "in")
	await asyncio.wait_for(blocked_task, timeout=1.0)
