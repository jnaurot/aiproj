from __future__ import annotations

import pytest

from app.runner.queues import QueueRegistry


@pytest.mark.asyncio
async def test_per_handle_named_input_queues_are_isolated() -> None:
	registry = QueueRegistry()
	await registry.enqueue("e1", "in_left", {"id": 1})
	await registry.enqueue("e1", "in_right", {"id": 2})

	left = await registry.dequeue("e1", "in_left")
	right = await registry.dequeue("e1", "in_right")
	assert left == {"id": 1}
	assert right == {"id": 2}

	m = registry.metrics()
	assert "e1:in_left" in m["edges"]
	assert "e1:in_right" in m["edges"]
