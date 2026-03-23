from __future__ import annotations

from app.runner.run import _build_fair_dequeue_plan


def test_fair_dequeue_plan_is_deterministic_for_same_input() -> None:
	requests = [("in_primary", 4), ("in_side", 2), ("in_aux", 1)]
	first = _build_fair_dequeue_plan(requests)
	for _ in range(25):
		assert _build_fair_dequeue_plan(requests) == first

