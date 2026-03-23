from __future__ import annotations

from app.runner.run import _build_fair_dequeue_plan


def test_fair_dequeue_plan_interleaves_single_item_with_batch_handles() -> None:
	plan = _build_fair_dequeue_plan([("in_a", 3), ("in_b", 1)])
	assert plan == ["in_a", "in_b", "in_a", "in_a"]


def test_fair_dequeue_plan_round_robins_multiple_handles() -> None:
	plan = _build_fair_dequeue_plan([("h1", 2), ("h2", 2), ("h3", 1)])
	assert plan == ["h1", "h2", "h3", "h1", "h2"]

