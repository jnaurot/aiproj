from __future__ import annotations

import tracemalloc
from time import monotonic

from app.runner.control_plane import reduce_edge_control_state


def _apply_basic_lifecycle(edge_state_by_id: dict[str, dict], edge_id: str, seq_start: int) -> int:
	seq = int(seq_start)
	for signal in ("upstream_opened", "item_enqueued", "input_ready", "input_drained", "upstream_closed"):
		seq += 1
		edge_state_by_id[edge_id] = reduce_edge_control_state(
			edge_state_by_id.get(edge_id),
			edge_id=edge_id,
			signal_type=signal,
			seq=seq,
			at="2026-01-01T00:00:00Z",
		)
	return seq


def test_control_plane_signal_reducer_throughput_budget() -> None:
	"""
	CP-15 throughput guard:
	Keep reduce-edge control updates comfortably fast under high event volume.
	"""
	edge_state_by_id: dict[str, dict] = {}
	edge_count = 250
	events_per_edge = 200
	total_events = edge_count * events_per_edge
	seq = 0
	t0 = monotonic()
	for i in range(total_events):
		edge_id = f"e_{i % edge_count}"
		signal = "item_enqueued" if (i % 4) else "input_drained"
		seq += 1
		edge_state_by_id[edge_id] = reduce_edge_control_state(
			edge_state_by_id.get(edge_id),
			edge_id=edge_id,
			signal_type=signal,
			seq=seq,
			at="2026-01-01T00:00:00Z",
		)
	elapsed = monotonic() - t0
	assert elapsed < 4.0
	assert len(edge_state_by_id) == edge_count


def test_control_plane_reducer_latency_budget_for_run_finalize_burst() -> None:
	"""
	CP-15 reducer latency guard:
	Simulate finalize burst (close + drain) and ensure update latency remains bounded.
	"""
	edge_state_by_id: dict[str, dict] = {}
	seq = 0
	edge_count = 1500
	for i in range(edge_count):
		edge_id = f"edge_{i}"
		seq = _apply_basic_lifecycle(edge_state_by_id, edge_id, seq)
	t0 = monotonic()
	for i in range(edge_count):
		edge_id = f"edge_{i}"
		seq += 1
		edge_state_by_id[edge_id] = reduce_edge_control_state(
			edge_state_by_id.get(edge_id),
			edge_id=edge_id,
			signal_type="upstream_closed",
			seq=seq,
			at="2026-01-01T00:00:01Z",
		)
	elapsed = monotonic() - t0
	assert elapsed < 1.5
	assert all(bool((edge_state_by_id.get(f"edge_{i}") or {}).get("closed")) for i in range(edge_count))


def test_control_plane_memory_growth_bounded_across_100_runs() -> None:
	"""
	CP-15 memory guard:
	Repeated runs should not accumulate unbounded reducer state.
	"""
	runs = 100
	edge_count = 120
	seq = 0
	tracemalloc.start()
	before_current, _before_peak = tracemalloc.get_traced_memory()
	for run_idx in range(runs):
		edge_state_by_id: dict[str, dict] = {}
		for i in range(edge_count):
			edge_id = f"r{run_idx}_e{i}"
			seq = _apply_basic_lifecycle(edge_state_by_id, edge_id, seq)
		assert len(edge_state_by_id) == edge_count
	after_current, _after_peak = tracemalloc.get_traced_memory()
	tracemalloc.stop()
	# Allow small allocator drift while ensuring no runaway retained memory.
	assert (after_current - before_current) < 8_000_000
