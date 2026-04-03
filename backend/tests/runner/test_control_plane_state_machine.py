from __future__ import annotations

from app.runner.control_plane import can_node_terminalize, reduce_edge_control_state


def test_edge_control_state_depth_never_negative_and_drain_zeroes_depth() -> None:
	state = reduce_edge_control_state(
		None,
		edge_id="e1",
		signal_type="ITEM_ENQUEUED",
		seq=1,
		at="2026-04-02T00:00:00Z",
	)
	assert int(state.get("depth") or 0) == 1
	state = reduce_edge_control_state(
		state,
		edge_id="e1",
		signal_type="INPUT_DRAINED",
		seq=2,
		at="2026-04-02T00:00:01Z",
	)
	assert int(state.get("depth") or 0) == 0


def test_upstream_closed_is_sticky() -> None:
	state = reduce_edge_control_state(
		None,
		edge_id="e1",
		signal_type="UPSTREAM_CLOSED",
		seq=3,
		at="2026-04-02T00:00:00Z",
	)
	assert bool(state.get("closed")) is True
	state = reduce_edge_control_state(
		state,
		edge_id="e1",
		signal_type="INPUT_READY",
		seq=4,
		at="2026-04-02T00:00:01Z",
	)
	assert bool(state.get("closed")) is True


def test_edge_control_state_ignores_stale_sequence_updates() -> None:
	state = reduce_edge_control_state(
		None,
		edge_id="e1",
		signal_type="ITEM_ENQUEUED",
		seq=10,
		at="2026-04-02T00:00:00Z",
	)
	assert int(state.get("depth") or 0) == 1
	state2 = reduce_edge_control_state(
		state,
		edge_id="e1",
		signal_type="INPUT_DRAINED",
		seq=9,
		at="2026-04-02T00:00:01Z",
	)
	assert int(state2.get("depth") or 0) == 1
	assert int(state2.get("lastSeq") or 0) == 10


def test_node_terminality_requires_closed_and_drained_inputs_and_no_lease() -> None:
	edge_state = {
		"e1": {
			"edgeId": "e1",
			"open": False,
			"closed": True,
			"depth": 0,
			"blocked": False,
			"lastSeq": 4,
		}
	}
	assert can_node_terminalize(
		required_work_edge_ids=["e1"],
		edge_control_state=edge_state,
		inflight_count=0,
		has_active_lease=False,
	)
	assert not can_node_terminalize(
		required_work_edge_ids=["e1"],
		edge_control_state={**edge_state, "e1": {**edge_state["e1"], "depth": 1}},
		inflight_count=0,
		has_active_lease=False,
	)
	assert not can_node_terminalize(
		required_work_edge_ids=["e1"],
		edge_control_state=edge_state,
		inflight_count=1,
		has_active_lease=False,
	)
	assert not can_node_terminalize(
		required_work_edge_ids=["e1"],
		edge_control_state=edge_state,
		inflight_count=0,
		has_active_lease=True,
	)
