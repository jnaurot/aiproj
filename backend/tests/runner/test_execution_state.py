from pathlib import Path

from app.runner.execution_state import (
	NODE_STATES,
	NODE_TRANSITIONS,
	RUN_STATES,
	RUN_TRANSITIONS,
	can_transition_node,
	can_transition_run,
)


def test_run_transition_matrix_allows_declared_and_rejects_illegal() -> None:
	for source, targets in RUN_TRANSITIONS.items():
		for target in targets:
			decision = can_transition_run(source, target)
			assert decision.ok is True
			assert decision.reason == "ok"

	illegal = can_transition_run("running", "pending")
	assert illegal.ok is False
	assert illegal.reason == "illegal_transition"

	unknown_source = can_transition_run("unknown", "running")
	assert unknown_source.ok is False
	assert unknown_source.reason == "unknown_source_state"

	unknown_target = can_transition_run("running", "unknown")
	assert unknown_target.ok is False
	assert unknown_target.reason == "unknown_target_state"


def test_node_transition_matrix_allows_declared_and_rejects_illegal() -> None:
	for source, targets in NODE_TRANSITIONS.items():
		for target in targets:
			decision = can_transition_node(source, target)
			assert decision.ok is True
			assert decision.reason == "ok"

	illegal = can_transition_node("idle", "failed")
	assert illegal.ok is False
	assert illegal.reason == "illegal_transition"

	unknown_source = can_transition_node("unknown", "running")
	assert unknown_source.ok is False
	assert unknown_source.reason == "unknown_source_state"

	unknown_target = can_transition_node("idle", "unknown")
	assert unknown_target.ok is False
	assert unknown_target.reason == "unknown_target_state"


def test_execution_state_machine_doc_mentions_all_canonical_states() -> None:
	doc_path = Path(__file__).resolve().parents[3] / "docs" / "execution_state_machine.md"
	text = doc_path.read_text(encoding="utf-8")
	for state in sorted(RUN_STATES):
		assert f"`{state}`" in text
	for state in sorted(NODE_STATES):
		assert f"`{state}`" in text
