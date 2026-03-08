from __future__ import annotations

from app.runner.compile import compile_plan


def _graph():
	return {
		"nodes": [{"id": "a"}, {"id": "b"}, {"id": "c"}, {"id": "d"}],
		"edges": [
			{"id": "e1", "source": "a", "target": "b"},
			{"id": "e2", "source": "c", "target": "d"},
		],
	}


def test_compile_plan_dirty_nodes_restricts_full_run_scope():
	plan = compile_plan(_graph(), run_from=None, run_mode=None, dirty_node_ids={"b"})
	assert set(plan.subgraph) == {"a", "b"}
	assert "c" not in plan.subgraph
	assert "d" not in plan.subgraph


def test_compile_plan_dirty_with_root_includes_downstream():
	plan = compile_plan(_graph(), run_from=None, run_mode=None, dirty_node_ids={"a"})
	assert set(plan.subgraph) == {"a", "b"}
