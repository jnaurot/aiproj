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


def _chain_graph():
	return {
		"nodes": [{"id": "a"}, {"id": "b"}, {"id": "c"}, {"id": "d"}],
		"edges": [
			{"id": "e1", "source": "a", "target": "b"},
			{"id": "e2", "source": "b", "target": "c"},
			{"id": "e3", "source": "c", "target": "d"},
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


def test_compile_plan_pinned_nodes_become_cache_only_in_full_run():
	plan = compile_plan(
		_graph(),
		run_from=None,
		run_mode=None,
		dirty_node_ids=None,
		pinned_node_ids={"a"},
	)
	assert set(plan.subgraph) == {"a", "b", "c", "d"}
	assert "a" in plan.cache_only_nodes
	assert "a" not in plan.execute_nodes
	assert "b" in plan.execute_nodes


def test_compile_plan_ignores_pins_not_in_subgraph_for_selected_only():
	plan = compile_plan(
		_graph(),
		run_from="b",
		run_mode="selected_only",
		dirty_node_ids=None,
		pinned_node_ids={"c"},
	)
	assert set(plan.subgraph) == {"a", "b"}
	assert "c" not in plan.cache_only_nodes
	assert "b" in plan.execute_nodes


def test_compile_plan_pinned_run_from_skips_ancestor_validation_from_selected_onward():
	plan = compile_plan(
		_graph(),
		run_from="b",
		run_mode="from_selected_onward",
		dirty_node_ids=None,
		pinned_node_ids={"b"},
	)
	assert set(plan.subgraph) == {"b"}
	assert "a" not in plan.subgraph
	assert "b" in plan.cache_only_nodes
	assert "b" not in plan.execute_nodes


def test_compile_plan_pinned_run_from_skips_ancestor_validation_selected_only():
	plan = compile_plan(
		_graph(),
		run_from="b",
		run_mode="selected_only",
		dirty_node_ids=None,
		pinned_node_ids={"b"},
	)
	assert set(plan.subgraph) == {"b"}
	assert "a" not in plan.subgraph
	assert "b" in plan.cache_only_nodes
	assert "b" not in plan.execute_nodes


def test_compile_plan_stops_upstream_walk_at_pinned_ancestor_from_selected_onward():
	plan = compile_plan(
		_chain_graph(),
		run_from="d",
		run_mode="from_selected_onward",
		dirty_node_ids=None,
		pinned_node_ids={"b"},
	)
	assert set(plan.subgraph) == {"b", "c", "d"}
	assert "a" not in plan.subgraph
	assert "b" in plan.cache_only_nodes
	assert "d" in plan.execute_nodes


def test_compile_plan_stops_upstream_walk_at_pinned_ancestor_selected_only():
	plan = compile_plan(
		_chain_graph(),
		run_from="d",
		run_mode="selected_only",
		dirty_node_ids=None,
		pinned_node_ids={"b"},
	)
	assert set(plan.subgraph) == {"b", "c", "d"}
	assert "a" not in plan.subgraph
	assert "b" in plan.cache_only_nodes
	assert "d" in plan.execute_nodes
