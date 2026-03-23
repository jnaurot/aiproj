from __future__ import annotations

import importlib
import sys
import types
from types import SimpleNamespace

import pytest

from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


def _ensure_duckdb_stub() -> None:
	if "duckdb" not in sys.modules:
		sys.modules["duckdb"] = types.SimpleNamespace()


@pytest.mark.asyncio
async def test_run_uses_snapshot_so_rewire_changes_apply_next_run(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		if str((node.get("id") or "")) == "b" and not upstream_artifact_ids:
			raise RuntimeError("missing upstream for downstream tool")
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

	graph = {
		"nodes": [
			{"id": "a", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [{"id": "e1", "source": "a", "target": "b", "sourceHandle": "out", "targetHandle": "in"}],
	}
	original_edge_count = len(graph["edges"])

	def _fake_expand_graph_components(graph_arg, component_store=None, max_depth=5):
		# Mutate original graph mid-run; snapshot should remain stable.
		graph["edges"] = []
		assert len(graph_arg.get("edges", [])) == original_edge_count
		return SimpleNamespace(
			graph=graph_arg,
			internal_to_parent={},
			parent_component_meta={},
			parent_to_internal={},
		)

	monkeypatch.setattr(run_mod, "expand_graph_components", _fake_expand_graph_components)
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-rewire-next-run",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-rewire-next-run", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-rewire-next-run",
	)

	assert len(graph["edges"]) == 0
	assert any(evt.get("type") == "run_finished" and evt.get("status") == "succeeded" for evt in events)
