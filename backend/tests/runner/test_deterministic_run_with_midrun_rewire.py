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


def _graph() -> dict:
	return {
		"nodes": [
			{"id": "a", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
		],
		"edges": [{"id": "e1", "source": "a", "target": "b", "sourceHandle": "out", "targetHandle": "in"}],
	}


@pytest.mark.asyncio
async def test_run_remains_deterministic_under_midrun_graph_mutation(monkeypatch) -> None:
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

	async def _run_once(run_id: str) -> list[dict]:
		graph = _graph()

		def _fake_expand(graph_arg, component_store=None, max_depth=5):
			# Simulate an operator rewiring graph during this run.
			graph["edges"] = []
			return SimpleNamespace(
				graph=graph_arg,
				internal_to_parent={},
				parent_component_meta={},
				parent_to_internal={},
			)

		monkeypatch.setattr(run_mod, "expand_graph_components", _fake_expand)
		events: list[dict] = []
		await run_mod.run_graph(
			run_id=run_id,
			graph=graph,
			run_from=None,
			bus=RunEventBus(run_id, on_emit=lambda evt: events.append(dict(evt))),
			artifact_store=MemoryArtifactStore(),
			cache=ExecutionCache(),
			graph_id=f"g-{run_id}",
		)
		return events

	events_a = await _run_once("run-deterministic-a")
	events_b = await _run_once("run-deterministic-b")

	def _node_started_order(events: list[dict]) -> list[str]:
		return [str(evt.get("nodeId")) for evt in events if evt.get("type") == "node_started"]

	assert _node_started_order(events_a) == _node_started_order(events_b)
	assert any(evt.get("type") == "run_finished" and evt.get("status") == "succeeded" for evt in events_a)
	assert any(evt.get("type") == "run_finished" and evt.get("status") == "succeeded" for evt in events_b)
