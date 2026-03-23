from __future__ import annotations

import importlib
import sys
import types

import pytest

from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


def _ensure_duckdb_stub() -> None:
	if "duckdb" not in sys.modules:
		sys.modules["duckdb"] = types.SimpleNamespace()


@pytest.mark.asyncio
async def test_reject_marked_in_output_is_non_error(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="failed",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"reject": True}, "meta": {"reject": True, "decision": "reject"}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-reject",
		graph={
			"nodes": [
				{"id": "filter_like", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}}
			],
			"edges": [],
		},
		run_from=None,
		bus=RunEventBus("run-reject", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-reject",
	)
	run_finished = [e for e in events if e.get("type") == "run_finished"]
	assert run_finished and run_finished[-1].get("status") == "succeeded"
