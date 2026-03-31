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
async def test_run_started_emits_execution_contract_v1(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
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
		"edges": [{"id": "e1", "source": "a", "target": "b"}],
	}
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-contract-v1",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-contract-v1", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-contract-v1",
	)
	started = next((evt for evt in events if evt.get("type") == "run_started"), None)
	assert isinstance(started, dict)
	contract = started.get("executionContract")
	assert isinstance(contract, dict)
	assert int(contract.get("contractVersion") or 0) == 1
	assert str(contract.get("graphId") or "") == "g-contract-v1"
	basis = contract.get("basis")
	assert isinstance(basis, dict)
	assert str(basis.get("graphId") or "") == "g-contract-v1"
	assert isinstance(basis.get("nodes"), dict)

