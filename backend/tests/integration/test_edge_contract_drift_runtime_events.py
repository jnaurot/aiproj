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
async def test_runtime_emits_contract_drift_event(monkeypatch) -> None:
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
			{
				"id": "a",
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}},
			},
			{
				"id": "b",
				"data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}},
			},
		],
		"edges": [
			{
				"id": "e_drift_evt",
				"source": "a",
				"target": "b",
				"data": {
					"mode": "work",
					"contract": {
						"payload": {"source": {"type": "json"}, "target": {"type": "json"}},
						"snapshot": {
							"sourceSchemaFingerprint": "{\"type\":\"text\"}",
							"targetSchemaFingerprint": "{\"type\":\"text\"}",
							"compatible": True,
							"decision": "native",
						},
					},
				},
			}
		],
	}
	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-drift-event",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-drift-event", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-drift-event",
	)
	drift_events = [evt for evt in events if evt.get("type") == "contract_drift"]
	assert drift_events, "expected contract_drift event"
	assert drift_events[0].get("edgeId") == "e_drift_evt"

