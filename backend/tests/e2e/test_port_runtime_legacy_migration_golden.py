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
async def test_port_runtime_legacy_migration_golden_emits_deprecation_warnings() -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	events: list[dict] = []

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {}},
		)

	run_mod.exec_tool = _fake_exec_tool
	graph = {
		"nodes": [
			{
				"id": "src",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"portContracts": {"out": {"out": {"affinity": "work"}}},
				},
			},
			{
				"id": "dst",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"schema": {
						"expectedInputSchema": {
							"typedSchema": {"type": "json", "fields": []},
							"source": "declared",
							"state": "fresh",
						}
					},
					"portContracts": {"in": {"in": {"affinity": "work"}}},
				},
			},
		],
		"edges": [
			{
				"id": "e1",
				"source": "src",
				"target": "dst",
				"targetHandle": "in",
				"data": {
					"mode": "work",
					"queue": {"policy": "round_robin", "max": 1000, "overflow": "block"},
					"contract": {
						"payload": {"source": {"type": "json"}, "target": {"type": "json"}},
						"snapshot": {
							"sourceSchemaFingerprint": "{\"type\":\"json\"}",
							"targetSchemaFingerprint": "{\"type\":\"json\"}",
							"compatible": True,
							"decision": "native",
						},
					},
				},
			}
		],
	}
	await run_mod.run_graph(
		run_id="run-port-runtime-legacy-migration-golden",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-port-runtime-legacy-migration-golden", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-port-runtime-legacy-migration-golden",
	)

	warn_logs = [evt for evt in events if str(evt.get("type") or "") == "log" and str(evt.get("level") or "") == "warn"]
	messages = [str(evt.get("message") or "") for evt in warn_logs]
	assert any("[LEGACY_EXPECTED_INPUT_SCHEMA_DEPRECATED]" in msg for msg in messages)
	assert any("[LEGACY_PORT_CONTRACTS_DEPRECATED]" in msg for msg in messages)
	assert any("[EDGE_QUEUE_POLICY_PREVIEW]" in msg for msg in messages)

	finished = [evt for evt in events if str(evt.get("type") or "") == "run_finished"]
	assert finished
	assert str(finished[-1].get("status") or "") == "succeeded"
