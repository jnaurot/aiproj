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
async def test_port_runtime_warning_dedupe_golden_same_handle_multi_edge(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "jobs":
			payload = [{"job_id": 1}, {"job_id": 2}]
		elif node_id in {"params_one", "params_two"}:
			payload = []
		else:
			payload = {"ok": True}
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": payload, "meta": {}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	events: list[dict] = []
	graph = {
		"nodes": [
			{"id": "jobs", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "params_one", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "params_two", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "select",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
				},
			},
		],
		"edges": [
			{"id": "e_work", "source": "jobs", "target": "select", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 16}}},
			{"id": "e_param_1", "source": "params_one", "target": "select", "targetHandle": "param_filters", "data": {"mode": "param"}},
			{"id": "e_param_2", "source": "params_two", "target": "select", "targetHandle": "param_filters", "data": {"mode": "param"}},
		],
	}
	await run_mod.run_graph(
		run_id="run-port-runtime-warning-dedupe",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-port-runtime-warning-dedupe", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-port-runtime-warning-dedupe",
	)

	warnings = [evt for evt in events if str(evt.get("type") or "") == "node_input_warning"]
	assert len(warnings) == 1
	assert str(warnings[0].get("nodeId") or "") == "select"
	assert str(warnings[0].get("handle") or "") == "param_filters"

	summaries = [evt for evt in events if str(evt.get("type") or "") == "node_warning_summary"]
	assert len(summaries) == 2
	assert [int(evt.get("count") or 0) for evt in summaries] == [1, 2]
	assert len({str(evt.get("warningKey") or "") for evt in summaries}) == 1

	finished = [evt for evt in events if str(evt.get("type") or "") == "run_finished"]
	assert finished
	assert str(finished[-1].get("status") or "") == "succeeded"
