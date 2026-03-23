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
async def test_runtime_warning_dedupe_emits_first_warning_once_and_summary_counts(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "work_src":
			payload = [{"job_id": 1}, {"job_id": 2}]
		elif node_id in {"param_a", "param_b"}:
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
			{"id": "work_src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "param_a", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{"id": "param_b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "dst",
				"data": {
					"kind": "tool",
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
				},
			},
		],
		"edges": [
			{"id": "e_work", "source": "work_src", "target": "dst", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 8}}},
			{"id": "e_param_a", "source": "param_a", "target": "dst", "targetHandle": "param_filters", "data": {"mode": "param"}},
			{"id": "e_param_b", "source": "param_b", "target": "dst", "targetHandle": "param_filters", "data": {"mode": "param"}},
		],
	}
	await run_mod.run_graph(
		run_id="run-warning-dedupe",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-warning-dedupe", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-warning-dedupe",
	)

	warnings = [evt for evt in events if str(evt.get("type") or "") == "node_input_warning"]
	assert len(warnings) == 1
	assert str(warnings[0].get("nodeId") or "") == "dst"
	assert str(warnings[0].get("handle") or "") == "param_filters"
	assert str(warnings[0].get("code") or "") == "PARAM_CONTROL_EMPTY_INPUT"

	summaries = [evt for evt in events if str(evt.get("type") or "") == "node_warning_summary"]
	assert len(summaries) == 2
	warning_keys = {str(evt.get("warningKey") or "") for evt in summaries}
	assert len(warning_keys) == 1
	counts = [int(evt.get("count") or 0) for evt in summaries]
	assert counts == [1, 2]
