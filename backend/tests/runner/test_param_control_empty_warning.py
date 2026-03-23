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
async def test_param_edge_empty_payload_warns_once_after_upstream_finishes(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "work_src":
			payload = [{"job_id": 1}]
		elif node_id == "param_src":
			payload = {}
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
			{"id": "param_src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
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
			{"id": "e_param", "source": "param_src", "target": "dst", "targetHandle": "param_filters", "data": {"mode": "param"}},
		],
	}
	await run_mod.run_graph(
		run_id="run-param-control-warning",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-param-control-warning", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="g-param-control-warning",
	)
	warnings = [evt for evt in events if str(evt.get("type") or "") == "node_input_warning"]
	assert len(warnings) == 1
	warning = warnings[0]
	assert str(warning.get("nodeId") or "") == "dst"
	assert str(warning.get("edgeId") or "") == "e_param"
	assert str(warning.get("plane") or "") == "param"
	assert str(warning.get("code") or "") == "PARAM_CONTROL_EMPTY_INPUT"

	# Warning should not appear before the upstream param node has finished.
	param_finished_index = next(
		idx
		for idx, evt in enumerate(events)
		if str(evt.get("type") or "") == "node_finished" and str(evt.get("nodeId") or "") == "param_src"
	)
	warning_index = next(
		idx for idx, evt in enumerate(events) if str(evt.get("type") or "") == "node_input_warning"
	)
	assert warning_index > param_finished_index
