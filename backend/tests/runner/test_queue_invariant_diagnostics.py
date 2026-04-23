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
async def test_run_emits_queue_invariant_summary_log_without_changing_success_outcome(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "src":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [1, 2], "meta": {"ok": True}},
			)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "sink",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "single_item", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
		],
		"edges": [
			{"id": "e_diag", "source": "src", "target": "sink", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items"}}}
		],
	}

	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-queue-invariant",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-queue-invariant", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-queue-invariant",
	)

	finished = [evt for evt in events if str(evt.get("type") or "") == "run_finished"]
	assert finished and str(finished[-1].get("status") or "") == "succeeded"

	invariant_logs = [
		evt
		for evt in events
		if str(evt.get("type") or "") == "log"
		and "[queue-invariant]" in str(evt.get("message") or "")
	]
	assert invariant_logs, "expected queue invariant diagnostics log"
	assert any("violations=0" in str(evt.get("message") or "") for evt in invariant_logs)
	assert not any(
		str(evt.get("reasonCode") or "") == "QUEUE_DEPTH_INVARIANT_VIOLATION"
		for evt in invariant_logs
	)


@pytest.mark.asyncio
async def test_run_emits_queue_invariant_warning_when_residual_depth_exists(monkeypatch) -> None:
	_ensure_duckdb_stub()
	run_mod = importlib.import_module("app.runner.run")
	orig_metrics = run_mod.QueueRegistry.metrics

	def _metrics_with_forced_mismatch(self):
		out = orig_metrics(self)
		edges = out.get("edges") if isinstance(out.get("edges"), dict) else {}
		if edges:
			for key in list(edges.keys()):
				bucket = edges.get(key) if isinstance(edges.get(key), dict) else {}
				bucket["depth"] = int(bucket.get("depth") or 0) + 1
				edges[key] = bucket
				break
		else:
			edges["forced:in"] = {"edgeId": "forced", "inputHandle": "in", "depth": 1, "enqueued": 0, "dequeued": 0}
		out["edges"] = edges
		return out

	monkeypatch.setattr(run_mod.QueueRegistry, "metrics", _metrics_with_forced_mismatch)

	async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
		node_id = str(node.get("id") or "")
		if node_id == "src":
			return NodeOutput(
				status="succeeded",
				metadata=None,
				execution_time_ms=1.0,
				data={"kind": "json", "payload": [1, 2, 3], "meta": {"ok": True}},
			)
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"ok": True}, "meta": {"ok": True}},
		)

	monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
	graph = {
		"nodes": [
			{"id": "src", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
			{
				"id": "once_sink",
				"data": {
					"kind": "tool",
					"processingPolicy": {"consume_mode": "once", "batch_size": 1, "max_inflight": 1},
					"params": {"provider": "builtin", "builtin": {"toolId": "noop"}},
				},
			},
		],
		"edges": [
			{"id": "e_once_diag", "source": "src", "target": "once_sink", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items"}}}
		],
	}

	events: list[dict] = []
	await run_mod.run_graph(
		run_id="run-queue-invariant-once",
		graph=graph,
		run_from=None,
		bus=RunEventBus("run-queue-invariant-once", on_emit=lambda evt: events.append(dict(evt))),
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-queue-invariant-once",
	)

	assert any(
		str(evt.get("type") or "") == "log"
		and str(evt.get("reasonCode") or "") == "QUEUE_DEPTH_INVARIANT_VIOLATION"
		for evt in events
	)
