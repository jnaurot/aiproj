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
async def test_work_stream_emits_item_dequeued_before_input_drained(monkeypatch) -> None:
	"""Contract test: dequeue should be explicit and ordered before drained/terminal signals."""
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
		work_item = ((node.get("data", {}).get("params", {}) or {}).get("_work_item") or {})
		return NodeOutput(
			status="succeeded",
			metadata=None,
			execution_time_ms=1.0,
			data={"kind": "json", "payload": {"item": work_item.get("itemPreview")}, "meta": {"ok": True}},
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
			{
				"id": "e_stream",
				"source": "src",
				"target": "sink",
				"targetHandle": "in",
				"data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 16}},
			}
		],
	}

	events: list[dict] = []
	bus = RunEventBus("run-dequeue-order", on_emit=lambda evt: events.append(dict(evt)))
	await run_mod.run_graph(
		run_id="run-dequeue-order",
		graph=graph,
		run_from=None,
		bus=bus,
		artifact_store=MemoryArtifactStore(),
		cache=ExecutionCache(),
		graph_id="graph-dequeue-order",
	)

	control = [evt for evt in events if str(evt.get("type") or "") == "control_signal"]
	item_dequeued_index = next(
		(i for i, evt in enumerate(control) if str(evt.get("signal") or "") == "item_dequeued" and str(evt.get("edgeId") or "") == "e_stream"),
		-1,
	)
	input_drained_index = next(
		(i for i, evt in enumerate(control) if str(evt.get("signal") or "") == "input_drained" and str(evt.get("edgeId") or "") == "e_stream"),
		-1,
	)
	node_terminal_index = next(
		(i for i, evt in enumerate(control) if str(evt.get("signal") or "") == "node_terminal" and str(evt.get("nodeId") or "") == "sink"),
		-1,
	)

	assert item_dequeued_index >= 0
	assert input_drained_index >= 0
	assert node_terminal_index >= 0
	assert item_dequeued_index < input_drained_index
	assert input_drained_index < node_terminal_index
