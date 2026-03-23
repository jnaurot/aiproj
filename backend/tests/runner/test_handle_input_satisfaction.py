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
async def test_same_handle_multi_edge_all_provided_emits_all(monkeypatch) -> None:
    _ensure_duckdb_stub()
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        node_id = str(node.get("id") or "")
        if node_id == "src_a":
            payload = [1, 2]
        elif node_id == "src_b":
            payload = [3, 4]
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
            {"id": "src_a", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
            {"id": "src_b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
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
            {"id": "e_a", "source": "src_a", "target": "dst", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 8}}},
            {"id": "e_b", "source": "src_b", "target": "dst", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 8}}},
        ],
    }
    await run_mod.run_graph(
        run_id="run-handle-sat-all",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-handle-sat-all", on_emit=lambda evt: events.append(dict(evt))),
        artifact_store=MemoryArtifactStore(),
        cache=ExecutionCache(),
        graph_id="g-handle-sat-all",
    )
    sat = [evt for evt in events if str(evt.get("type") or "") == "node_handle_satisfaction" and str(evt.get("nodeId") or "") == "dst"]
    assert sat
    assert str(sat[-1].get("status") or "") == "all"
    assert int(sat[-1].get("connectedEdges") or 0) == 2
    assert int(sat[-1].get("providedEdges") or 0) == 2


@pytest.mark.asyncio
async def test_same_handle_multi_edge_partial_provided_warns(monkeypatch) -> None:
    _ensure_duckdb_stub()
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        node_id = str(node.get("id") or "")
        if node_id == "src_a":
            payload = [1, 2]
        elif node_id == "src_b":
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
            {"id": "src_a", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
            {"id": "src_b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
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
            {"id": "e_a", "source": "src_a", "target": "dst", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 8}}},
            {"id": "e_b", "source": "src_b", "target": "dst", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 8}}},
        ],
    }
    await run_mod.run_graph(
        run_id="run-handle-sat-partial",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-handle-sat-partial", on_emit=lambda evt: events.append(dict(evt))),
        artifact_store=MemoryArtifactStore(),
        cache=ExecutionCache(),
        graph_id="g-handle-sat-partial",
    )
    sat = [evt for evt in events if str(evt.get("type") or "") == "node_handle_satisfaction" and str(evt.get("nodeId") or "") == "dst"]
    assert sat
    assert any(str(evt.get("status") or "") == "partial" for evt in sat)


@pytest.mark.asyncio
async def test_same_handle_multi_edge_none_provided_fails(monkeypatch) -> None:
    _ensure_duckdb_stub()
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        node_id = str(node.get("id") or "")
        if node_id.startswith("src_"):
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
            {"id": "src_a", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
            {"id": "src_b", "data": {"kind": "tool", "params": {"provider": "builtin", "builtin": {"toolId": "noop"}}}},
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
            {"id": "e_a", "source": "src_a", "target": "dst", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 8}}},
            {"id": "e_b", "source": "src_b", "target": "dst", "targetHandle": "in", "data": {"mode": "work", "work": {"item_mode": "json_items", "max_items": 8}}},
        ],
    }
    await run_mod.run_graph(
        run_id="run-handle-sat-none",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-handle-sat-none", on_emit=lambda evt: events.append(dict(evt))),
        artifact_store=MemoryArtifactStore(),
        cache=ExecutionCache(),
        graph_id="g-handle-sat-none",
    )
    sat = [evt for evt in events if str(evt.get("type") or "") == "node_handle_satisfaction" and str(evt.get("nodeId") or "") == "dst"]
    assert sat
    assert str(sat[-1].get("status") or "") == "none"
    dst_finished = [evt for evt in events if str(evt.get("type") or "") == "node_finished" and str(evt.get("nodeId") or "") == "dst"]
    assert dst_finished
    assert str(dst_finished[-1].get("status") or "") == "failed"
    assert str(dst_finished[-1].get("errorCode") or "") == "HANDLE_INPUT_NONE_PROVIDED"
