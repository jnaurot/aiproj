import asyncio
import importlib
import sys
import types

import pytest

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.metadata import NodeOutput
from app.runtime import RuntimeManager


async def _wait_for_node_running(rt: RuntimeManager, run_id: str, node_id: str, tries: int = 80):
    for _ in range(tries):
        h = rt.get_run(run_id)
        if h and h.node_status.get(node_id) == "running":
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"Node did not reach running state: {node_id}")


@pytest.mark.asyncio
async def test_cancel_run_is_idempotent_and_marks_cancelled(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")

    async def _slow_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        await asyncio.sleep(2.0)
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_tool", _slow_exec_tool)
    monkeypatch.setenv("ARTIFACT_STORE", "disk")
    monkeypatch.setenv("ARTIFACT_DIR", str(tmp_path / "artifact-root"))

    graph = {
        "nodes": [
            {
                "id": "tool_1",
                "data": {
                    "kind": "tool",
                    "label": "Tool",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                    "ports": {"in": None, "out": "json"},
                },
            }
        ],
        "edges": [],
    }

    rt = RuntimeManager()
    run_id = "run-cancel-1"
    rt.create_run(run_id)
    await rt.start_run(run_id, graph, run_from=None, graph_id="graph-cancel-1")

    # Wait until run is active.
    for _ in range(40):
        h = rt.get_run(run_id)
        if h and h.status in ("running", "cancel_requested"):
            break
        await asyncio.sleep(0.05)

    first = await rt.request_cancel(run_id)
    assert first["cancelRequested"] is True
    assert first["status"] == "cancel_requested"

    second = await rt.request_cancel(run_id)
    assert second["cancelRequested"] is True
    assert second["status"] == "cancel_requested"

    await rt.get_run(run_id).task
    h = rt.get_run(run_id)
    assert h.status == "cancelled"

    events = await rt.list_run_events(run_id, after_id=0, limit=5000)
    event_types = [e["type"] for e in events]
    assert "run_cancel_requested" in event_types
    assert "run_cancelled" in event_types
    assert "run_finished" in event_types
    finished_payloads = [e["payload"] for e in events if e["type"] == "run_finished"]
    assert finished_payloads and finished_payloads[-1].get("status") == "cancelled"


@pytest.mark.asyncio
async def test_cancel_during_level1_prevents_level2_start(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")

    async def _slow_exec_source(run_id, node, context, upstream_artifact_ids=None):
        await asyncio.sleep(2.0)
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="source")

    async def _fast_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_source", _slow_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fast_exec_tool)
    monkeypatch.setenv("ARTIFACT_STORE", "disk")
    monkeypatch.setenv("ARTIFACT_DIR", str(tmp_path / "artifact-root"))

    graph = {
        "nodes": [
            {
                "id": "source_1",
                "data": {
                    "kind": "source",
                    "label": "Source",
                    "sourceKind": "file",
                    "params": {"file_path": "dummy.txt", "file_format": "txt"},
                    "ports": {"in": None, "out": "text"},
                },
            },
            {
                "id": "tool_l2",
                "data": {
                    "kind": "tool",
                    "label": "Tool L2",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                    "ports": {"in": "text", "out": "json"},
                },
            },
        ],
        "edges": [{"id": "e1", "source": "source_1", "target": "tool_l2"}],
    }

    rt = RuntimeManager()
    run_id = "run-cancel-level1"
    rt.create_run(run_id)
    await rt.start_run(run_id, graph, run_from=None, graph_id="graph-cancel-level1")
    await _wait_for_node_running(rt, run_id, "source_1")
    cancel = await rt.request_cancel(run_id)
    assert cancel["cancelRequested"] is True
    await rt.get_run(run_id).task

    h = rt.get_run(run_id)
    assert h.status == "cancelled"
    events = await rt.list_run_events(run_id, after_id=0, limit=5000)
    event_types = [e["type"] for e in events]
    assert "run_cancel_requested" in event_types
    assert "run_cancelled" in event_types

    level2_starts = [
        e for e in events if e["type"] == "node_started" and e["payload"].get("nodeId") == "tool_l2"
    ]
    assert not level2_starts


@pytest.mark.asyncio
async def test_cancel_inflight_llm_emits_node_cancelled_and_no_artifact(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")

    async def _fast_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="hello")

    async def _slow_exec_llm(run_id, node, context, upstream_artifact_ids=None):
        await asyncio.sleep(2.0)
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="llm output")

    monkeypatch.setattr(run_mod, "exec_source", _fast_exec_source)
    monkeypatch.setattr(run_mod, "exec_llm", _slow_exec_llm)
    monkeypatch.setenv("ARTIFACT_STORE", "disk")
    monkeypatch.setenv("ARTIFACT_DIR", str(tmp_path / "artifact-root"))

    graph = {
        "nodes": [
            {
                "id": "source_1",
                "data": {
                    "kind": "source",
                    "label": "Source",
                    "sourceKind": "file",
                    "params": {"file_path": "dummy.txt", "file_format": "txt"},
                    "ports": {"in": None, "out": "text"},
                },
            },
            {
                "id": "llm_1",
                "data": {
                    "kind": "llm",
                    "label": "LLM",
                    "llmKind": "ollama",
                    "params": {
                        "base_url": "http://localhost:11434",
                        "model": "fake-model",
                        "user_prompt": "summarize",
                        "output_mode": "text",
                    },
                    "ports": {"in": "text", "out": "text"},
                },
            },
        ],
        "edges": [{"id": "e1", "source": "source_1", "target": "llm_1"}],
    }

    rt = RuntimeManager()
    run_id = "run-cancel-llm"
    rt.create_run(run_id)
    await rt.start_run(run_id, graph, run_from=None, graph_id="graph-cancel-llm")
    await _wait_for_node_running(rt, run_id, "llm_1")
    await rt.request_cancel(run_id)
    await rt.get_run(run_id).task

    events = await rt.list_run_events(run_id, after_id=0, limit=5000)
    llm_cancelled = [
        e for e in events if e["type"] == "node_cancelled" and e["payload"].get("nodeId") == "llm_1"
    ]
    assert llm_cancelled

    llm_outputs = [
        e for e in events if e["type"] == "node_output" and e["payload"].get("nodeId") == "llm_1"
    ]
    assert not llm_outputs
    assert "llm_1" not in rt.get_run(run_id).node_outputs
