import importlib
import sys
import time
import types

import pytest
from fastapi.testclient import TestClient

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.metadata import NodeOutput
from app.runtime import RuntimeManager


def _graph() -> dict:
    return {
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
                "id": "tool_mid",
                "data": {
                    "kind": "tool",
                    "label": "Mid",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {"k": 1}}},
                    "ports": {"in": "text", "out": "json"},
                },
            },
            {
                "id": "tool_end",
                "data": {
                    "kind": "tool",
                    "label": "End",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {"k": 2}}},
                    "ports": {"in": "json", "out": "json"},
                },
            },
            {
                "id": "tool_side",
                "data": {
                    "kind": "tool",
                    "label": "Side",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {"k": 3}}},
                    "ports": {"in": "text", "out": "json"},
                },
            },
        ],
        "edges": [
            {"id": "e1", "source": "source_1", "target": "tool_mid"},
            {"id": "e2", "source": "tool_mid", "target": "tool_end"},
            {"id": "e3", "source": "source_1", "target": "tool_side"},
        ],
    }


@pytest.mark.asyncio
async def test_accept_params_marks_only_node_and_descendants_stale(monkeypatch):
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="hello")

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True, "node": node["id"]}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
    monkeypatch.setenv("ARTIFACT_STORE", "memory")

    from app.main import app

    graph = _graph()
    with TestClient(app) as client:
        create = client.post("/runs", json={"runFrom": None, "graph": graph})
        assert create.status_code == 200, create.text
        run_id = create.json()["runId"]

        status = None
        for _ in range(80):
            res = client.get(f"/runs/{run_id}")
            assert res.status_code == 200, res.text
            status = res.json()
            if status.get("status") in {"succeeded", "failed", "cancelled"}:
                break
            time.sleep(0.05)
        assert status and status.get("status") == "succeeded"

        before_bindings = status.get("nodeBindings") or {}
        for nid in ("source_1", "tool_mid", "tool_end", "tool_side"):
            assert before_bindings.get(nid, {}).get("lastArtifactId"), f"missing lastArtifactId for {nid}"
            assert before_bindings.get(nid, {}).get("currentArtifactId"), f"missing currentArtifactId for {nid}"

        updated_graph = _graph()
        for n in updated_graph["nodes"]:
            if n["id"] == "tool_mid":
                n["data"]["params"] = {"provider": "builtin", "builtin": {"toolId": "noop", "args": {"k": 999}}}

        accept = client.post(
            f"/runs/{run_id}/nodes/tool_mid/accept-params",
            json={"graph": updated_graph, "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {"k": 999}}}},
        )
        assert accept.status_code == 200, accept.text
        payload = accept.json()
        assert payload.get("affectedNodeIds") == ["tool_end", "tool_mid"]

        after = client.get(f"/runs/{run_id}")
        assert after.status_code == 200, after.text
        state = after.json()
        bindings = state.get("nodeBindings") or {}
        outputs = state.get("nodeOutputs") or {}

        assert bindings["tool_mid"]["status"] == "stale"
        assert bindings["tool_mid"]["staleReason"] == "PARAMS_CHANGED"
        assert bindings["tool_mid"]["currentArtifactId"] is None
        assert bindings["tool_mid"]["lastArtifactId"]

        assert bindings["tool_end"]["status"] == "stale"
        assert bindings["tool_end"]["staleReason"] == "UPSTREAM_CHANGED"
        assert bindings["tool_end"]["currentArtifactId"] is None
        assert bindings["tool_end"]["lastArtifactId"]

        assert bindings["source_1"]["status"] == "succeeded_up_to_date"
        assert bindings["source_1"]["currentArtifactId"]
        assert bindings["tool_side"]["status"] == "succeeded_up_to_date"
        assert bindings["tool_side"]["currentArtifactId"]

        assert "tool_mid" not in outputs
        assert "tool_end" not in outputs
        assert "source_1" in outputs
        assert "tool_side" in outputs


@pytest.mark.asyncio
async def test_partial_rerun_keeps_sibling_binding_sticky(monkeypatch):
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="hello")

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True, "node": node["id"]}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
    monkeypatch.setenv("ARTIFACT_STORE", "memory")

    rt = RuntimeManager()
    run_id = "sticky-run"
    graph = _graph()

    rt.create_run(run_id)
    await rt.start_run(run_id, graph, run_from=None)
    await rt.get_run(run_id).task

    h = rt.get_run(run_id)
    sibling_before = dict(h.node_bindings["tool_side"])
    before_events = await rt.list_run_events(run_id)
    before_last_event_id = int(before_events[-1].get("id") if before_events else 0)

    await rt.start_run(run_id, graph, run_from="tool_mid", run_mode="from_selected_onward")
    await rt.get_run(run_id).task

    h_after = rt.get_run(run_id)
    sibling_after = dict(h_after.node_bindings["tool_side"])
    assert sibling_after["status"] == sibling_before["status"]
    assert sibling_after["isUpToDate"] == sibling_before["isUpToDate"]
    assert sibling_after["currentArtifactId"] == sibling_before["currentArtifactId"]
    assert sibling_after["lastArtifactId"] == sibling_before["lastArtifactId"]
    assert sibling_after["lastRunId"] == sibling_before["lastRunId"]

    new_events = await rt.list_run_events(run_id, after_id=before_last_event_id)
    side_node_events = [
        row.get("payload", {})
        for row in new_events
        if (row.get("payload") or {}).get("nodeId") == "tool_side"
    ]
    assert not side_node_events
