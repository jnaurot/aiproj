import importlib
import sys
import types

import pytest

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.metadata import NodeOutput
from app.runtime import RuntimeManager


@pytest.mark.asyncio
async def test_run_events_persist_and_replay_after_restart(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
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
                },
            }
        ],
        "edges": [],
    }

    rt1 = RuntimeManager()
    run_id = "run-events-1"
    rt1.create_run(run_id)
    await rt1.start_run(run_id, graph, run_from=None)
    await rt1.get_run(run_id).task

    replay_1 = await rt1.list_run_events(run_id, after_id=0, limit=2000)
    assert replay_1
    assert replay_1 == sorted(replay_1, key=lambda r: int(r["id"]))
    event_types = [r["type"] for r in replay_1]
    assert "run_started" in event_types
    assert "node_started" in event_types
    assert "node_output" in event_types
    assert "node_finished" in event_types
    assert "run_finished" in event_types

    # Simulate backend restart and replay from persisted DB.
    rt2 = RuntimeManager()
    replay_2 = await rt2.list_run_events(run_id, after_id=0, limit=2000)
    assert len(replay_2) == len(replay_1)
    assert [r["type"] for r in replay_2] == event_types
