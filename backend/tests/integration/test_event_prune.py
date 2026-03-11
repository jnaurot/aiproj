import importlib
import sys
import types

import pytest

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.metadata import NodeOutput
from app.runtime import RuntimeManager


def _tool_graph(node_id: str) -> dict:
    return {
        "nodes": [
            {
                "id": node_id,
                "data": {
                    "kind": "tool",
                    "label": "Tool",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                },
            }
        ],
        "edges": [],
    }


async def _run_once(rt: RuntimeManager, run_id: str, graph: dict):
    rt.create_run(run_id)
    await rt.start_run(run_id, graph, run_from=None)
    await rt.get_run(run_id).task


@pytest.mark.asyncio
async def test_prune_events_keep_last_per_run_with_dry_run(monkeypatch, tmp_path):
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

    rt = RuntimeManager()
    await _run_once(rt, "run-prune-a", _tool_graph("tool_a"))
    await _run_once(rt, "run-prune-b", _tool_graph("tool_b"))

    before_a = await rt.list_run_events("run-prune-a", after_id=0, limit=5000)
    before_b = await rt.list_run_events("run-prune-b", after_id=0, limit=5000)
    assert len(before_a) >= 2
    assert len(before_b) >= 2

    dry = await rt.prune_events(keep_last=2, dry_run=True, run_id=None)
    assert dry["dry_run"] is True
    assert dry["runs_affected"] >= 2
    assert dry["rows_deleted"] >= (len(before_a) - 2) + (len(before_b) - 2)

    # dry_run does not mutate
    after_dry_a = await rt.list_run_events("run-prune-a", after_id=0, limit=5000)
    after_dry_b = await rt.list_run_events("run-prune-b", after_id=0, limit=5000)
    assert len(after_dry_a) == len(before_a)
    assert len(after_dry_b) == len(before_b)

    done = await rt.prune_events(keep_last=2, dry_run=False, run_id=None)
    assert done["dry_run"] is False
    assert done["runs_affected"] >= 2
    assert done["rows_deleted"] >= 1

    after_a = await rt.list_run_events("run-prune-a", after_id=0, limit=5000)
    after_b = await rt.list_run_events("run-prune-b", after_id=0, limit=5000)
    assert len(after_a) == 2
    assert len(after_b) == 2
    assert [e["id"] for e in after_a] == sorted(e["id"] for e in after_a)
    assert [e["id"] for e in after_b] == sorted(e["id"] for e in after_b)


@pytest.mark.asyncio
async def test_prune_events_single_run_scope(monkeypatch, tmp_path):
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

    rt = RuntimeManager()
    await _run_once(rt, "run-prune-only-a", _tool_graph("tool_a"))
    await _run_once(rt, "run-prune-only-b", _tool_graph("tool_b"))

    before_a = await rt.list_run_events("run-prune-only-a", after_id=0, limit=5000)
    before_b = await rt.list_run_events("run-prune-only-b", after_id=0, limit=5000)

    res = await rt.prune_events(keep_last=1, dry_run=False, run_id="run-prune-only-a")
    assert res["run_id"] == "run-prune-only-a"
    assert res["runs_affected"] in (0, 1)

    after_a = await rt.list_run_events("run-prune-only-a", after_id=0, limit=5000)
    after_b = await rt.list_run_events("run-prune-only-b", after_id=0, limit=5000)
    assert len(after_a) == min(1, len(before_a))
    assert len(after_b) == len(before_b)
