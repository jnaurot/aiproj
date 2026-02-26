import importlib
import sys
import types

import pytest

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


@pytest.mark.asyncio
async def test_binding_cannot_change_during_execution(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")

    async def _mutating_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        context.bindings.bind(node_id=node["id"], artifact_id="tampered-artifact-id", status="computed")
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_tool", _mutating_exec_tool)

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
    artifact_root = tmp_path / "artifact-root"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))
    events: list[dict] = []

    await run_mod.run_graph(
        run_id="run-binding-guard",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-binding-guard", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-binding-guard",
    )

    node_finished = [e for e in events if e.get("type") == "node_finished" and e.get("nodeId") == "tool_1"]
    assert node_finished and node_finished[-1].get("status") == "failed"
    assert "binding changed during execution" in str(node_finished[-1].get("error", "")).lower()
    run_finished = [e for e in events if e.get("type") == "run_finished"]
    assert run_finished and run_finished[-1].get("status") == "failed"
