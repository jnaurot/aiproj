import importlib
import sys
import types

import pytest

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus


@pytest.mark.asyncio
async def test_run_fails_fast_on_removed_chat_port(tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    events = []
    graph = {
        "nodes": [
            {
                "id": "n1",
                "data": {
                    "kind": "source",
                    "label": "Source",
                    "sourceKind": "file",
                    "params": {"file_path": "dummy.txt", "file_format": "txt"},
                    "ports": {"in": None, "out": "chat"},
                },
            }
        ],
        "edges": [],
    }
    store = DiskArtifactStore(tmp_path / "artifacts")
    cache = SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite"))
    await run_mod.run_graph(
        run_id="run-chat-port-invalid",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-chat-port-invalid", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-chat-port-invalid",
    )
    finish = [e for e in events if e.get("type") == "run_finished"]
    assert finish and finish[-1].get("status") == "failed"
    logs = [e for e in events if e.get("type") == "log"]
    assert any("Unsupported output port type 'chat'" in str(x.get("message", "")) for x in logs)
