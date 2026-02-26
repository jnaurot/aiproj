import importlib
import os
import sys
import time
import types
from pathlib import Path

import pytest

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


def _source_graph(file_path: str) -> dict:
    return {
        "nodes": [
            {
                "id": "source_1",
                "data": {
                    "kind": "source",
                    "label": "Source",
                    "sourceKind": "file",
                    "params": {"file_path": file_path, "file_format": "txt"},
                    "ports": {"in": None, "out": "text"},
                },
            }
        ],
        "edges": [],
    }


@pytest.mark.asyncio
async def test_source_file_fingerprint_drives_cache_hit_and_miss(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    calls = {"source": 0}

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        calls["source"] += 1
        p = Path(node["data"]["params"]["file_path"])
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data=p.read_text(encoding="utf-8"),
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)

    file_path = tmp_path / "input.txt"
    file_path.write_text("alpha", encoding="utf-8")

    artifact_root = tmp_path / "artifact-root"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))
    graph = _source_graph(str(file_path))

    events_1: list[dict] = []
    await run_mod.run_graph(
        run_id="run-source-fp-1",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-source-fp-1", on_emit=lambda e: events_1.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-source-fp",
    )
    decisions_1 = [e for e in events_1 if e.get("type") == "cache_decision" and e.get("nodeId") == "source_1"]
    assert decisions_1 and decisions_1[-1].get("decision") == "cache_miss"
    out_1 = [e for e in events_1 if e.get("type") == "node_output" and e.get("nodeId") == "source_1"]
    assert out_1
    first_artifact_id = out_1[-1]["artifactId"]
    assert calls["source"] == 1

    events_2: list[dict] = []
    await run_mod.run_graph(
        run_id="run-source-fp-2",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-source-fp-2", on_emit=lambda e: events_2.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-source-fp",
    )
    decisions_2 = [e for e in events_2 if e.get("type") == "cache_decision" and e.get("nodeId") == "source_1"]
    assert decisions_2 and decisions_2[-1].get("decision") == "cache_hit"
    out_2 = [e for e in events_2 if e.get("type") == "node_output" and e.get("nodeId") == "source_1"]
    assert out_2 and out_2[-1]["artifactId"] == first_artifact_id
    assert out_2[-1].get("cached") is True
    assert calls["source"] == 1

    # Ensure fingerprint changes by modifying bytes and mtime.
    time.sleep(0.02)
    file_path.write_text("alpha-updated", encoding="utf-8")
    os.utime(file_path, None)

    events_3: list[dict] = []
    await run_mod.run_graph(
        run_id="run-source-fp-3",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-source-fp-3", on_emit=lambda e: events_3.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-source-fp",
    )
    decisions_3 = [e for e in events_3 if e.get("type") == "cache_decision" and e.get("nodeId") == "source_1"]
    assert decisions_3 and decisions_3[-1].get("decision") == "cache_miss"
    out_3 = [e for e in events_3 if e.get("type") == "node_output" and e.get("nodeId") == "source_1"]
    assert out_3 and out_3[-1]["artifactId"] != first_artifact_id
    assert calls["source"] == 2

