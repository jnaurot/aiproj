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


@pytest.mark.asyncio
async def test_run_from_selected_llm_has_non_empty_upstream_and_succeeds(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    seen = {"upstream_counts": []}

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="hello source")

    async def _fake_exec_llm(run_id, node, context, upstream_artifact_ids=None):
        upstream_artifact_ids = upstream_artifact_ids or []
        seen["upstream_counts"].append(len(upstream_artifact_ids))
        assert upstream_artifact_ids, "Expected non-empty upstream artifacts for selected LLM run"
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="llm output")

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_llm", _fake_exec_llm)

    artifact_root = tmp_path / "artifacts"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))
    graph = _graph()

    events_1 = []
    await run_mod.run_graph(
        run_id="run-llm-full",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-llm-full", on_emit=lambda e: events_1.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-run-from-selected-llm",
    )
    assert seen["upstream_counts"] and seen["upstream_counts"][-1] >= 1
    run_finished_1 = [e for e in events_1 if e.get("type") == "run_finished"]
    assert run_finished_1 and run_finished_1[-1].get("status") == "succeeded"

    events_2 = []
    await run_mod.run_graph(
        run_id="run-llm-selected",
        graph=graph,
        run_from="llm_1",
        bus=RunEventBus("run-llm-selected", on_emit=lambda e: events_2.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-run-from-selected-llm",
    )

    # No upstream-empty failure, no pydantic/node-output failure, succeeds via cache or execution.
    assert all(c >= 1 for c in seen["upstream_counts"])
    run_finished = [e for e in events_2 if e.get("type") == "run_finished"]
    assert run_finished and run_finished[-1].get("status") == "succeeded"
    assert not any("pydantic" in str(e.get("message", "")).lower() for e in events_2 if e.get("type") == "log")
