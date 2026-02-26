import asyncio
import importlib
import sys
import types
from pathlib import Path

import pytest

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.artifacts import DiskArtifactStore
from app.runner.cache import SqliteExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput
from app.runtime import RuntimeManager


def _ensure_duckdb_stub() -> None:
    if "duckdb" not in sys.modules:
        sys.modules["duckdb"] = types.SimpleNamespace()


def _tool_graph(node_id: str = "tool_1") -> dict:
    return {
        "nodes": [
            {
                "id": node_id,
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


@pytest.mark.asyncio
async def test_restart_cache_hit_still_works(monkeypatch, tmp_path):
    _ensure_duckdb_stub()
    run_mod = importlib.import_module("app.runner.run")
    call_count = {"tool": 0}

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        call_count["tool"] += 1
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True, "node": node["id"]}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

    artifact_dir = tmp_path / "artifacts"
    cache_db = artifact_dir / "meta" / "artifacts.sqlite"

    events_1 = []
    store_1 = DiskArtifactStore(artifact_dir)
    cache_1 = SqliteExecutionCache(str(cache_db))
    bus_1 = RunEventBus("run-cache-1", on_emit=lambda evt: events_1.append(dict(evt)))
    graph = _tool_graph()

    await run_mod.run_graph(
        run_id="run-cache-1",
        graph=graph,
        run_from=None,
        bus=bus_1,
        artifact_store=store_1,
        cache=cache_1,
        graph_id="graph-cache",
    )

    assert call_count["tool"] == 1
    out_1 = [e for e in events_1 if e.get("type") == "node_output" and e.get("nodeId") == "tool_1"]
    assert out_1
    artifact_1 = out_1[-1]["artifactId"]

    # Simulate restart: rebuild store/cache from the same disk location.
    events_2 = []
    store_2 = DiskArtifactStore(artifact_dir)
    cache_2 = SqliteExecutionCache(str(cache_db))
    bus_2 = RunEventBus("run-cache-2", on_emit=lambda evt: events_2.append(dict(evt)))
    await run_mod.run_graph(
        run_id="run-cache-2",
        graph=graph,
        run_from=None,
        bus=bus_2,
        artifact_store=store_2,
        cache=cache_2,
        graph_id="graph-cache",
    )

    # Should be a cache hit; executor should not run again.
    assert call_count["tool"] == 1
    out_2 = [e for e in events_2 if e.get("type") == "node_output" and e.get("nodeId") == "tool_1"]
    assert out_2
    assert out_2[-1]["artifactId"] == artifact_1
    finish_2 = [e for e in events_2 if e.get("type") == "node_finished" and e.get("nodeId") == "tool_1"]
    assert finish_2 and finish_2[-1].get("cached") is True


@pytest.mark.asyncio
async def test_hard_delete_removes_artifacts_cache_and_unreferenced_blobs(monkeypatch, tmp_path):
    _ensure_duckdb_stub()
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
    run_id = "run-delete-1"
    rt.create_run(run_id)
    await rt.start_run(run_id, _tool_graph(), run_from=None, graph_id="graph-delete-run")
    await rt.get_run(run_id).task

    handle = rt.get_run(run_id)
    artifact_id = handle.node_outputs["tool_1"]
    art = await rt.artifact_store.get(artifact_id)
    blob_path = Path(art.storage_uri)
    assert blob_path.exists()

    # Cache row exists before delete.
    cache_conn = rt.cache._conn  # sqlite cache implementation
    cache_rows_before = cache_conn.execute(
        "SELECT COUNT(*) FROM execution_cache WHERE artifact_id=?", (artifact_id,)
    ).fetchone()[0]
    assert cache_rows_before == 1

    result = await rt.delete_run(run_id, mode="hard", gc="unreferenced")
    assert result["runDeleted"] is True
    assert result["mode"] == "hard"
    assert result["artifactsRemoved"] >= 1
    assert result["cacheRowsRemoved"] >= 1
    assert result["blobsDeleted"] >= 1

    assert not await rt.artifact_store.exists(artifact_id)
    cache_rows_after = cache_conn.execute(
        "SELECT COUNT(*) FROM execution_cache WHERE artifact_id=?", (artifact_id,)
    ).fetchone()[0]
    assert cache_rows_after == 0
    assert not blob_path.exists()


@pytest.mark.asyncio
async def test_lineage_inputs_producer_consumers_round_trip(monkeypatch, tmp_path):
    _ensure_duckdb_stub()
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data="hello from source",
        )

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"summary": "ok"}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

    events = []
    store = DiskArtifactStore(tmp_path / "artifacts")
    cache = SqliteExecutionCache(str(tmp_path / "artifacts" / "meta" / "artifacts.sqlite"))
    bus = RunEventBus("run-lineage-1", on_emit=lambda evt: events.append(dict(evt)))
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
                "id": "tool_1",
                "data": {
                    "kind": "tool",
                    "label": "Tool",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                    "ports": {"in": "text", "out": "json"},
                },
            },
        ],
        "edges": [{"id": "e1", "source": "source_1", "target": "tool_1"}],
    }

    await run_mod.run_graph(
        run_id="run-lineage-1",
        graph=graph,
        run_from=None,
        bus=bus,
        artifact_store=store,
        cache=cache,
        graph_id="graph-lineage",
    )

    node_output_by_node = {
        e["nodeId"]: e["artifactId"]
        for e in events
        if e.get("type") == "node_output" and e.get("nodeId") in {"source_1", "tool_1"}
    }
    source_artifact_id = node_output_by_node["source_1"]
    tool_artifact_id = node_output_by_node["tool_1"]

    tool_art = await store.get(tool_artifact_id)
    assert tool_art.node_id == "tool_1"
    assert tool_art.run_id == "run-lineage-1"
    assert tool_art.upstream_ids == [source_artifact_id]

    consumers = await store.get_consumers(source_artifact_id, limit=20)
    assert any(
        c.get("consumerNodeId") == "tool_1"
        and c.get("consumerRunId") == "run-lineage-1"
        and c.get("outputArtifactId") == tool_artifact_id
        for c in consumers
    )


@pytest.mark.asyncio
async def test_scheduler_caps_enforced_and_fail_fast_per_level(monkeypatch, tmp_path):
    _ensure_duckdb_stub()
    run_mod = importlib.import_module("app.runner.run")
    events = []

    max_seen = {"global": 0, "tool": 0, "llm": 0}
    state = {"global": 0, "tool": 0, "llm": 0}
    lock = asyncio.Lock()

    async def _inc(kind: str):
        async with lock:
            state["global"] += 1
            state[kind] += 1
            max_seen["global"] = max(max_seen["global"], state["global"])
            max_seen[kind] = max(max_seen[kind], state[kind])

    async def _dec(kind: str):
        async with lock:
            state["global"] -= 1
            state[kind] -= 1

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="src")

    async def _fake_exec_llm(run_id, node, context, upstream_artifact_ids=None):
        await _inc("llm")
        try:
            await asyncio.sleep(0.1)
            return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="llm ok")
        finally:
            await _dec("llm")

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        await _inc("tool")
        try:
            await asyncio.sleep(0.1)
            if node["id"] == "tool_b":
                return NodeOutput(
                    status="failed",
                    metadata=None,
                    execution_time_ms=1.0,
                    data={"kind": "json", "payload": {"ok": False}},
                    error="intentional tool failure",
                )
            return NodeOutput(
                status="succeeded",
                metadata=None,
                execution_time_ms=1.0,
                data={"kind": "json", "payload": {"ok": True}, "meta": {"status": "ok"}},
            )
        finally:
            await _dec("tool")

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_llm", _fake_exec_llm)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
    monkeypatch.setenv("RUNNER_MAX_CONCURRENCY", "2")
    monkeypatch.setenv("RUNNER_MAX_LLM", "1")
    monkeypatch.setenv("RUNNER_MAX_TOOL", "1")
    monkeypatch.setenv("RUNNER_MAX_SOURCE", "1")
    monkeypatch.setenv("RUNNER_MAX_TRANSFORM", "1")

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
            {
                "id": "tool_a",
                "data": {
                    "kind": "tool",
                    "label": "Tool A",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                    "ports": {"in": "text", "out": "json"},
                },
            },
            {
                "id": "tool_b",
                "data": {
                    "kind": "tool",
                    "label": "Tool B",
                    "params": {
                        "provider": "builtin",
                        "builtin": {"toolId": "noop", "args": {}},
                        "side_effect_mode": "effectful",
                        "armed": True,
                    },
                    "ports": {"in": "text", "out": "json"},
                },
            },
            {
                "id": "tool_z",
                "data": {
                    "kind": "tool",
                    "label": "Tool Z",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                    "ports": {"in": "text", "out": "json"},
                },
            },
        ],
        "edges": [
            {"id": "e1", "source": "source_1", "target": "llm_1"},
            {"id": "e2", "source": "source_1", "target": "tool_a"},
            {"id": "e3", "source": "source_1", "target": "tool_b"},
            {"id": "e4", "source": "llm_1", "target": "tool_z"},
        ],
    }

    bus = RunEventBus("run-scheduler-1", on_emit=lambda evt: events.append(dict(evt)))
    artifact_root = tmp_path / "scheduler-artifacts"
    await run_mod.run_graph(
        run_id="run-scheduler-1",
        graph=graph,
        run_from=None,
        bus=bus,
        artifact_store=DiskArtifactStore(artifact_root),
        cache=SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite")),
        graph_id="graph-scheduler",
    )

    assert max_seen["global"] <= 2
    assert max_seen["tool"] <= 1
    assert max_seen["llm"] <= 1

    # Fail-fast across levels: level containing tool_z must not execute.
    started_nodes = [e.get("nodeId") for e in events if e.get("type") == "node_started"]
    assert "tool_z" not in started_nodes

    run_finished = [e for e in events if e.get("type") == "run_finished"]
    assert run_finished and run_finished[-1].get("status") == "failed"
    cache_summary = [e for e in events if e.get("type") == "cache_summary"]
    assert cache_summary and cache_summary[-1].get("schema_version") == 1


@pytest.mark.asyncio
async def test_node_retention_keeps_last_five_artifacts(monkeypatch, tmp_path):
    _ensure_duckdb_stub()
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        marker = (node.get("data", {}).get("params", {}) or {}).get("marker")
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"marker": marker}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
    monkeypatch.setenv("ARTIFACT_STORE", "disk")
    monkeypatch.setenv("ARTIFACT_DIR", str(tmp_path / "artifact-retention"))

    rt = RuntimeManager()
    seen_graph_id = None
    for i in range(7):
        run_id = f"run-retention-{i}"
        graph = _tool_graph()
        graph["nodes"][0]["data"]["params"]["marker"] = i
        rt.create_run(run_id)
        await rt.start_run(run_id, graph, run_from=None, graph_id="graph-retention")
        await rt.get_run(run_id).task
        if seen_graph_id is None:
            seen_graph_id = rt.get_run(run_id).graph_id

    conn = rt.artifact_store._index._conn
    count = conn.execute(
        "SELECT COUNT(*) FROM artifacts WHERE graph_id=? AND node_id=?",
        (seen_graph_id, "tool_1"),
    ).fetchone()[0]
    assert count == 5


@pytest.mark.asyncio
async def test_delete_node_artifacts_hard_deletes_and_marks_descendants_stale(monkeypatch, tmp_path):
    _ensure_duckdb_stub()
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="hello")

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
    monkeypatch.setenv("ARTIFACT_STORE", "disk")
    monkeypatch.setenv("ARTIFACT_DIR", str(tmp_path / "artifact-delete-node"))

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
                "id": "tool_1",
                "data": {
                    "kind": "tool",
                    "label": "Tool",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                    "ports": {"in": "text", "out": "json"},
                },
            },
        ],
        "edges": [{"id": "e1", "source": "source_1", "target": "tool_1"}],
    }

    rt = RuntimeManager()
    run_id = "run-delete-node-1"
    rt.create_run(run_id)
    await rt.start_run(run_id, graph, run_from=None, graph_id="graph-delete-node")
    await rt.get_run(run_id).task
    handle = rt.get_run(run_id)
    graph_id = handle.graph_id

    before = rt.artifact_store._index._conn.execute(
        "SELECT COUNT(*) FROM artifacts WHERE graph_id=? AND node_id=?",
        (graph_id, "source_1"),
    ).fetchone()[0]
    assert before >= 1

    result = await rt.delete_node_artifacts(run_id=run_id, node_id="source_1", graph=graph)
    assert result["artifactsRemoved"] >= 1

    after = rt.artifact_store._index._conn.execute(
        "SELECT COUNT(*) FROM artifacts WHERE graph_id=? AND node_id=?",
        (graph_id, "source_1"),
    ).fetchone()[0]
    assert after == 0
    assert handle.node_bindings["source_1"]["status"] == "stale"
    assert handle.node_bindings["tool_1"]["status"] == "stale"


@pytest.mark.asyncio
async def test_no_cross_graph_reuse(monkeypatch, tmp_path):
    _ensure_duckdb_stub()
    run_mod = importlib.import_module("app.runner.run")
    calls = {"tool": 0}

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        calls["tool"] += 1
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
    artifact_dir = tmp_path / "artifacts-cross-graph"
    cache_db = artifact_dir / "meta" / "artifacts.sqlite"
    store = DiskArtifactStore(artifact_dir)
    cache = SqliteExecutionCache(str(cache_db))
    graph = _tool_graph()

    await run_mod.run_graph(
        run_id="run-graph-a",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-graph-a"),
        artifact_store=store,
        cache=cache,
        graph_id="graph-A",
    )
    await run_mod.run_graph(
        run_id="run-graph-b",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-graph-b"),
        artifact_store=store,
        cache=cache,
        graph_id="graph-B",
    )
    # same node/config in different graphs should compute twice (no cross-graph cache reuse)
    assert calls["tool"] == 2
