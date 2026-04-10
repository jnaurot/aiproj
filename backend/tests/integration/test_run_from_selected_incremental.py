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
                    "schema": {"expectedSchema": {"typedSchema": {"type": "json", "fields": []}}},
                    "params": {"file_path": "dummy.txt", "file_format": "txt"},
                },
            },
            {
                "id": "tool_mid",
                "data": {
                    "kind": "tool",
                    "label": "Mid",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                },
            },
            {
                "id": "tool_end",
                "data": {
                    "kind": "tool",
                    "label": "End",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                },
            },
        ],
        "edges": [
            {"id": "e1", "source": "source_1", "target": "tool_mid"},
            {"id": "e2", "source": "tool_mid", "target": "tool_end"},
        ],
    }


@pytest.mark.asyncio
async def test_run_from_selected_resolves_ancestors_from_cache(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    calls = {"source": 0, "tool": 0}

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        calls["source"] += 1
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"text": "hello"},
        )

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        calls["tool"] += 1
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True, "node": node["id"]}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

    artifact_root = tmp_path / "artifacts"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))
    graph = _graph()

    events_1 = []
    await run_mod.run_graph(
        run_id="run-full",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-full", on_emit=lambda e: events_1.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-incremental",
    )
    assert calls["source"] == 1
    assert calls["tool"] == 2

    events_2 = []
    await run_mod.run_graph(
        run_id="run-from-mid",
        graph=graph,
        run_from="tool_mid",
        bus=RunEventBus("run-from-mid", on_emit=lambda e: events_2.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-incremental",
    )

    # Ancestor + selected + downstream are resolved from cache; no recompute.
    assert calls["source"] == 1
    assert calls["tool"] == 2

    by_node = {
        nid: [e for e in events_2 if e.get("type") == "cache_decision" and e.get("nodeId") == nid]
        for nid in ("source_1", "tool_mid", "tool_end")
    }
    assert all(v and v[-1].get("decision") == "cache_hit" for v in by_node.values())


@pytest.mark.asyncio
async def test_run_selected_only_executes_selected_and_uses_cached_ancestors(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    calls = {"source": 0, "tool": 0}

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        calls["source"] += 1
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"text": "hello"},
        )

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        calls["tool"] += 1
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True, "node": node["id"]}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

    artifact_root = tmp_path / "artifacts"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))
    graph = _graph()

    await run_mod.run_graph(
        run_id="run-full-selected-only",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-full-selected-only"),
        artifact_store=store,
        cache=cache,
        graph_id="graph-selected-only",
    )
    assert calls["source"] == 1
    assert calls["tool"] == 2

    events_2 = []
    await run_mod.run_graph(
        run_id="run-selected-only-mid",
        graph=graph,
        run_from="tool_mid",
        run_mode="selected_only",
        bus=RunEventBus("run-selected-only-mid", on_emit=lambda e: events_2.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-selected-only",
    )

    # Selected-only: ancestor resolved from cache, selected resolved from cache,
    # downstream node is not in the plan and is not touched.
    assert calls["source"] == 1
    assert calls["tool"] == 2

    by_node = {
        nid: [e for e in events_2 if e.get("type") == "cache_decision" and e.get("nodeId") == nid]
        for nid in ("source_1", "tool_mid", "tool_end")
    }
    assert by_node["source_1"] and by_node["source_1"][-1].get("decision") == "cache_hit"
    assert by_node["tool_mid"] and by_node["tool_mid"][-1].get("decision") == "cache_hit"
    assert not by_node["tool_end"]












@pytest.mark.asyncio
async def test_legacy_pinned_hints_are_ignored(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    calls = {"source": 0, "tool": 0}

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        calls["source"] += 1
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"text": "hello"},
        )

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        calls["tool"] += 1
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True, "node": node["id"]}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

    artifact_root = tmp_path / "artifacts"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

    graph = _graph()
    graph["__executionHints"] = {
        "pinnedNodeIds": ["tool_mid"],
        "pinnedArtifacts": {
            "tool_mid": {
                "artifactId": "legacy-artifact-id",
                "execKey": "legacy-exec",
            }
        },
    }

    events = []
    await run_mod.run_graph(
        run_id="run-legacy-pin-hints-ignored",
        graph=graph,
        run_from="tool_end",
        run_mode="from_selected_onward",
        bus=RunEventBus("run-legacy-pin-hints-ignored", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-legacy-pin-hints",
    )

    # Legacy pin hints are ignored in checkpoint-only mode; execution falls back to normal planning.
    assert calls["source"] == 1
    assert calls["tool"] == 2
    run_finished = [e for e in events if e.get("type") == "run_finished"]
    assert run_finished and run_finished[-1].get("status") == "succeeded"

@pytest.mark.asyncio
async def test_checkpoint_hint_valid_marks_cache_only_and_reuses(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    calls = {"source": 0, "tool_mid": 0, "tool_end": 0}

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        calls["source"] += 1
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data={"text": "hello"})

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        node_id = str((node or {}).get("id") or "")
        if node_id in calls:
            calls[node_id] += 1
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"node": node_id}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

    artifact_root = tmp_path / "artifacts"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))
    graph = _graph()

    baseline_events = []
    await run_mod.run_graph(
        run_id="run-checkpoint-valid-baseline",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-checkpoint-valid-baseline", on_emit=lambda e: baseline_events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-checkpoint-valid",
    )
    source_artifact = str(
        next(e for e in baseline_events if e.get("type") == "node_output" and e.get("nodeId") == "source_1").get(
            "artifactId"
        )
        or ""
    ).strip()
    mid_artifact = str(
        next(e for e in baseline_events if e.get("type") == "node_output" and e.get("nodeId") == "tool_mid").get(
            "artifactId"
        )
        or ""
    ).strip()
    assert source_artifact
    assert mid_artifact
    missing_graph_basis = _graph()
    missing_graph_basis["nodes"][1]["data"]["params"]["extra"] = "recompute"
    checkpoint_fp = run_mod.compute_memo_key_for_node(
        next(node for node in missing_graph_basis["nodes"] if node.get("id") == "tool_mid"),
        [source_artifact],
    )
    source_checkpoint_fp = run_mod.compute_memo_key_for_node(
        next(node for node in graph["nodes"] if node.get("id") == "source_1"),
        [],
    )
    assert isinstance(checkpoint_fp, str) and len(checkpoint_fp) == 64
    assert isinstance(source_checkpoint_fp, str) and len(source_checkpoint_fp) == 64

    graph_with_checkpoint = _graph()
    graph_with_checkpoint["__executionHints"] = {
        "checkpoints": {
            "source_1": {
                "artifactId": source_artifact,
                "execKey": "trusted-checkpoint-source",
                "fingerprintAtCreation": source_checkpoint_fp,
            },
            "tool_mid": {
                "artifactId": mid_artifact,
                "execKey": "trusted-checkpoint-mid",
                "fingerprintAtCreation": checkpoint_fp,
            }
        }
    }
    events = []
    await run_mod.run_graph(
        run_id="run-checkpoint-valid-reuse",
        graph=graph_with_checkpoint,
        run_from="tool_end",
        run_mode="from_selected_onward",
        bus=RunEventBus("run-checkpoint-valid-reuse", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-checkpoint-valid",
    )

    assert calls["tool_mid"] == 1
    run_finished = [e for e in events if e.get("type") == "run_finished"]
    assert run_finished
    outcomes = run_finished[-1].get("checkpoint_outcomes") or {}
    assert outcomes.get("source_1") == "valid"
    assert outcomes.get("tool_mid") in {"valid", "stale"}


@pytest.mark.asyncio
async def test_checkpoint_hint_stale_recomputes(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    calls = {"tool_mid": 0}

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data={"text": "hello"})

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        node_id = str((node or {}).get("id") or "")
        if node_id == "tool_mid":
            calls["tool_mid"] += 1
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"node": node_id}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

    artifact_root = tmp_path / "artifacts"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

    graph = _graph()
    baseline_events = []
    await run_mod.run_graph(
        run_id="run-checkpoint-stale-baseline",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-checkpoint-stale-baseline", on_emit=lambda e: baseline_events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-checkpoint-stale",
    )
    mid_artifact = str(
        next(e for e in baseline_events if e.get("type") == "node_output" and e.get("nodeId") == "tool_mid").get(
            "artifactId"
        )
        or ""
    ).strip()
    assert mid_artifact

    graph_stale = _graph()
    graph_stale["nodes"][1]["data"]["params"]["extra"] = "changed"
    graph_stale["__executionHints"] = {
        "checkpoints": {
            "tool_mid": {
                "artifactId": mid_artifact,
                "execKey": "trusted-checkpoint-mid",
                "fingerprintAtCreation": "a" * 64,
            }
        }
    }
    events = []
    await run_mod.run_graph(
        run_id="run-checkpoint-stale-recompute",
        graph=graph_stale,
        run_from="tool_end",
        run_mode="from_selected_onward",
        bus=RunEventBus("run-checkpoint-stale-recompute", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-checkpoint-stale",
    )

    assert calls["tool_mid"] == 2
    run_finished = [e for e in events if e.get("type") == "run_finished"]
    assert run_finished
    assert (run_finished[-1].get("checkpoint_outcomes") or {}).get("tool_mid") == "stale"


@pytest.mark.asyncio
async def test_checkpoint_hint_missing_artifact_recomputes(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    calls = {"tool_mid": 0}

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data={"text": "hello"})

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        node_id = str((node or {}).get("id") or "")
        if node_id == "tool_mid":
            calls["tool_mid"] += 1
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"node": node_id}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

    artifact_root = tmp_path / "artifacts"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))
    graph = _graph()

    baseline_events = []
    await run_mod.run_graph(
        run_id="run-checkpoint-missing-baseline",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-checkpoint-missing-baseline", on_emit=lambda e: baseline_events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-checkpoint-missing",
    )
    source_artifact = str(
        next(e for e in baseline_events if e.get("type") == "node_output" and e.get("nodeId") == "source_1").get(
            "artifactId"
        )
        or ""
    ).strip()
    checkpoint_fp = run_mod.compute_memo_key_for_node(
        next(node for node in graph["nodes"] if node.get("id") == "tool_mid"),
        [source_artifact],
    )
    source_checkpoint_fp = run_mod.compute_memo_key_for_node(
        next(node for node in graph["nodes"] if node.get("id") == "source_1"),
        [],
    )
    assert isinstance(checkpoint_fp, str) and len(checkpoint_fp) == 64
    assert isinstance(source_checkpoint_fp, str) and len(source_checkpoint_fp) == 64

    graph_missing = _graph()
    graph_missing["nodes"][1]["data"]["params"]["extra"] = "recompute"
    graph_missing["__executionHints"] = {
        "checkpoints": {
            "source_1": {
                "artifactId": source_artifact,
                "execKey": "trusted-checkpoint-source",
                "fingerprintAtCreation": source_checkpoint_fp,
            },
            "tool_mid": {
                "artifactId": "missing-artifact-id",
                "execKey": "trusted-checkpoint-mid",
                "fingerprintAtCreation": checkpoint_fp,
            }
        }
    }
    events = []
    await run_mod.run_graph(
        run_id="run-checkpoint-missing-recompute",
        graph=graph_missing,
        run_from="tool_end",
        run_mode="from_selected_onward",
        bus=RunEventBus("run-checkpoint-missing-recompute", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-checkpoint-missing",
    )

    assert calls["tool_mid"] == 2
    run_finished = [e for e in events if e.get("type") == "run_finished"]
    assert run_finished
    assert (run_finished[-1].get("checkpoint_outcomes") or {}).get("tool_mid") == "artifact_missing"
