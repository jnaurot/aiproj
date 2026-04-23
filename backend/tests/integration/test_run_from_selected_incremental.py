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
async def test_node_output_emits_exec_key_for_compute_and_cache_hit(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"text": "hello"},
        )

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
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
        run_id="run-output-exec-key-baseline",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-output-exec-key-baseline", on_emit=lambda e: events_1.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-output-exec-key",
    )
    outputs_1 = [e for e in events_1 if e.get("type") == "node_output"]
    assert outputs_1
    for evt in outputs_1:
        exec_key = str(evt.get("execKey") or "").strip()
        artifact_id = str(evt.get("artifactId") or "").strip()
        assert exec_key, f"missing node_output.execKey for node {evt.get('nodeId')}"
        assert artifact_id
        assert exec_key == artifact_id

    events_2 = []
    await run_mod.run_graph(
        run_id="run-output-exec-key-cache-hit",
        graph=graph,
        run_from="tool_mid",
        run_mode="from_selected_onward",
        bus=RunEventBus("run-output-exec-key-cache-hit", on_emit=lambda e: events_2.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-output-exec-key",
    )
    outputs_2 = [e for e in events_2 if e.get("type") == "node_output"]
    assert outputs_2
    for evt in outputs_2:
        exec_key = str(evt.get("execKey") or "").strip()
        artifact_id = str(evt.get("artifactId") or "").strip()
        assert exec_key, f"missing node_output.execKey for node {evt.get('nodeId')}"
        assert artifact_id
        assert exec_key == artifact_id


@pytest.mark.asyncio
async def test_run_graph_runtime_does_not_depend_on_execution_cache_index(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")

    class _RaisingCache:
        async def get_artifact_id(self, execution_key: str):
            raise AssertionError("run_graph should not call execution cache index lookup")

        async def store_artifact_id(self, execution_key: str, artifact_id: str):
            raise AssertionError("run_graph should not call execution cache index store")

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"text": "hello"},
        )

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
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
    graph = _graph()
    raising_cache = _RaisingCache()

    await run_mod.run_graph(
        run_id="run-no-cache-index-baseline",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-no-cache-index-baseline"),
        artifact_store=store,
        cache=raising_cache,
        graph_id="graph-no-cache-index",
    )

    # Cache-hit run should also avoid execution-cache index calls.
    await run_mod.run_graph(
        run_id="run-no-cache-index-hit",
        graph=graph,
        run_from="tool_mid",
        run_mode="from_selected_onward",
        bus=RunEventBus("run-no-cache-index-hit"),
        artifact_store=store,
        cache=raising_cache,
        graph_id="graph-no-cache-index",
    )


@pytest.mark.asyncio
async def test_cache_hit_emits_started_before_finished_for_reuse(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"text": "hello"},
        )

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
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

    # Prime cache/artifacts.
    await run_mod.run_graph(
        run_id="run-cache-hit-order-baseline",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-cache-hit-order-baseline"),
        artifact_store=store,
        cache=cache,
        graph_id="graph-cache-hit-order",
    )

    # Trigger cache-hit reuse path.
    events_2 = []
    await run_mod.run_graph(
        run_id="run-cache-hit-order-reuse",
        graph=graph,
        run_from="tool_mid",
        run_mode="from_selected_onward",
        bus=RunEventBus("run-cache-hit-order-reuse", on_emit=lambda e: events_2.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-cache-hit-order",
    )

    for node_id in ("source_1", "tool_mid", "tool_end"):
        node_events = [e for e in events_2 if e.get("nodeId") == node_id]
        started_idx = next((i for i, e in enumerate(node_events) if e.get("type") == "node_started"), -1)
        finished_idx = next((i for i, e in enumerate(node_events) if e.get("type") == "node_finished"), -1)
        assert started_idx >= 0, f"expected node_started for cache-hit node {node_id}"
        assert finished_idx >= 0, f"expected node_finished for cache-hit node {node_id}"
        assert started_idx < finished_idx, f"expected node_started before node_finished for {node_id}"
        assert node_events[finished_idx].get("status") == "succeeded"


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
    # Regression: trusted checkpoint reuse must emit memo.execute_decision so frontend
    # memoState is repopulated after run_started clears planned-node memo state.
    memo_logs = [
        e
        for e in events
        if e.get("type") == "log"
        and e.get("nodeId") == "tool_mid"
        and "[trace][memo.execute_decision]" in str(e.get("message") or "")
    ]
    assert memo_logs, "expected memo.execute_decision trace for checkpoint reuse"
    latest_memo_msg = str(memo_logs[-1].get("message") or "")
    assert "\"decision\": \"reuse\"" in latest_memo_msg
    assert f"\"memoKey\": \"{checkpoint_fp}\"" in latest_memo_msg


@pytest.mark.asyncio
async def test_checkpoint_hint_stale_is_hard_cut_and_reuses_without_upstream_recompute(monkeypatch, tmp_path):
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

    # Hard-cut behavior: stale checkpoint still reuses tool_mid and does not
    # trigger recompute of that node.
    assert calls["tool_mid"] == 1
    run_started = [e for e in events if e.get("type") == "run_started"]
    assert run_started
    planned_node_ids = set(run_started[-1].get("plannedNodeIds") or [])
    assert "tool_mid" in planned_node_ids
    assert "tool_end" in planned_node_ids
    assert "source_1" not in planned_node_ids
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
