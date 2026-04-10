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
async def test_run_from_selected_uses_trusted_pinned_artifact_without_upstream_revalidation(monkeypatch, tmp_path):
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
        run_id="run-full-pin-trust",
        graph=graph,
        run_from=None,
        bus=RunEventBus("run-full-pin-trust", on_emit=lambda e: events_1.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-pin-trust",
    )
    mid_outputs = [e for e in events_1 if e.get("type") == "node_output" and e.get("nodeId") == "tool_mid"]
    assert mid_outputs
    mid_artifact_id = str(mid_outputs[-1].get("artifactId") or "").strip()
    assert mid_artifact_id
    assert calls["source"] == 1
    assert calls["tool"] == 2

    graph_2 = _graph()
    graph_2["nodes"][0]["data"]["params"]["file_path"] = "different.txt"
    graph_2["__executionHints"] = {
        "pinnedNodeIds": ["tool_mid"],
        "pinnedArtifacts": {
            "tool_mid": {"artifactId": mid_artifact_id, "execKey": "trusted-mid"}
        },
    }

    events_2 = []
    await run_mod.run_graph(
        run_id="run-from-end-with-pin",
        graph=graph_2,
        run_from="tool_end",
        run_mode="from_selected_onward",
        bus=RunEventBus("run-from-end-with-pin", on_emit=lambda e: events_2.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-pin-trust",
    )

    # Source is outside pinned checkpoint boundary and must not be revalidated/executed.
    assert calls["source"] == 1
    cache_events_mid = [e for e in events_2 if e.get("type") == "cache_decision" and e.get("nodeId") == "tool_mid"]
    assert cache_events_mid
    assert cache_events_mid[-1].get("decision") == "cache_hit"
    assert cache_events_mid[-1].get("reason") == "PINNED_TRUSTED_ARTIFACT"
    assert not [
        e for e in events_2
        if e.get("type") == "cache_decision"
        and e.get("nodeId") == "tool_mid"
        and e.get("decision") == "cache_miss"
    ]
    trace_messages = [
        str(e.get("message") or "")
        for e in events_2
        if e.get("type") == "log"
    ]
    assert any("[trace][pin.backend_parse]" in msg for msg in trace_messages)
    assert any("[trace][pin.plan]" in msg for msg in trace_messages)
    execute_msgs = [
        msg for msg in trace_messages
        if "[trace][pin.execute_decision]" in msg and '"nodeId": "tool_mid"' in msg
    ]
    assert len(execute_msgs) == 1
    assert "PIN_TRUSTED_ARTIFACT_PRESENT" in execute_msgs[0]
    run_finished = [e for e in events_2 if e.get("type") == "run_finished"]
    assert run_finished and run_finished[-1].get("status") == "succeeded"


@pytest.mark.asyncio
async def test_full_run_pinned_node_reuses_artifact_and_skips_recompute(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    source_calls = {"count": 0}
    tool_calls = {"tool_mid": 0, "tool_end": 0}

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        source_calls["count"] += 1
        file_path = str((((node or {}).get("data") or {}).get("params") or {}).get("file_path") or "")
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"text": file_path},
        )

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        node_id = str((node or {}).get("id") or "")
        if node_id in tool_calls:
            tool_calls[node_id] += 1
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={
                "kind": "json",
                "payload": {"node": node_id, "upstream": sorted(list(upstream_artifact_ids or []))},
                "meta": {"status": "ok"},
            },
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

    artifact_root = tmp_path / "artifacts"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

    graph_1 = _graph()
    events_1 = []
    await run_mod.run_graph(
        run_id="run-full-pin-baseline",
        graph=graph_1,
        run_from=None,
        bus=RunEventBus("run-full-pin-baseline", on_emit=lambda e: events_1.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-full-pin",
    )
    mid_output_1 = [e for e in events_1 if e.get("type") == "node_output" and e.get("nodeId") == "tool_mid"]
    assert mid_output_1
    mid_artifact_id = str(mid_output_1[-1].get("artifactId") or "").strip()
    assert mid_artifact_id
    assert source_calls["count"] == 1
    assert tool_calls["tool_mid"] == 1
    assert tool_calls["tool_end"] == 1

    graph_2 = _graph()
    graph_2["nodes"][0]["data"]["params"]["file_path"] = "changed-source.txt"
    graph_2["__executionHints"] = {
        "pinnedNodeIds": ["tool_mid"],
        "pinnedArtifacts": {
            "tool_mid": {"artifactId": mid_artifact_id, "execKey": "trusted-mid-full-run"}
        },
    }
    events_2 = []
    await run_mod.run_graph(
        run_id="run-full-pin-reuse",
        graph=graph_2,
        run_from=None,
        bus=RunEventBus("run-full-pin-reuse", on_emit=lambda e: events_2.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-full-pin",
    )

    mid_cache_events = [e for e in events_2 if e.get("type") == "cache_decision" and e.get("nodeId") == "tool_mid"]
    assert mid_cache_events
    assert mid_cache_events[-1].get("decision") == "cache_hit"
    assert mid_cache_events[-1].get("reason") == "PINNED_TRUSTED_ARTIFACT"
    assert source_calls["count"] == 2
    assert tool_calls["tool_mid"] == 1
    run_finished = [e for e in events_2 if e.get("type") == "run_finished"]
    assert run_finished and run_finished[-1].get("status") == "succeeded"


@pytest.mark.asyncio
async def test_downstream_cache_identity_changes_with_pinned_artifact(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    tool_calls = {"tool_mid": 0, "tool_end": 0}

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        file_path = str((((node or {}).get("data") or {}).get("params") or {}).get("file_path") or "")
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"text": file_path},
        )

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        node_id = str((node or {}).get("id") or "")
        if node_id in tool_calls:
            tool_calls[node_id] += 1
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={
                "kind": "json",
                "payload": {"node": node_id, "upstream": sorted(list(upstream_artifact_ids or []))},
                "meta": {"status": "ok"},
            },
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

    artifact_root = tmp_path / "artifacts"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

    graph_a = _graph()
    graph_a["nodes"][0]["data"]["params"]["file_path"] = "profile-A.txt"
    events_a = []
    await run_mod.run_graph(
        run_id="run-pin-determinism-A",
        graph=graph_a,
        run_from=None,
        bus=RunEventBus("run-pin-determinism-A", on_emit=lambda e: events_a.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-pin-determinism",
    )
    mid_a = str(next(e for e in events_a if e.get("type") == "node_output" and e.get("nodeId") == "tool_mid").get("artifactId") or "").strip()
    end_a = str(next(e for e in events_a if e.get("type") == "node_output" and e.get("nodeId") == "tool_end").get("artifactId") or "").strip()
    assert mid_a and end_a

    graph_b = _graph()
    graph_b["nodes"][0]["data"]["params"]["file_path"] = "profile-B.txt"
    events_b = []
    await run_mod.run_graph(
        run_id="run-pin-determinism-B",
        graph=graph_b,
        run_from=None,
        bus=RunEventBus("run-pin-determinism-B", on_emit=lambda e: events_b.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-pin-determinism",
    )
    mid_b = str(next(e for e in events_b if e.get("type") == "node_output" and e.get("nodeId") == "tool_mid").get("artifactId") or "").strip()
    end_b = str(next(e for e in events_b if e.get("type") == "node_output" and e.get("nodeId") == "tool_end").get("artifactId") or "").strip()
    assert mid_b and end_b
    assert mid_a != mid_b
    assert end_a != end_b

    graph_resume = _graph()
    graph_resume["nodes"][0]["data"]["params"]["file_path"] = "profile-B.txt"
    graph_resume["__executionHints"] = {
        "pinnedNodeIds": ["tool_mid"],
        "pinnedArtifacts": {
            "tool_mid": {"artifactId": mid_a, "execKey": "trusted-mid-A"}
        },
    }
    events_resume = []
    await run_mod.run_graph(
        run_id="run-pin-determinism-resume",
        graph=graph_resume,
        run_from="tool_end",
        run_mode="from_selected_onward",
        bus=RunEventBus("run-pin-determinism-resume", on_emit=lambda e: events_resume.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-pin-determinism",
    )

    mid_cache_events = [e for e in events_resume if e.get("type") == "cache_decision" and e.get("nodeId") == "tool_mid"]
    assert mid_cache_events and mid_cache_events[-1].get("reason") == "PINNED_TRUSTED_ARTIFACT"
    end_output_events = [e for e in events_resume if e.get("type") == "node_output" and e.get("nodeId") == "tool_end"]
    assert end_output_events
    end_resumed = str(end_output_events[-1].get("artifactId") or "").strip()
    assert end_resumed == end_a
    assert end_resumed != end_b


@pytest.mark.asyncio
async def test_pinned_component_reuses_boundary_artifact_for_downstream(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    calls = {"source": 0, "tool": 0}
    observed_upstream_for_end: list[list[str]] = []

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
        node_id = str((node or {}).get("id") or "")
        upstream = sorted([str(a) for a in (upstream_artifact_ids or []) if str(a).strip()])
        if node_id == "tool_end":
            observed_upstream_for_end.append(upstream)
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True, "node": node_id}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

    artifact_root = tmp_path / "artifacts"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

    # Seed a reusable artifact id from a normal execution path.
    seed_events = []
    await run_mod.run_graph(
        run_id="run-seed-component-pin",
        graph=_graph(),
        run_from=None,
        bus=RunEventBus("run-seed-component-pin", on_emit=lambda e: seed_events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-component-pin",
    )
    seed_mid = [
        e for e in seed_events if e.get("type") == "node_output" and e.get("nodeId") == "tool_mid"
    ]
    assert seed_mid
    pinned_artifact_id = str(seed_mid[-1].get("artifactId") or "").strip()
    assert pinned_artifact_id
    assert calls["source"] == 1
    assert calls["tool"] == 2

    graph_component_pin = {
        "nodes": [
            {
                "id": "source_1",
                "data": {
                    "kind": "source",
                    "label": "Source",
                    "sourceKind": "file",
                    "params": {"file_path": "changed.txt", "file_format": "txt"},
                },
            },
            {
                "id": "component_wrap",
                "data": {
                    "kind": "component",
                    "label": "Component",
                    "params": {
                        "componentRef": {"componentId": "cmp_test", "revisionId": "crev_test", "apiVersion": "v1"},
                        "api": {"outputs": [{"name": "out", "required": True, "typedSchema": {"type": "json"}}]},
                    },
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
            {"id": "e_src_component", "source": "source_1", "target": "component_wrap", "targetHandle": "in"},
            {"id": "e_component_end", "source": "component_wrap", "sourceHandle": "out", "target": "tool_end"},
        ],
        "__executionHints": {
            "pinnedNodeIds": ["component_wrap"],
            "pinnedArtifacts": {
                "component_wrap": {"artifactId": pinned_artifact_id, "execKey": "trusted-component-pin"}
            },
        },
    }
    pin_events = []
    await run_mod.run_graph(
        run_id="run-component-pinned",
        graph=graph_component_pin,
        run_from="tool_end",
        run_mode="from_selected_onward",
        bus=RunEventBus("run-component-pinned", on_emit=lambda e: pin_events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-component-pin",
    )

    # Pinned component boundary artifact is reused; upstream source remains outside the selected plan.
    assert calls["source"] == 1
    assert observed_upstream_for_end
    assert observed_upstream_for_end[-1] == [pinned_artifact_id]


@pytest.mark.asyncio
async def test_run_from_selected_respects_pinned_internal_component_node_from_graph_meta(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    calls = {"tool": 0}
    calls_by_node: dict[str, int] = {}
    observed_upstream_for_end: list[list[str]] = []

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        calls["tool"] += 1
        node_id = str((node or {}).get("id") or "")
        calls_by_node[node_id] = int(calls_by_node.get(node_id, 0)) + 1
        if node_id == "tool_end":
            observed_upstream_for_end.append(
                sorted([str(a) for a in (upstream_artifact_ids or []) if str(a).strip()])
            )
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True, "node": node_id}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

    artifact_root = tmp_path / "artifacts"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

    internal_node_id = "cmp:component_wrap:n_summary"
    internal_seed_graph = {
        "nodes": [
            {
                "id": internal_node_id,
                "data": {
                    "kind": "tool",
                    "label": "Internal Summary",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                },
            }
        ],
        "edges": [],
    }
    seed_events = []
    await run_mod.run_graph(
        run_id="run-seed-internal-pin",
        graph=internal_seed_graph,
        run_from=None,
        bus=RunEventBus("run-seed-internal-pin", on_emit=lambda e: seed_events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-internal-pin",
    )
    internal_seed_outputs = [
        e for e in seed_events if e.get("type") == "node_output" and e.get("nodeId") == internal_node_id
    ]
    assert internal_seed_outputs
    internal_artifact_id = str(internal_seed_outputs[-1].get("artifactId") or "").strip()
    assert internal_artifact_id
    assert calls["tool"] == 1

    run_id = "run-from-selected-internal-pin"
    component_revision = types.SimpleNamespace(
        definition={
            "graph": {
                "nodes": [
                    {
                        "id": "n_summary",
                        "data": {
                            "kind": "tool",
                            "label": "Internal Summary",
                            "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                            "meta": {"freeze": {"enabled": True, "mode": "sticky"}},
                        },
                    }
                ],
                "edges": [],
            }
        }
    )

    class _ComponentStore:
        def get_revision(self, component_id, revision_id):
            if str(component_id) == "cmp_test" and str(revision_id) == "crev_test":
                return component_revision
            return None

    runtime_ref = types.SimpleNamespace(
        component_revisions=_ComponentStore(),
        runs={
            run_id: types.SimpleNamespace(
                node_bindings={
                    internal_node_id: {
                        "status": "succeeded",
                        "current": {"artifactId": internal_artifact_id, "execKey": "trusted-internal-pin"},
                        "outputLineage": {
                            "out": {"artifactId": internal_artifact_id, "execKey": "trusted-internal-pin"}
                        },
                    }
                }
            )
        },
    )

    graph = {
        "nodes": [
            {
                "id": "component_wrap",
                "data": {
                    "kind": "component",
                    "label": "Component",
                    "params": {
                        "componentRef": {"componentId": "cmp_test", "revisionId": "crev_test", "apiVersion": "v1"},
                        "api": {
                            "outputs": [{"name": "summary", "required": True, "typedSchema": {"type": "json"}}]
                        },
                        "published_profile": [
                            {
                                "kind": "data_output",
                                "alias": "summary",
                                "handle_id": "data_out::summary",
                                "internal_source_path": "n_summary.out",
                            }
                        ],
                    },
                },
            },
            {
                "id": "tool_end",
                "data": {
                    "kind": "tool",
                    "label": "Downstream",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                },
            },
        ],
        "edges": [
            {
                "id": "e_component_end",
                "source": "component_wrap",
                "sourceHandle": "summary",
                "target": "tool_end",
                "targetHandle": "in",
            },
        ],
    }

    events = []
    await run_mod.run_graph(
        run_id=run_id,
        graph=graph,
        run_from="tool_end",
        run_mode="from_selected_onward",
        bus=RunEventBus(run_id, on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-internal-pin",
        runtime_ref=runtime_ref,
    )

    # Seed run executes internal once; from-selected run must not re-execute pinned internal node.
    assert calls_by_node.get(internal_node_id, 0) == 1
    if observed_upstream_for_end:
        assert observed_upstream_for_end[-1] == [internal_artifact_id]

    trace_messages = [str(e.get("message") or "") for e in events if e.get("type") == "log"]
    backend_parse_with_graph_pin = any(
        "[trace][pin.backend_parse]" in msg
        and "graphPinnedNodeIdsParsed" in msg
        and internal_node_id in msg
        for msg in trace_messages
    )
    assert backend_parse_with_graph_pin, trace_messages
    execute_reuse_trace = any(
        "[trace][pin.execute_decision]" in msg
        and '"nodeId": "cmp:component_wrap:n_summary"' in msg
        and '"decision": "reuse"' in msg
        and "PIN_TRUSTED_ARTIFACT_PRESENT" in msg
        for msg in trace_messages
    )
    assert execute_reuse_trace, trace_messages
    run_finished = [e for e in events if e.get("type") == "run_finished"]
    assert run_finished and run_finished[-1].get("status") == "succeeded"


@pytest.mark.asyncio
async def test_run_from_selected_derives_nested_internal_pin_hints_from_component_lineage(monkeypatch, tmp_path):
    run_mod = importlib.import_module("app.runner.run")
    calls_by_node: dict[str, int] = {}

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        node_id = str((node or {}).get("id") or "")
        calls_by_node[node_id] = int(calls_by_node.get(node_id, 0)) + 1
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True, "node": node_id}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)

    artifact_root = tmp_path / "artifacts"
    store = DiskArtifactStore(artifact_root)
    cache = SqliteExecutionCache(str(artifact_root / "meta" / "artifacts.sqlite"))

    nested_internal_runtime_id = "cmp:component_wrap:cmp:inner_node:n_leaf"
    seed_events = []
    await run_mod.run_graph(
        run_id="run-seed-nested-internal-pin",
        graph={
            "nodes": [
                {
                    "id": nested_internal_runtime_id,
                    "data": {
                        "kind": "tool",
                        "label": "Nested Internal Leaf",
                        "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                    },
                }
            ],
            "edges": [],
        },
        run_from=None,
        bus=RunEventBus("run-seed-nested-internal-pin", on_emit=lambda e: seed_events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-nested-internal-pin",
    )
    seeded = [
        e for e in seed_events if e.get("type") == "node_output" and e.get("nodeId") == nested_internal_runtime_id
    ]
    assert seeded
    nested_artifact_id = str(seeded[-1].get("artifactId") or "").strip()
    assert nested_artifact_id
    assert calls_by_node.get(nested_internal_runtime_id, 0) == 1

    component_revision_inner = types.SimpleNamespace(
        definition={
            "graph": {
                "nodes": [
                    {
                        "id": "n_leaf",
                        "data": {
                            "kind": "tool",
                            "label": "Inner Leaf",
                            "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                            "meta": {
                                "freeze": {"enabled": True, "mode": "sticky"},
                                "freezeLineage": {
                                    "artifactId": nested_artifact_id,
                                    "execKey": "trusted-nested-pin",
                                    "outputs": {
                                        "out": {
                                            "artifactId": nested_artifact_id,
                                            "execKey": "trusted-nested-pin",
                                        }
                                    },
                                },
                            },
                        },
                    }
                ],
                "edges": [],
            }
        }
    )

    component_revision_outer = types.SimpleNamespace(
        definition={
            "graph": {
                "nodes": [
                    {
                        "id": "inner_node",
                        "data": {
                            "kind": "component",
                            "label": "Inner Component Node",
                            "params": {
                                "componentRef": {
                                    "componentId": "cmp_inner",
                                    "revisionId": "crev_inner",
                                    "apiVersion": "v1",
                                },
                                "api": {
                                    "outputs": [
                                        {"name": "summary", "required": True, "typedSchema": {"type": "json"}}
                                    ]
                                },
                                "published_profile": [
                                    {
                                        "kind": "data_output",
                                        "alias": "summary",
                                        "handle_id": "data_out::summary",
                                        "internal_source_path": "n_leaf.out",
                                    }
                                ],
                            },
                        },
                    }
                ],
                "edges": [],
            }
        }
    )

    class _ComponentStore:
        def get_revision(self, component_id, revision_id):
            component_id = str(component_id)
            revision_id = str(revision_id)
            if component_id == "cmp_outer" and revision_id == "crev_outer":
                return component_revision_outer
            if component_id == "cmp_inner" and revision_id == "crev_inner":
                return component_revision_inner
            return None

    graph = {
        "nodes": [
            {
                "id": "component_wrap",
                "data": {
                    "kind": "component",
                    "label": "Outer Component",
                    "params": {
                        "componentRef": {"componentId": "cmp_outer", "revisionId": "crev_outer", "apiVersion": "v1"},
                        "api": {"outputs": [{"name": "summary", "required": True, "typedSchema": {"type": "json"}}]},
                        "published_profile": [
                            {
                                "kind": "data_output",
                                "alias": "summary",
                                "handle_id": "data_out::summary",
                                "internal_source_path": "inner_node.summary",
                            }
                        ],
                    },
                },
            },
            {
                "id": "tool_end",
                "data": {
                    "kind": "tool",
                    "label": "Downstream",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                },
            },
        ],
        "edges": [
            {
                "id": "e_component_end",
                "source": "component_wrap",
                "sourceHandle": "summary",
                "target": "tool_end",
                "targetHandle": "in",
            }
        ],
    }

    events = []
    await run_mod.run_graph(
        run_id="run-from-selected-nested-internal-pin",
        graph=graph,
        run_from="tool_end",
        run_mode="from_selected_onward",
        bus=RunEventBus("run-from-selected-nested-internal-pin", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-nested-internal-pin",
        runtime_ref=types.SimpleNamespace(component_revisions=_ComponentStore(), runs={}),
    )

    assert calls_by_node.get(nested_internal_runtime_id, 0) == 1

    trace_messages = [str(e.get("message") or "") for e in events if e.get("type") == "log"]
    assert any(
        "[trace][pin.backend_parse]" in msg
        and "graphPinnedDerivedLineageNodeIds" in msg
        and nested_internal_runtime_id in msg
        for msg in trace_messages
    ), trace_messages
    assert any(
        "[trace][pin.execute_decision]" in msg
        and f'"nodeId": "{nested_internal_runtime_id}"' in msg
        and '"decision": "reuse"' in msg
        and "PIN_TRUSTED_ARTIFACT_PRESENT" in msg
        for msg in trace_messages
    ), trace_messages
    run_finished = [e for e in events if e.get("type") == "run_finished"]
    assert run_finished and run_finished[-1].get("status") == "succeeded"


@pytest.mark.asyncio
async def test_run_from_selected_pinned_artifact_missing_emits_pin_execute_fail_trace(monkeypatch, tmp_path):
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
                "artifactId": "missing-artifact-id",
                "execKey": "trusted-mid-missing",
            }
        },
    }

    events = []
    await run_mod.run_graph(
        run_id="run-from-end-pin-missing-artifact",
        graph=graph,
        run_from="tool_end",
        run_mode="from_selected_onward",
        bus=RunEventBus("run-from-end-pin-missing-artifact", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-pin-missing-artifact",
    )

    # Pinned ancestor should fail in trusted-pin branch before normal execution.
    assert calls["source"] == 0

    trace_messages = [
        str(e.get("message") or "")
        for e in events
        if e.get("type") == "log"
    ]
    assert any("[trace][pin.backend_parse]" in msg for msg in trace_messages)
    assert any("[trace][pin.plan]" in msg for msg in trace_messages)
    assert any(
        "[trace][pin.execute_decision]" in msg and "PIN_TRUSTED_ARTIFACT_MISSING_IN_STORE" in msg
        for msg in trace_messages
    )

    run_finished = [e for e in events if e.get("type") == "run_finished"]
    assert run_finished and run_finished[-1].get("status") == "failed"


@pytest.mark.asyncio
async def test_run_from_selected_pin_plan_marks_not_in_subgraph_reason(monkeypatch, tmp_path):
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
    graph["__executionHints"] = {
        "pinnedNodeIds": ["ghost_node"],
        "pinnedArtifacts": {
            "ghost_node": {
                "artifactId": "ghost-artifact",
                "execKey": "ghost-exec",
            }
        },
    }
    events = []
    await run_mod.run_graph(
        run_id="run-pin-not-in-subgraph",
        graph=graph,
        run_from="tool_end",
        run_mode="from_selected_onward",
        bus=RunEventBus("run-pin-not-in-subgraph", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-pin-not-in-subgraph",
    )

    trace_messages = [
        str(e.get("message") or "")
        for e in events
        if e.get("type") == "log"
    ]
    assert any("[trace][pin.plan]" in msg and "PIN_HINT_NOT_IN_SUBGRAPH" in msg for msg in trace_messages)


@pytest.mark.asyncio
async def test_run_from_selected_cache_only_without_trusted_pin_emits_fallback_recompute(monkeypatch, tmp_path):
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
    graph["__executionHints"] = {
        "pinnedNodeIds": ["tool_mid"],
        "pinnedArtifacts": {
            # Missing execKey forces parse rejection for trusted pin payload.
            "tool_mid": {
                "artifactId": "some-artifact",
            }
        },
    }

    events = []
    await run_mod.run_graph(
        run_id="run-pin-fallback-recompute",
        graph=graph,
        run_from="tool_end",
        run_mode="from_selected_onward",
        bus=RunEventBus("run-pin-fallback-recompute", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        graph_id="graph-pin-fallback-recompute",
    )

    trace_messages = [
        str(e.get("message") or "")
        for e in events
        if e.get("type") == "log"
    ]
    assert any(
        "[trace][pin.execute_decision]" in msg and "PIN_FALLBACK_RECOMPUTE" in msg and '"nodeId": "tool_mid"' in msg
        for msg in trace_messages
    )


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
