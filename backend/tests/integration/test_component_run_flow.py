import importlib
import json
import sys
import types
from types import SimpleNamespace

import pytest

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.artifacts import MemoryArtifactStore
from app.runner.cache import ExecutionCache
from app.runner.events import RunEventBus
from app.runner.metadata import NodeOutput


class _ComponentStoreStub:
    def __init__(self, revisions):
        self._revisions = revisions

    def get_revision(self, component_id: str, revision_id: str):
        return self._revisions.get((component_id, revision_id))


def _component_definition():
    return {
        "graph": {
            "nodes": [
                {
                    "id": "inner_tool",
                    "data": {
                        "kind": "tool",
                        "label": "Inner Tool",
                        "params": {"provider": "builtin", "builtin": {"toolId": "echo", "args": {}}},
                        "ports": {"in": "text", "out": "json"},
                    },
                }
            ],
            "edges": [],
        },
        "api": {"inputs": [{"name": "in"}], "outputs": [{"name": "out", "portType": "json"}]},
    }


def _multi_output_component_definition():
    return {
        "graph": {
            "nodes": [
                {
                    "id": "inner_summary",
                    "data": {
                        "kind": "tool",
                        "label": "Inner Summary",
                        "params": {"provider": "builtin", "builtin": {"toolId": "echo", "args": {}}},
                        "ports": {"in": "text", "out": "text"},
                    },
                },
                {
                    "id": "inner_source",
                    "data": {
                        "kind": "tool",
                        "label": "Inner Source",
                        "params": {"provider": "builtin", "builtin": {"toolId": "echo", "args": {}}},
                        "ports": {"in": "text", "out": "text"},
                    },
                },
            ],
            "edges": [],
        },
        "api": {
            "inputs": [],
            "outputs": [
                {"name": "summary", "portType": "text", "required": True, "typedSchema": {"type": "text", "fields": []}},
                {"name": "source", "portType": "text", "required": True, "typedSchema": {"type": "text", "fields": []}},
            ],
        },
    }


def _graph(component_revision_id: str, *, output_artifact_mode: str = "current"):
    return {
        "nodes": [
            {
                "id": "src",
                "data": {
                    "kind": "source",
                    "label": "Source",
                    "sourceKind": "file",
                    "params": {"snapshot_id": "a" * 64, "file_format": "txt", "output_mode": "text"},
                    "ports": {"in": None, "out": "text"},
                },
            },
            {
                "id": "cmp_node",
                "data": {
                    "kind": "component",
                    "label": "Component",
                    "params": {
                        "componentRef": {"componentId": "cmp_echo", "revisionId": component_revision_id},
                        "bindings": {
                            "inputs": {},
                            "outputs": {"out": {"nodeId": "inner_tool", "artifact": output_artifact_mode}},
                            "config": {},
                        },
                        "config": {"mode": "default"},
                    },
                    "ports": {"in": "text", "out": "json"},
                },
            },
        ],
        "edges": [{"id": "e1", "source": "src", "target": "cmp_node"}],
    }


def _graph_with_named_component_edges(
    component_revision_id: str,
    *,
    source_binding_node_id: str = "inner_source",
    source_artifact_mode: str = "current",
    source_edge_handle: str = "source",
):
    return {
        "nodes": [
            {
                "id": "src",
                "data": {
                    "kind": "source",
                    "label": "Source",
                    "sourceKind": "file",
                    "params": {"snapshot_id": "a" * 64, "file_format": "txt", "output_mode": "text"},
                    "ports": {"in": None, "out": "text"},
                },
            },
            {
                "id": "cmp_node",
                "data": {
                    "kind": "component",
                    "label": "Component",
                    "params": {
                        "componentRef": {"componentId": "cmp_multi_named", "revisionId": component_revision_id},
                        "api": {
                            "inputs": [],
                            "outputs": [
                                {"name": "summary", "portType": "text", "required": True, "typedSchema": {"type": "text", "fields": []}},
                                {"name": "source", "portType": "text", "required": True, "typedSchema": {"type": "text", "fields": []}},
                            ],
                        },
                        "bindings": {
                            "inputs": {},
                            "outputs": {
                                "summary": {"nodeId": "inner_summary", "artifact": "current"},
                                "source": {"nodeId": source_binding_node_id, "artifact": source_artifact_mode},
                            },
                            "config": {},
                        },
                        "config": {},
                    },
                    "ports": {"in": "text", "out": "json"},
                },
            },
            {
                "id": "llm_summary",
                "data": {
                    "kind": "llm",
                    "label": "LLM Summary",
                    "llmKind": "ollama",
                    "params": {"baseUrl": "http://localhost:11434", "model": "x", "user_prompt": "summarize"},
                    "ports": {"in": "text", "out": "text"},
                },
            },
            {
                "id": "llm_source",
                "data": {
                    "kind": "llm",
                    "label": "LLM Source",
                    "llmKind": "ollama",
                    "params": {"baseUrl": "http://localhost:11434", "model": "x", "user_prompt": "translate"},
                    "ports": {"in": "text", "out": "text"},
                },
            },
        ],
        "edges": [
            {"id": "e_src_cmp", "source": "src", "target": "cmp_node"},
            {
                "id": "e_cmp_summary",
                "source": "cmp_node",
                "sourceHandle": "summary",
                "target": "llm_summary",
                "targetHandle": "in",
            },
            {
                "id": "e_cmp_source",
                "source": "cmp_node",
                "sourceHandle": source_edge_handle,
                "target": "llm_source",
                "targetHandle": "in",
            },
        ],
    }


@pytest.mark.asyncio
async def test_component_run_emits_parent_lifecycle_and_artifact_component_meta(monkeypatch):
    run_mod = importlib.import_module("app.runner.run")
    store = MemoryArtifactStore()
    cache = ExecutionCache()
    events = []

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="hello")

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True, "node": node["id"]}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
    runtime_ref = SimpleNamespace(component_revisions=_ComponentStoreStub({("cmp_echo", "rev_1"): SimpleNamespace(definition=_component_definition())}))

    await run_mod.run_graph(
        run_id="run-component-1",
        graph=_graph("rev_1"),
        run_from=None,
        bus=RunEventBus("run-component-1", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        runtime_ref=runtime_ref,
        graph_id="graph-component",
    )

    assert any(e.get("type") == "component_started" and e.get("nodeId") == "cmp_node" for e in events)
    assert any(e.get("type") == "component_finished" and e.get("nodeId") == "cmp_node" for e in events)
    assert any(
        e.get("type") == "node_finished" and e.get("nodeId") == "cmp_node" and e.get("status") == "succeeded"
        for e in events
    )

    internal_output = next(
        e for e in events if e.get("type") == "node_output" and str(e.get("nodeId", "")).startswith("cmp:cmp_node:")
    )
    parent_output = next(
        e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "cmp_node"
    )
    assert str(parent_output.get("artifactId") or "") != str(internal_output.get("artifactId") or "")
    art = await store.get(str(parent_output["artifactId"]))
    wrapper_bytes = await store.read(str(art.artifact_id))
    wrapper_data = json.loads(wrapper_bytes.decode("utf-8")) if isinstance(wrapper_bytes, (bytes, bytearray)) else {}
    payload = (
        wrapper_data
        if isinstance(wrapper_data.get("component"), dict)
        else (
            wrapper_data.get("payload")
            if isinstance(wrapper_data.get("payload"), dict)
            else {}
        )
    )
    wrapper_component = payload.get("component") if isinstance(payload.get("component"), dict) else {}
    assert wrapper_component.get("instanceNodeId") == "cmp_node"
    assert wrapper_component.get("componentId") == "cmp_echo"
    assert wrapper_component.get("revisionId") == "rev_1"
    outputs = payload.get("outputs") if isinstance(payload.get("outputs"), dict) else {}
    out_binding = outputs.get("out") if isinstance(outputs.get("out"), dict) else {}
    assert str(out_binding.get("artifact_id") or "") == str(internal_output.get("artifactId") or "")


@pytest.mark.asyncio
async def test_component_revision_change_busts_internal_exec_key(monkeypatch):
    run_mod = importlib.import_module("app.runner.run")
    store = MemoryArtifactStore()
    cache = ExecutionCache()
    output_artifact_ids = []

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
    runtime_ref = SimpleNamespace(
        component_revisions=_ComponentStoreStub(
            {
                ("cmp_echo", "rev_1"): SimpleNamespace(definition=_component_definition()),
                ("cmp_echo", "rev_2"): SimpleNamespace(definition=_component_definition()),
            }
        )
    )

    for idx, rev in enumerate(["rev_1", "rev_2"], start=1):
        events = []
        await run_mod.run_graph(
            run_id=f"run-component-rev-{idx}",
            graph=_graph(rev),
            run_from=None,
            bus=RunEventBus(f"run-component-rev-{idx}", on_emit=lambda e: events.append(dict(e))),
            artifact_store=store,
            cache=cache,
            runtime_ref=runtime_ref,
            graph_id="graph-component-revision",
        )
        internal_output = next(
            e for e in events if e.get("type") == "node_output" and str(e.get("nodeId", "")).startswith("cmp:cmp_node:")
        )
        output_artifact_ids.append(str(internal_output["artifactId"]))

    assert output_artifact_ids[0] != output_artifact_ids[1]


@pytest.mark.asyncio
async def test_component_output_binding_last_resolves_previous_internal_artifact(monkeypatch):
    run_mod = importlib.import_module("app.runner.run")
    store = MemoryArtifactStore()
    cache = ExecutionCache()

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="hello")

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "json", "payload": {"ok": True, "node": node["id"]}, "meta": {"status": "ok"}},
        )

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
    runtime_ref = SimpleNamespace(
        component_revisions=_ComponentStoreStub(
            {
                ("cmp_echo", "rev_1"): SimpleNamespace(definition=_component_definition()),
                ("cmp_echo", "rev_2"): SimpleNamespace(definition=_component_definition()),
            }
        )
    )

    run1_events = []
    await run_mod.run_graph(
        run_id="run-component-last-1",
        graph=_graph("rev_1", output_artifact_mode="current"),
        run_from=None,
        bus=RunEventBus("run-component-last-1", on_emit=lambda e: run1_events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        runtime_ref=runtime_ref,
        graph_id="graph-component-last",
    )
    run1_internal = next(
        e for e in run1_events if e.get("type") == "node_output" and str(e.get("nodeId", "")).startswith("cmp:cmp_node:")
    )
    run1_internal_artifact_id = str(run1_internal.get("artifactId") or "")

    run2_events = []
    await run_mod.run_graph(
        run_id="run-component-last-2",
        graph=_graph("rev_2", output_artifact_mode="last"),
        run_from=None,
        bus=RunEventBus("run-component-last-2", on_emit=lambda e: run2_events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        runtime_ref=runtime_ref,
        graph_id="graph-component-last",
    )
    run2_internal = next(
        e for e in run2_events if e.get("type") == "node_output" and str(e.get("nodeId", "")).startswith("cmp:cmp_node:")
    )
    run2_internal_artifact_id = str(run2_internal.get("artifactId") or "")
    assert run2_internal_artifact_id != run1_internal_artifact_id

    run2_parent = next(
        e for e in run2_events if e.get("type") == "node_output" and e.get("nodeId") == "cmp_node"
    )
    run2_parent_art = await store.get(str(run2_parent.get("artifactId") or ""))
    run2_parent_bytes = await store.read(str(run2_parent_art.artifact_id))
    wrapper_data = json.loads(run2_parent_bytes.decode("utf-8")) if isinstance(run2_parent_bytes, (bytes, bytearray)) else {}
    payload = (
        wrapper_data
        if isinstance(wrapper_data.get("component"), dict)
        else (
            wrapper_data.get("payload")
            if isinstance(wrapper_data.get("payload"), dict)
            else {}
        )
    )
    outputs = payload.get("outputs") if isinstance(payload.get("outputs"), dict) else {}
    out_binding = outputs.get("out") if isinstance(outputs.get("out"), dict) else {}
    assert str(out_binding.get("artifact_id") or "") == run1_internal_artifact_id


@pytest.mark.asyncio
async def test_component_named_output_edges_route_to_bound_internal_artifacts(monkeypatch):
    run_mod = importlib.import_module("app.runner.run")
    store = MemoryArtifactStore()
    cache = ExecutionCache()
    llm_upstreams: dict[str, list[str]] = {}

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="seed")

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "text", "payload": f"payload:{node['id']}", "meta": {"status": "ok"}},
        )

    async def _fake_exec_llm(run_id, node, context, upstream_artifact_ids=None):
        llm_upstreams[str(node.get("id"))] = [str(a) for a in (upstream_artifact_ids or [])]
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="ok")

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
    monkeypatch.setattr(run_mod, "exec_llm", _fake_exec_llm)
    runtime_ref = SimpleNamespace(
        component_revisions=_ComponentStoreStub(
            {("cmp_multi_named", "crev_named"): SimpleNamespace(definition=_multi_output_component_definition())}
        )
    )

    events = []
    await run_mod.run_graph(
        run_id="run-component-named-routing",
        graph=_graph_with_named_component_edges("crev_named"),
        run_from=None,
        bus=RunEventBus("run-component-named-routing", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        runtime_ref=runtime_ref,
        graph_id="graph-component-named-routing",
    )

    component_wrapper_output = next(
        e for e in events if e.get("type") == "node_output" and e.get("nodeId") == "cmp_node"
    )

    summary_upstream = llm_upstreams.get("llm_summary") or []
    source_upstream = llm_upstreams.get("llm_source") or []
    assert len(summary_upstream) == 1
    assert len(source_upstream) == 1
    assert summary_upstream[0] != source_upstream[0]
    assert str(component_wrapper_output.get("artifactId") or "") not in llm_upstreams.get("llm_summary", [])
    assert str(component_wrapper_output.get("artifactId") or "") not in llm_upstreams.get("llm_source", [])
    assert any(e.get("type") == "run_finished" and e.get("status") == "succeeded" for e in events)


@pytest.mark.asyncio
async def test_component_named_output_unresolved_handle_fails_without_wrapper_fallback(monkeypatch):
    run_mod = importlib.import_module("app.runner.run")
    store = MemoryArtifactStore()
    cache = ExecutionCache()

    async def _fake_exec_source(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="seed")

    async def _fake_exec_tool(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(
            status="succeeded",
            metadata=None,
            execution_time_ms=1.0,
            data={"kind": "text", "payload": f"payload:{node['id']}", "meta": {"status": "ok"}},
        )

    async def _fake_exec_llm(run_id, node, context, upstream_artifact_ids=None):
        return NodeOutput(status="succeeded", metadata=None, execution_time_ms=1.0, data="ok")

    monkeypatch.setattr(run_mod, "exec_source", _fake_exec_source)
    monkeypatch.setattr(run_mod, "exec_tool", _fake_exec_tool)
    monkeypatch.setattr(run_mod, "exec_llm", _fake_exec_llm)
    runtime_ref = SimpleNamespace(
        component_revisions=_ComponentStoreStub(
            {("cmp_multi_named", "crev_named"): SimpleNamespace(definition=_multi_output_component_definition())}
        )
    )

    events = []
    await run_mod.run_graph(
        run_id="run-component-named-unresolved",
        graph=_graph_with_named_component_edges("crev_named", source_edge_handle="missing"),
        run_from=None,
        bus=RunEventBus("run-component-named-unresolved", on_emit=lambda e: events.append(dict(e))),
        artifact_store=store,
        cache=cache,
        runtime_ref=runtime_ref,
        graph_id="graph-component-named-unresolved",
    )

    assert any(
        e.get("type") == "log"
        and str(e.get("level") or "").lower() == "error"
        and "COMPONENT_OUTPUT_HANDLE_UNRESOLVED" in str(e.get("message") or "")
        for e in events
    )
    assert any(e.get("type") == "run_finished" and e.get("status") == "failed" for e in events)
