import importlib
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
        "api": {"inputs": [{"name": "in"}], "outputs": [{"name": "out"}]},
    }


def _graph(component_revision_id: str):
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
                        "bindings": {"inputs": {}, "config": {}},
                        "config": {"mode": "default"},
                    },
                    "ports": {"in": "text", "out": "json"},
                },
            },
        ],
        "edges": [{"id": "e1", "source": "src", "target": "cmp_node"}],
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
    art = await store.get(str(internal_output["artifactId"]))
    payload_schema = art.payload_schema if isinstance(art.payload_schema, dict) else {}
    artifact_meta = payload_schema.get("artifactMetadataV1") if isinstance(payload_schema.get("artifactMetadataV1"), dict) else {}
    component_meta = artifact_meta.get("component") if isinstance(artifact_meta.get("component"), dict) else {}
    assert component_meta.get("instanceNodeId") == "cmp_node"
    assert component_meta.get("componentId") == "cmp_echo"
    assert component_meta.get("componentRevisionId") == "rev_1"


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
