from types import SimpleNamespace

import pytest

from app.runner.components import ComponentExpansionError, expand_graph_components


class _ComponentStoreStub:
    def __init__(self, revisions):
        self._revisions = revisions
        self.calls = []

    def get_revision(self, component_id: str, revision_id: str):
        self.calls.append((component_id, revision_id))
        return self._revisions.get((component_id, revision_id))


def _component_revision():
    definition = {
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
    return SimpleNamespace(definition=definition)


def test_expand_component_rewrites_nodes_and_edges():
    graph = {
        "nodes": [
            {
                "id": "src",
                "data": {
                    "kind": "source",
                    "label": "Source",
                    "params": {"source_type": "text", "text": "hello"},
                    "ports": {"in": None, "out": "text"},
                },
            },
            {
                "id": "cmp_node",
                "data": {
                    "kind": "component",
                    "label": "Component",
                    "params": {
                        "componentRef": {"componentId": "cmp_echo", "revisionId": "rev_1"},
                        "bindings": {"inputs": {}, "outputs": {"out": {"nodeId": "inner_tool", "artifact": "current"}}, "config": {}},
                        "config": {},
                    },
                    "ports": {"in": "text", "out": "json"},
                },
            },
            {
                "id": "sink",
                "data": {
                    "kind": "tool",
                    "label": "Sink",
                    "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                    "ports": {"in": "json", "out": "json"},
                },
            },
        ],
        "edges": [
            {"id": "e1", "source": "src", "target": "cmp_node"},
            {"id": "e2", "source": "cmp_node", "target": "sink"},
        ],
    }
    store = _ComponentStoreStub({("cmp_echo", "rev_1"): _component_revision()})
    expanded = expand_graph_components(graph, component_store=store)

    node_ids = {str(n.get("id")) for n in expanded.graph["nodes"]}
    assert "cmp_node" in node_ids
    assert "cmp:cmp_node:inner_tool" in node_ids

    internal_meta = next(n for n in expanded.graph["nodes"] if n["id"] == "cmp:cmp_node:inner_tool")["data"]["meta"]["component"]
    assert internal_meta["componentId"] == "cmp_echo"
    assert internal_meta["componentRevisionId"] == "rev_1"
    assert internal_meta["instanceNodeId"] == "cmp_node"

    edge_pairs = {(e["source"], e["target"]) for e in expanded.graph["edges"]}
    assert ("src", "cmp:cmp_node:inner_tool") in edge_pairs
    assert ("cmp:cmp_node:inner_tool", "cmp_node") in edge_pairs
    assert ("cmp_node", "sink") in edge_pairs

    assert expanded.internal_to_parent["cmp:cmp_node:inner_tool"] == "cmp_node"
    assert "cmp:cmp_node:inner_tool" in expanded.parent_to_internal["cmp_node"]


def test_expand_component_rejects_missing_revision():
    graph = {
        "nodes": [
            {
                "id": "cmp_node",
                "data": {
                    "kind": "component",
                    "params": {"componentRef": {"componentId": "cmp_echo", "revisionId": "missing"}},
                },
            }
        ],
        "edges": [],
    }
    store = _ComponentStoreStub({})
    with pytest.raises(ComponentExpansionError) as ex:
        expand_graph_components(graph, component_store=store)
    assert ex.value.code == "COMPONENT_REVISION_NOT_FOUND"


def test_expand_component_supports_nested_component_nodes():
    nested_definition = {
        "graph": {
            "nodes": [
                {
                    "id": "deep_tool",
                    "data": {
                        "kind": "tool",
                        "params": {"provider": "builtin", "builtin": {"toolId": "noop", "args": {}}},
                        "ports": {"in": "json", "out": "json"},
                    },
                }
            ],
            "edges": [],
        },
        "api": {"inputs": [], "outputs": []},
    }
    parent_definition = {
        "graph": {
            "nodes": [
                {
                    "id": "inner_component",
                    "data": {
                        "kind": "component",
                        "params": {"componentRef": {"componentId": "cmp_nested", "revisionId": "rev_nested"}},
                    },
                }
            ],
            "edges": [],
        },
        "api": {"inputs": [], "outputs": []},
    }
    graph = {
        "nodes": [
            {
                "id": "cmp_node",
                "data": {
                    "kind": "component",
                    "params": {"componentRef": {"componentId": "cmp_parent", "revisionId": "rev_1"}},
                },
            }
        ],
        "edges": [],
    }
    store = _ComponentStoreStub(
        {
            ("cmp_parent", "rev_1"): SimpleNamespace(definition=parent_definition),
            ("cmp_nested", "rev_nested"): SimpleNamespace(definition=nested_definition),
        }
    )
    expanded = expand_graph_components(graph, component_store=store)
    node_ids = {str(n.get("id")) for n in expanded.graph["nodes"]}
    assert "cmp_node" in node_ids
    assert "cmp:cmp_node:inner_component" in node_ids
    assert "cmp:cmp_node:cmp:inner_component:deep_tool" in node_ids
    assert expanded.internal_to_parent["cmp:cmp_node:inner_component"] == "cmp_node"
    assert expanded.internal_to_parent["cmp:cmp_node:cmp:inner_component:deep_tool"] == "cmp:cmp_node:inner_component"
    assert ("cmp_parent", "rev_1") in store.calls
    assert ("cmp_nested", "rev_nested") in store.calls
