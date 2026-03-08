import pytest

from app.runner.run import ContractMismatchError, resolve_input_refs


class _NoopArtifactStore:
    async def read(self, artifact_id: str) -> bytes:
        return b"{}"


@pytest.mark.asyncio
async def test_resolve_input_refs_component_out_handle_is_rejected_for_multi_outputs():
    edges = {
        "e1": {
            "id": "e1",
            "source": "cmp1",
            "target": "llm1",
            "sourceHandle": "out",
            "targetHandle": "in",
            "data": {"contract": {"out": "text", "in": "text"}},
        }
    }
    component_node = {
        "id": "cmp1",
        "data": {
            "kind": "component",
            "params": {
                "api": {
                    "outputs": [
                        {"name": "SOD", "portType": "text"},
                        {"name": "Source", "portType": "json"},
                    ]
                }
            },
        },
    }

    with pytest.raises(ContractMismatchError) as exc:
        await resolve_input_refs(
            edges=edges,
            node_id="llm1",
            get_current_artifact=lambda node_id: "wrapper_a1" if node_id == "cmp1" else None,
            get_node_by_id=lambda node_id: component_node if node_id == "cmp1" else None,
            artifact_store=_NoopArtifactStore(),
        )

    assert exc.value.code == "COMPONENT_OUTPUT_HANDLE_UNRESOLVED"


@pytest.mark.asyncio
async def test_resolve_input_refs_component_uses_named_source_handle_when_direct_binding_present():
    edges = {
        "e1": {
            "id": "e1",
            "source": "cmp1",
            "target": "llm1",
            "sourceHandle": "Source",
            "targetHandle": "in",
            "data": {"contract": {"out": "text", "in": "text"}},
        }
    }
    component_node = {
        "id": "cmp1",
        "data": {
            "kind": "component",
            "params": {
                "api": {
                    "outputs": [
                        {"name": "SOD", "portType": "text"},
                        {"name": "Source", "portType": "text"},
                    ]
                },
                "bindings": {"outputs": {"Source": {"nodeId": "n_source", "artifact": "current"}}},
            },
        },
    }

    refs = await resolve_input_refs(
        edges=edges,
        node_id="llm1",
        get_current_artifact=lambda node_id: (
            "wrapper_a1"
            if node_id == "cmp1"
            else ("direct_art_source" if node_id == "cmp:cmp1:n_source" else None)
        ),
        get_node_by_id=lambda node_id: component_node if node_id == "cmp1" else None,
        artifact_store=_NoopArtifactStore(),
    )

    assert refs == [("in", "direct_art_source")]


@pytest.mark.asyncio
async def test_resolve_input_refs_component_explicit_named_handle_requires_binding():
    edges = {
        "e1": {
            "id": "e1",
            "source": "cmp1",
            "target": "llm1",
            "sourceHandle": "out_2",
            "targetHandle": "in",
            "data": {"contract": {"out": "text", "in": "text"}},
        }
    }
    component_node = {
        "id": "cmp1",
        "data": {
            "kind": "component",
            "params": {
                "api": {
                    "outputs": [
                        {"name": "out_data", "portType": "text"},
                        {"name": "out_2", "portType": "text"},
                    ]
                }
            },
        },
    }

    with pytest.raises(ContractMismatchError) as exc:
        await resolve_input_refs(
            edges=edges,
            node_id="llm1",
            get_current_artifact=lambda node_id: "wrapper_a1" if node_id == "cmp1" else None,
            get_node_by_id=lambda node_id: component_node if node_id == "cmp1" else None,
            artifact_store=_NoopArtifactStore(),
        )

    assert exc.value.code == "COMPONENT_OUTPUT_HANDLE_UNRESOLVED"


@pytest.mark.asyncio
async def test_resolve_input_refs_component_uses_direct_binding_artifact_before_wrapper():
    edges = {
        "e1": {
            "id": "e1",
            "source": "cmp1",
            "target": "llm1",
            "sourceHandle": "source",
            "targetHandle": "in",
            "data": {"contract": {"out": "text", "in": "text"}},
        }
    }
    component_node = {
        "id": "cmp1",
        "data": {
            "kind": "component",
            "params": {
                "api": {"outputs": [{"name": "source", "portType": "text"}]},
                "bindings": {"outputs": {"source": {"nodeId": "n_source", "artifact": "current"}}},
            },
        },
    }

    refs = await resolve_input_refs(
        edges=edges,
        node_id="llm1",
        get_current_artifact=lambda node_id: (
            "wrapper_a1"
            if node_id == "cmp1"
            else ("direct_art_source" if node_id == "cmp:cmp1:n_source" else None)
        ),
        get_node_by_id=lambda node_id: component_node if node_id == "cmp1" else None,
        artifact_store=_NoopArtifactStore(),
    )

    assert refs == [("in", "direct_art_source")]


@pytest.mark.asyncio
async def test_resolve_input_refs_component_named_binding_without_artifact_raises_before_wrapper():
    edges = {
        "e1": {
            "id": "e1",
            "source": "cmp1",
            "target": "llm1",
            "sourceHandle": "source",
            "targetHandle": "in",
            "data": {"contract": {"out": "text", "in": "text"}},
        }
    }
    component_node = {
        "id": "cmp1",
        "data": {
            "kind": "component",
            "params": {
                "api": {"outputs": [{"name": "source", "portType": "text"}]},
                "bindings": {"outputs": {"source": {"nodeId": "n_source", "artifact": "current"}}},
            },
        },
    }

    with pytest.raises(ContractMismatchError) as exc:
        await resolve_input_refs(
            edges=edges,
            node_id="llm1",
            get_current_artifact=lambda node_id: "wrapper_a1" if node_id == "cmp1" else None,
            get_node_by_id=lambda node_id: component_node if node_id == "cmp1" else None,
            artifact_store=_NoopArtifactStore(),
        )

    assert exc.value.code == "COMPONENT_OUTPUT_HANDLE_UNRESOLVED"


@pytest.mark.asyncio
async def test_resolve_input_refs_ignores_legacy_component_output_binding_for_named_resolution():
    edges = {
        "e1": {
            "id": "e1",
            "source": "cmp1",
            "target": "llm1",
            "sourceHandle": "out",
            "targetHandle": "in",
            "data": {
                "contract": {"out": "text", "in": "text"},
                "componentOutputBinding": {"output": "source"},
            },
        }
    }
    component_node = {
        "id": "cmp1",
        "data": {
            "kind": "component",
            "params": {
                "api": {"outputs": [{"name": "summary", "portType": "text"}, {"name": "source", "portType": "text"}]},
                "bindings": {"outputs": {"source": {"nodeId": "n_source", "artifact": "current"}}},
            },
        },
    }

    with pytest.raises(ContractMismatchError) as exc:
        await resolve_input_refs(
            edges=edges,
            node_id="llm1",
            get_current_artifact=lambda node_id: "wrapper_a1" if node_id == "cmp1" else None,
            get_node_by_id=lambda node_id: component_node if node_id == "cmp1" else None,
            artifact_store=_NoopArtifactStore(),
        )

    assert exc.value.code == "COMPONENT_OUTPUT_HANDLE_UNRESOLVED"
