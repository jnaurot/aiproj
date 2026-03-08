import json

import pytest

from app.runner.run import ContractMismatchError, resolve_input_refs


class _ArtifactStore:
    async def read(self, artifact_id: str) -> bytes:
        assert artifact_id == "wrapper_a1"
        return json.dumps(
            {
                "outputs": {
                    "SOD": {"artifact_id": "text_art_1"},
                    "Source": {"artifact_id": "text_art_2"},
                }
            }
        ).encode("utf-8")


@pytest.mark.asyncio
async def test_resolve_input_refs_component_out_handle_infers_named_output_from_contract():
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

    refs = await resolve_input_refs(
        edges=edges,
        node_id="llm1",
        get_current_artifact=lambda node_id: "wrapper_a1" if node_id == "cmp1" else None,
        get_node_by_id=lambda node_id: component_node if node_id == "cmp1" else None,
        artifact_store=_ArtifactStore(),
    )

    assert refs == [("in", "text_art_1")]


@pytest.mark.asyncio
async def test_resolve_input_refs_component_uses_component_output_binding_when_present():
    edges = {
        "e1": {
            "id": "e1",
            "source": "cmp1",
            "target": "llm1",
            "sourceHandle": "out",
            "targetHandle": "in",
            "data": {
                "contract": {"out": "text", "in": "text"},
                "componentOutputBinding": {"output": "Source"},
            },
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
                }
            },
        },
    }

    refs = await resolve_input_refs(
        edges=edges,
        node_id="llm1",
        get_current_artifact=lambda node_id: "wrapper_a1" if node_id == "cmp1" else None,
        get_node_by_id=lambda node_id: component_node if node_id == "cmp1" else None,
        artifact_store=_ArtifactStore(),
    )

    assert refs == [("in", "text_art_2")]


@pytest.mark.asyncio
async def test_resolve_input_refs_component_explicit_named_handle_does_not_fall_back_to_wrapper_json():
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

    class _MissingOutputArtifactStore:
        async def read(self, artifact_id: str) -> bytes:
            assert artifact_id == "wrapper_a1"
            return json.dumps({"outputs": {"out_data": {"artifact_id": "text_art_1"}}}).encode("utf-8")

    with pytest.raises(ContractMismatchError) as exc:
        await resolve_input_refs(
            edges=edges,
            node_id="llm1",
            get_current_artifact=lambda node_id: "wrapper_a1" if node_id == "cmp1" else None,
            get_node_by_id=lambda node_id: component_node if node_id == "cmp1" else None,
            artifact_store=_MissingOutputArtifactStore(),
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

    class _BadWrapperArtifactStore:
        async def read(self, artifact_id: str) -> bytes:
            # Wrapper has no "source" field; resolver must not depend on this path
            return json.dumps({"outputs": {"other": {"artifact_id": "wrapper_other"}}}).encode("utf-8")

    refs = await resolve_input_refs(
        edges=edges,
        node_id="llm1",
        get_current_artifact=lambda node_id: (
            "wrapper_a1"
            if node_id == "cmp1"
            else ("direct_art_source" if node_id == "cmp:cmp1:n_source" else None)
        ),
        get_node_by_id=lambda node_id: component_node if node_id == "cmp1" else None,
        artifact_store=_BadWrapperArtifactStore(),
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

    class _WrapperWouldResolveButShouldNotBeUsed:
        async def read(self, artifact_id: str) -> bytes:
            return json.dumps({"outputs": {"source": {"artifact_id": "wrapper_art_source"}}}).encode("utf-8")

    with pytest.raises(ContractMismatchError) as exc:
        await resolve_input_refs(
            edges=edges,
            node_id="llm1",
            get_current_artifact=lambda node_id: "wrapper_a1" if node_id == "cmp1" else None,
            get_node_by_id=lambda node_id: component_node if node_id == "cmp1" else None,
            artifact_store=_WrapperWouldResolveButShouldNotBeUsed(),
        )

    assert exc.value.code == "COMPONENT_OUTPUT_HANDLE_UNRESOLVED"
