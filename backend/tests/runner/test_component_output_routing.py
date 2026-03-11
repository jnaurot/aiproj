import pytest

from app.runner.node_state import build_exec_key, build_node_state_hash
from app.runner.run import resolve_input_refs


def _component_node() -> dict:
    return {
        "id": "component_1",
        "data": {
            "kind": "component",
            "params": {
                "api": {
                    "inputs": [],
                    "outputs": [
                        {"name": "out_data", "typedSchema": {"type": "text", "fields": []}},
                        {"name": "out_2", "typedSchema": {"type": "text", "fields": []}},
                    ],
                },
                "bindings": {
                    "outputs": {
                        "out_data": {"artifact": "current", "outputRef": "llm_summary.out"},
                        "out_2": {"artifact": "current", "outputRef": "source_text.out"},
                    }
                },
            },
        },
    }


def _llm_node(node_id: str) -> dict:
    return {
        "id": node_id,
        "data": {
            "kind": "llm",
            "schema": {"expectedSchema": {"typedSchema": {"type": "text", "fields": []}}},
            "params": {"model": "x", "user_prompt": "test", "output_mode": "text"},
        },
    }


@pytest.mark.asyncio
async def test_component_named_outputs_route_to_distinct_internal_artifacts():
    nodes = {
        "component_1": _component_node(),
        "llm_a": _llm_node("llm_a"),
        "llm_b": _llm_node("llm_b"),
    }
    edges = {
        # Internal projection edges (expanded runtime graph)
        "e_internal_summary": {
            "id": "e_internal_summary",
            "source": "cmp:component_1:n_llm_summary",
            "target": "component_1",
            "targetHandle": "out_data",
        },
        "e_internal_source": {
            "id": "e_internal_source",
            "source": "cmp:component_1:n_source_text",
            "target": "component_1",
            "targetHandle": "out_2",
        },
        # Parent graph edges from component outputs
        "e_parent_out_data": {
            "id": "e_parent_out_data",
            "source": "component_1",
            "sourceHandle": "out_data",
            "target": "llm_a",
            "targetHandle": "in",
        },
        "e_parent_out_2": {
            "id": "e_parent_out_2",
            "source": "component_1",
            "sourceHandle": "out_2",
            "target": "llm_b",
            "targetHandle": "in",
        },
    }
    artifacts = {
        "cmp:component_1:n_llm_summary": "artifact_summary",
        "cmp:component_1:n_source_text": "artifact_source",
        "component_1": "artifact_component_wrapper",
    }

    refs_a = await resolve_input_refs(
        edges=edges,
        node_id="llm_a",
        get_current_artifact=lambda nid: artifacts.get(str(nid), None),
        get_node_by_id=lambda nid: nodes.get(str(nid), None),
        artifact_store=None,
    )
    refs_b = await resolve_input_refs(
        edges=edges,
        node_id="llm_b",
        get_current_artifact=lambda nid: artifacts.get(str(nid), None),
        get_node_by_id=lambda nid: nodes.get(str(nid), None),
        artifact_store=None,
    )

    assert refs_a == [("in", "artifact_summary")]
    assert refs_b == [("in", "artifact_source")]


@pytest.mark.asyncio
async def test_component_output_routing_is_deterministic_across_reruns_and_keys():
    nodes = {
        "component_1": _component_node(),
        "llm_b": _llm_node("llm_b"),
    }
    edges = {
        "e_internal_source": {
            "id": "e_internal_source",
            "source": "cmp:component_1:n_source_text",
            "target": "component_1",
            "targetHandle": "out_2",
        },
        "e_parent_out_2": {
            "id": "e_parent_out_2",
            "source": "component_1",
            "sourceHandle": "out_2",
            "target": "llm_b",
            "targetHandle": "in",
        },
    }

    artifacts_run_1 = {
        "cmp:component_1:n_source_text": "artifact_source_v1",
        "component_1": "artifact_component_wrapper_v1",
    }
    refs_1 = await resolve_input_refs(
        edges=edges,
        node_id="llm_b",
        get_current_artifact=lambda nid: artifacts_run_1.get(str(nid), None),
        get_node_by_id=lambda nid: nodes.get(str(nid), None),
        artifact_store=None,
    )
    refs_2 = await resolve_input_refs(
        edges=edges,
        node_id="llm_b",
        get_current_artifact=lambda nid: artifacts_run_1.get(str(nid), None),
        get_node_by_id=lambda nid: nodes.get(str(nid), None),
        artifact_store=None,
    )
    assert refs_1 == refs_2 == [("in", "artifact_source_v1")]

    llm_node = nodes["llm_b"]
    llm_params = dict((llm_node.get("data") or {}).get("params") or {})
    node_state_hash = build_node_state_hash(node=llm_node, params=llm_params, execution_version="v1")

    key_1 = build_exec_key(
        graph_id="graph_component_routing",
        node_id="llm_b",
        node_kind="llm",
        node_state_hash=node_state_hash,
        upstream_artifact_ids=[aid for _, aid in refs_1],
        input_refs=refs_1,
        determinism_env={"executor_code_hash": "a" * 64},
        execution_version="v1",
        node_impl_version="LLM@1",
    )
    key_2 = build_exec_key(
        graph_id="graph_component_routing",
        node_id="llm_b",
        node_kind="llm",
        node_state_hash=node_state_hash,
        upstream_artifact_ids=[aid for _, aid in refs_2],
        input_refs=refs_2,
        determinism_env={"executor_code_hash": "a" * 64},
        execution_version="v1",
        node_impl_version="LLM@1",
    )
    assert key_1 == key_2

    # Semantic change in bound internal artifact must change downstream key.
    artifacts_run_2 = {
        "cmp:component_1:n_source_text": "artifact_source_v2",
        "component_1": "artifact_component_wrapper_v2",
    }
    refs_3 = await resolve_input_refs(
        edges=edges,
        node_id="llm_b",
        get_current_artifact=lambda nid: artifacts_run_2.get(str(nid), None),
        get_node_by_id=lambda nid: nodes.get(str(nid), None),
        artifact_store=None,
    )
    key_3 = build_exec_key(
        graph_id="graph_component_routing",
        node_id="llm_b",
        node_kind="llm",
        node_state_hash=node_state_hash,
        upstream_artifact_ids=[aid for _, aid in refs_3],
        input_refs=refs_3,
        determinism_env={"executor_code_hash": "a" * 64},
        execution_version="v1",
        node_impl_version="LLM@1",
    )
    assert refs_3 == [("in", "artifact_source_v2")]
    assert key_3 != key_1
