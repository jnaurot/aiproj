from app.runner.node_state import build_exec_key, build_node_state_hash


def test_component_context_changes_exec_key():
    base = dict(
        graph_id="graph_component_exec",
        node_id="cmp:n_component:inner",
        node_kind="tool",
        node_state_hash="state_hash_a",
        upstream_artifact_ids=["artifact_a"],
        input_refs=[("in", "artifact_a")],
        execution_version="2",
        node_impl_version="TOOL@1",
    )

    key_v1 = build_exec_key(
        **base,
        determinism_env={
            "component_instance": {
                "component_id": "cmp_echo",
                "component_revision_id": "rev_1",
                "instance_node_id": "component_node",
            }
        },
    )
    key_v2 = build_exec_key(
        **base,
        determinism_env={
            "component_instance": {
                "component_id": "cmp_echo",
                "component_revision_id": "rev_2",
                "instance_node_id": "component_node",
            }
        },
    )

    assert key_v1 != key_v2


def _component_node(component_id: str, revision_id: str, output_ref: str) -> dict:
    return {
        "id": "cmp_node_1",
        "data": {
            "kind": "component",
            "schema": {
                "expectedSchema": {
                    "typedSchema": {"type": "text", "fields": []},
                    "updatedAt": "2026-03-11T00:00:00Z",
                },
                "observedSchema": {
                    "typedSchema": {"type": "text", "fields": []},
                    "updatedAt": "2026-03-12T00:00:00Z",
                },
            },
            "settings": {},
            "params": {
                "componentRef": {
                    "componentId": component_id,
                    "revisionId": revision_id,
                    "apiVersion": "v1",
                },
                "api": {"inputs": [], "outputs": [{"name": "summary", "typedSchema": {"type": "text", "fields": []}}]},
                "bindings": {
                    "outputs": {
                        "summary": {
                            "outputRef": output_ref,
                            "artifact": "current",
                        }
                    }
                },
            },
        },
    }


def test_component_exec_key_is_invariant_to_non_semantic_reordering():
    node_a = _component_node("cmp_nested", "crev_1", "llm_main.out")
    node_b = _component_node("cmp_nested", "crev_1", "llm_main.out")
    # Reordered authoring-only maps should not impact state/hash identity.
    node_b["data"]["params"] = {
        "bindings": node_b["data"]["params"]["bindings"],
        "api": node_b["data"]["params"]["api"],
        "componentRef": node_b["data"]["params"]["componentRef"],
    }
    state_a = build_node_state_hash(
        node=node_a,
        params=node_a["data"]["params"],
        execution_version="2",
    )
    state_b = build_node_state_hash(
        node=node_b,
        params=node_b["data"]["params"],
        execution_version="2",
    )
    key_a = build_exec_key(
        graph_id="graph_1",
        node_id="cmp_node_1",
        node_kind="component",
        node_state_hash=state_a,
        upstream_artifact_ids=[],
        input_refs=[],
        determinism_env={"component_instance": {"component_revision_id": "crev_1"}},
        execution_version="2",
        node_impl_version="COMPONENT@1",
    )
    key_b = build_exec_key(
        graph_id="graph_1",
        node_id="cmp_node_1",
        node_kind="component",
        node_state_hash=state_b,
        upstream_artifact_ids=[],
        input_refs=[],
        determinism_env={"component_instance": {"component_revision_id": "crev_1"}},
        execution_version="2",
        node_impl_version="COMPONENT@1",
    )
    assert state_a == state_b
    assert key_a == key_b


def test_component_exec_key_changes_when_output_ref_changes():
    node_a = _component_node("cmp_nested", "crev_1", "llm_main.out")
    node_b = _component_node("cmp_nested", "crev_1", "child_component.summary")
    state_a = build_node_state_hash(
        node=node_a,
        params=node_a["data"]["params"],
        execution_version="2",
    )
    state_b = build_node_state_hash(
        node=node_b,
        params=node_b["data"]["params"],
        execution_version="2",
    )
    key_a = build_exec_key(
        graph_id="graph_1",
        node_id="cmp_node_1",
        node_kind="component",
        node_state_hash=state_a,
        upstream_artifact_ids=[],
        input_refs=[],
        determinism_env={"component_instance": {"component_revision_id": "crev_1"}},
        execution_version="2",
        node_impl_version="COMPONENT@1",
    )
    key_b = build_exec_key(
        graph_id="graph_1",
        node_id="cmp_node_1",
        node_kind="component",
        node_state_hash=state_b,
        upstream_artifact_ids=[],
        input_refs=[],
        determinism_env={"component_instance": {"component_revision_id": "crev_1"}},
        execution_version="2",
        node_impl_version="COMPONENT@1",
    )
    assert state_a != state_b
    assert key_a != key_b
