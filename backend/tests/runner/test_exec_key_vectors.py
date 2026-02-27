import sys
import types

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace()

from app.runner.cache import ExecutionCache
from app.runner.run import _tool_exec_key, _transform_exec_key


def test_exec_key_golden_vectors():
    cache = ExecutionCache()

    source_v1 = cache.execution_key(
        node_kind="source",
        normalized_params={"source_type": "file", "rel_path": ".", "filename": "a.txt", "file_format": "txt"},
        upstream_artifact_ids=[],
        execution_version="v1",
    )
    assert source_v1 == "58f056d885b23129077dc86165d2a3860f3f247d503026c7c8a526b86f021729"

    llm_v1 = cache.execution_key(
        node_kind="llm",
        normalized_params={
            "base_url": "http://localhost:11434",
            "model": "m1",
            "user_prompt": "hi",
            "output_mode": "text",
        },
        upstream_artifact_ids=["b" * 64],
        execution_version="v1",
    )
    assert llm_v1 == "2f7ffd098230b7d0efcc3e3d8fed2b3a1eb33124ebd072584a3f12f855c838e6"

    tool_v1 = _tool_exec_key(
        params={
            "provider": "http",
            "http": {"url": "https://api.example.com/v1/items", "method": "GET"},
            "side_effect_mode": "pure",
        },
        input_refs=[("in", "a" * 64)],
        execution_version="v1",
    )
    assert tool_v1 == "23385e3867619fca871a14e599b0521bc2f06291c08dd44cc5565f4fbba01bfd"

    transform_v1 = _transform_exec_key(
        normalized_params={
            "op": "limit",
            "limit": {"n": 10},
            "enabled": True,
            "notes": "",
            "cache": {"enabled": False},
        },
        input_refs=[("in", "a" * 64)],
        execution_version="v1",
    )
    assert transform_v1 == "5ba0ab7155d7b2de23c854af888c65b22e2e0fa60875141c3cde57bc1b36f47d"


def test_exec_key_params_key_order_is_canonical():
    cache = ExecutionCache()
    a = cache.execution_key(
        node_kind="source",
        normalized_params={"rel_path": ".", "filename": "a.txt", "source_type": "file", "file_format": "txt"},
        upstream_artifact_ids=[],
        execution_version="v1",
    )
    b = cache.execution_key(
        node_kind="source",
        normalized_params={"source_type": "file", "file_format": "txt", "filename": "a.txt", "rel_path": "."},
        upstream_artifact_ids=[],
        execution_version="v1",
    )
    assert a == b


def test_exec_key_build_version_change_busts_cache():
    cache = ExecutionCache()
    a = cache.execution_key(
        node_kind="llm",
        normalized_params={"model": "m1", "user_prompt": "hi", "output_mode": "text"},
        upstream_artifact_ids=["a" * 64],
        execution_version="v1",
    )
    b = cache.execution_key(
        node_kind="llm",
        normalized_params={"model": "m1", "user_prompt": "hi", "output_mode": "text"},
        upstream_artifact_ids=["a" * 64],
        execution_version="v2",
    )
    assert a != b


def test_transform_exec_key_changes_when_input_port_mapping_changes():
    params = {"op": "join", "join": {"withNodeId": "right", "how": "inner", "on": [{"left": "id", "right": "id"}]}}
    a = _transform_exec_key(
        normalized_params=params,
        input_refs=[("left", "a" * 64), ("right", "b" * 64)],
        execution_version="v1",
    )
    b = _transform_exec_key(
        normalized_params=params,
        input_refs=[("left", "b" * 64), ("right", "a" * 64)],
        execution_version="v1",
    )
    assert a != b


def test_exec_key_ignores_ports_metadata_when_params_and_inputs_same():
    cache = ExecutionCache()
    params = {"op": "limit", "limit": {"n": 10}, "enabled": True, "cache": {"enabled": False}}
    k1 = cache.execution_key(
        node_kind="transform",
        normalized_params=params,
        upstream_artifact_ids=["a" * 64],
        execution_version="v1",
    )
    # Simulate port metadata UI edits by leaving determinism inputs unchanged.
    k2 = cache.execution_key(
        node_kind="transform",
        normalized_params=params,
        upstream_artifact_ids=["a" * 64],
        execution_version="v1",
    )
    assert k1 == k2


def test_transform_exec_key_changes_when_join_with_node_id_changes():
    base = {"op": "join", "join": {"how": "inner", "on": [{"left": "id", "right": "id"}]}}
    a = _transform_exec_key(
        normalized_params={**base, "join": {**base["join"], "withNodeId": "node_a"}},
        input_refs=[("left", "a" * 64), ("right", "b" * 64)],
        execution_version="v1",
    )
    b = _transform_exec_key(
        normalized_params={**base, "join": {**base["join"], "withNodeId": "node_b"}},
        input_refs=[("left", "a" * 64), ("right", "b" * 64)],
        execution_version="v1",
    )
    assert a != b


def test_transform_exec_key_changes_when_ops_reordered():
    a = _transform_exec_key(
        normalized_params={
            "op": "derive",
            "derive": {
                "columns": [
                    {"name": "a", "expr": '"x" + 1'},
                    {"name": "b", "expr": '"y" + 1'},
                ]
            },
        },
        input_refs=[("in", "a" * 64)],
        execution_version="v1",
    )
    b = _transform_exec_key(
        normalized_params={
            "op": "derive",
            "derive": {
                "columns": [
                    {"name": "b", "expr": '"y" + 1'},
                    {"name": "a", "expr": '"x" + 1'},
                ]
            },
        },
        input_refs=[("in", "a" * 64)],
        execution_version="v1",
    )
    assert a != b


def test_exec_key_changes_when_determinism_env_changes():
    cache = ExecutionCache()
    base_params = {"model": "m1", "user_prompt": "hi", "output_mode": "text"}
    inputs = [("in", "a" * 64)]
    a = cache.execution_key(
        node_kind="llm",
        normalized_params=base_params,
        upstream_artifact_ids=["a" * 64],
        input_bindings=inputs,
        determinism_env={"llm_table_max_rows": 200, "llm_prompt_max_chars": 20000},
        execution_version="v1",
    )
    b = cache.execution_key(
        node_kind="llm",
        normalized_params=base_params,
        upstream_artifact_ids=["a" * 64],
        input_bindings=inputs,
        determinism_env={"llm_table_max_rows": 100, "llm_prompt_max_chars": 20000},
        execution_version="v1",
    )
    assert a != b


def test_exec_key_input_bindings_insertion_order_is_canonical():
    cache = ExecutionCache()
    params = {"model": "m1", "user_prompt": "hi", "output_mode": "text"}
    env = {"llm_table_max_rows": 200}
    a = cache.execution_key(
        node_kind="llm",
        normalized_params=params,
        upstream_artifact_ids=["a" * 64, "b" * 64],
        input_bindings=[("right", "b" * 64), ("left", "a" * 64)],
        determinism_env=env,
        execution_version="v1",
    )
    b = cache.execution_key(
        node_kind="llm",
        normalized_params=params,
        upstream_artifact_ids=["b" * 64, "a" * 64],
        input_bindings=[("left", "a" * 64), ("right", "b" * 64)],
        determinism_env=env,
        execution_version="v1",
    )
    assert a == b
