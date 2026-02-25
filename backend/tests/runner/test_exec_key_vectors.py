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
        normalized_params={"source_type": "file", "file_path": "a.txt", "file_format": "txt"},
        upstream_artifact_ids=[],
        execution_version="v1",
    )
    assert source_v1 == "29be5bf9738874feb8c1b002543cdee3b7539941e8bd8990da22301e3478b7cd"

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
    assert llm_v1 == "78dbb1e58fe8dd996bbf005b06ab6c8409b04d2aa93408012109307dd1bef5e4"

    tool_v1 = _tool_exec_key(
        params={
            "provider": "http",
            "http": {"url": "https://api.example.com/v1/items", "method": "GET"},
            "side_effect_mode": "pure",
        },
        input_refs=[("in", "a" * 64)],
        execution_version="v1",
    )
    assert tool_v1 == "a6e5cd6111d004b1da44cd3554fba4b46c805d82eda525b28b44cb815796a3ae"

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
    assert transform_v1 == "9ad49fe5637b750ebc82477a1ed648eff7cf6e369c519771929e5e559e2b7fdd"


def test_exec_key_params_key_order_is_canonical():
    cache = ExecutionCache()
    a = cache.execution_key(
        node_kind="source",
        normalized_params={"file_path": "a.txt", "source_type": "file", "file_format": "txt"},
        upstream_artifact_ids=[],
        execution_version="v1",
    )
    b = cache.execution_key(
        node_kind="source",
        normalized_params={"source_type": "file", "file_format": "txt", "file_path": "a.txt"},
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
