from app.runner.node_state import (
    build_exec_key,
    build_node_state_hash,
    build_source_fingerprint,
    canonical_cache_schema_view,
)
import pytest


def _node(kind: str = "tool", schema=None, settings=None):
    return {
        "id": "n1",
        "data": {
            "kind": kind,
            "schema": schema if schema is not None else {},
            "settings": settings if settings is not None else {},
            "sourceKind": "file" if kind == "source" else None,
        },
    }


def test_node_state_hash_changes_when_execution_version_changes():
    params = {"provider": "builtin", "builtin": {"toolId": "noop"}}
    n = _node()
    h1 = build_node_state_hash(node=n, params=params, execution_version="v1")
    h2 = build_node_state_hash(node=n, params=params, execution_version="v2")
    assert h1 != h2


def test_source_node_state_hash_includes_source_fingerprint():
    source_node = _node(kind="source")
    p1 = {"source_type": "file", "rel_path": ".", "filename": "a.txt", "file_format": "txt"}
    p2 = {"source_type": "file", "rel_path": ".", "filename": "b.txt", "file_format": "txt"}
    h1 = build_node_state_hash(node=source_node, params=p1, execution_version="v1")
    h2 = build_node_state_hash(node=source_node, params=p2, execution_version="v1")
    assert h1 != h2


def test_source_node_state_hash_changes_when_file_stat_changes(tmp_path, monkeypatch):
    source_node = _node(kind="source")
    file_path = tmp_path / "state.txt"
    file_path.write_text("a", encoding="utf-8")
    params = {"source_type": "file", "rel_path": str(tmp_path), "filename": "state.txt", "file_format": "txt"}
    h1 = build_node_state_hash(node=source_node, params=params, execution_version="v1")

    file_path.write_text("abc", encoding="utf-8")
    h2 = build_node_state_hash(node=source_node, params=params, execution_version="v1")
    assert h1 != h2


def _llm_node(schema=None, settings=None):
    return {
        "id": "llm_1",
        "data": {
            "kind": "llm",
            "schema": schema if schema is not None else {"uiOnly": True},
            "settings": settings if settings is not None else {"editor": {"collapsed": False}},
        },
    }


def _llm_params():
    return {
        "base_url": "http://localhost:11434",
        "model": "m1",
        "user_prompt": "hello",
        "output_mode": "json",
        "output_schema": {"type": "object", "properties": {"ok": {"type": "boolean"}}},
        "output_strict": True,
        "embedding_contract": {"dims": 1536, "dtype": "float32", "layout": "1d"},
        "temperature": 0.7,
        "top_p": 0.9,
        "max_tokens": 128,
        "seed": 42,
        "stop_sequences": ["END"],
        "presence_penalty": 0.1,
        "frequency_penalty": 0.2,
        "repeat_penalty": 1.1,
        "thinking": {"enabled": False, "mode": "none"},
        "input_encoding": "text",
    }


def test_llm_node_state_hash_ignores_ui_only_schema_settings():
    params = _llm_params()
    h1 = build_node_state_hash(
        node=_llm_node(
            schema={"ui": {"panel": "open"}},
            settings={"editor": {"x": 1, "y": 2}},
        ),
        params=params,
        execution_version="v1",
    )
    h2 = build_node_state_hash(
        node=_llm_node(
            schema={"ui": {"panel": "closed"}},
            settings={"editor": {"x": 999, "theme": "dark"}},
        ),
        params=params,
        execution_version="v1",
    )
    assert h1 == h2


@pytest.mark.parametrize(
    "key,a,b",
    [
        ("model", "m1", "m2"),
        ("base_url", "http://localhost:11434", "http://localhost:11435"),
        ("output_mode", "json", "text"),
        ("output_schema", {"type": "object"}, {"type": "array"}),
        ("embedding_contract", {"dims": 1536, "dtype": "float32", "layout": "1d"}, {"dims": 1024, "dtype": "float32", "layout": "1d"}),
        ("temperature", 0.7, 0.8),
        ("top_p", 0.9, 0.8),
        ("max_tokens", 128, 129),
        ("seed", 42, 43),
        ("stop_sequences", ["A"], ["B"]),
        ("presence_penalty", 0.1, 0.2),
        ("frequency_penalty", 0.1, 0.2),
        ("repeat_penalty", 1.0, 1.1),
        ("thinking", {"enabled": False, "mode": "none"}, {"enabled": True, "mode": "visible"}),
        ("input_encoding", "text", "json_canonical"),
        ("output_strict", True, False),
    ],
)
def test_llm_node_state_hash_changes_for_execution_params(key, a, b):
    p1 = _llm_params()
    p2 = _llm_params()
    p1[key] = a
    p2[key] = b
    h1 = build_node_state_hash(node=_llm_node(), params=p1, execution_version="v1")
    h2 = build_node_state_hash(node=_llm_node(), params=p2, execution_version="v1")
    assert h1 != h2


def test_source_api_fingerprint_redacts_auth_header_and_body_secrets():
    node = _node(kind="source")
    node["data"]["sourceKind"] = "api"
    params_a = {
        "source_type": "api",
        "url": "https://api.example.com/v1/data?a=1&x=0",
        "method": "POST",
        "headers": {"Authorization": "Bearer a", "X-Custom": "ok"},
        "query": {"a": "2"},
        "content_type": "application/json",
        "body_mode": "json",
        "body_json": {"token": "aaa", "query": "x"},
        "timeout_seconds": 30,
        "auth_token_ref": "TOKEN_A",
    }
    params_b = {
        **params_a,
        "headers": {"Authorization": "Bearer b", "X-Custom": "ok"},
        "body_json": {"token": "bbb", "query": "x"},
        "auth_token_ref": "TOKEN_B",
    }
    fp_a = build_source_fingerprint(node, params_a)
    fp_b = build_source_fingerprint(node, params_b)
    assert fp_a == fp_b
    assert "authorization" not in {str(k).lower() for k in fp_a.get("headers", {}).keys()}
    assert fp_a.get("query", {}).get("a") == "2"
    assert fp_a.get("body_mode") == "json"
    assert "token" not in fp_a.get("body", {})


def test_source_api_fingerprint_changes_when_effective_query_changes():
    node = _node(kind="source")
    node["data"]["sourceKind"] = "api"
    params_a = {
        "source_type": "api",
        "url": "https://api.example.com/v1/data?a=1",
        "method": "GET",
        "query": {"a": "2"},
    }
    params_b = {
        "source_type": "api",
        "url": "https://api.example.com/v1/data?a=1",
        "method": "GET",
        "query": {"a": "3"},
    }
    fp_a = build_source_fingerprint(node, params_a)
    fp_b = build_source_fingerprint(node, params_b)
    assert fp_a != fp_b


def test_exec_key_changes_when_node_impl_version_changes():
    key_a = build_exec_key(
        graph_id="g1",
        node_id="n1",
        node_kind="source",
        node_state_hash="h1",
        upstream_artifact_ids=[],
        input_refs=[],
        determinism_env={},
        execution_version="v1",
        node_impl_version="SOURCE@1",
    )
    key_b = build_exec_key(
        graph_id="g1",
        node_id="n1",
        node_kind="source",
        node_state_hash="h1",
        upstream_artifact_ids=[],
        input_refs=[],
        determinism_env={},
        execution_version="v1",
        node_impl_version="SOURCE@2",
    )
    assert key_a != key_b


def test_node_state_hash_ignores_volatile_schema_runtime_metadata():
    params = {"provider": "builtin", "builtin": {"toolId": "noop"}}
    schema_a = {
        "expectedSchema": {
            "typedSchema": {"type": "json", "fields": [{"name": "x", "type": "text"}]},
            "updatedAt": "2026-03-11T10:00:00Z",
            "state": "fresh",
            "source": "authoring",
            "schemaFingerprint": "abc",
        },
        "observedSchema": {
            "typedSchema": {"type": "json", "fields": [{"name": "x", "type": "text"}]},
            "updatedAt": "2026-03-11T10:01:00Z",
            "state": "fresh",
            "source": "runtime",
            "schemaFingerprint": "def",
        },
    }
    schema_b = {
        "expectedSchema": {
            "typedSchema": {"type": "json", "fields": [{"name": "x", "type": "text"}]},
            "updatedAt": "2030-01-01T00:00:00Z",
            "state": "stale",
            "source": "migration",
            "schemaFingerprint": "zzz",
        },
        "observedSchema": {
            "typedSchema": {"type": "json", "fields": [{"name": "x", "type": "text"}]},
            "updatedAt": "2030-01-01T00:00:01Z",
            "state": "unknown",
            "source": "runtime",
            "schemaFingerprint": "yyy",
        },
    }
    h1 = build_node_state_hash(node=_node(schema=schema_a), params=params, execution_version="v1")
    h2 = build_node_state_hash(node=_node(schema=schema_b), params=params, execution_version="v1")
    assert h1 == h2


def test_node_state_hash_changes_when_expected_typed_schema_changes():
    params = {"provider": "builtin", "builtin": {"toolId": "noop"}}
    schema_a = {
        "expectedSchema": {
            "typedSchema": {"type": "json", "fields": [{"name": "x", "type": "text"}]},
            "updatedAt": "2026-03-11T10:00:00Z",
        }
    }
    schema_b = {
        "expectedSchema": {
            "typedSchema": {"type": "json", "fields": [{"name": "x", "type": "binary"}]},
            "updatedAt": "2026-03-11T10:00:01Z",
        }
    }
    h1 = build_node_state_hash(node=_node(schema=schema_a), params=params, execution_version="v1")
    h2 = build_node_state_hash(node=_node(schema=schema_b), params=params, execution_version="v1")
    assert h1 != h2


def test_canonical_cache_schema_view_excludes_observed_runtime_channel():
    raw_schema = {
        "inferredSchema": {"typedSchema": {"type": "json", "fields": []}, "updatedAt": "2026-03-11T10:00:00Z"},
        "observedSchema": {
            "typedSchema": {"type": "text", "fields": []},
            "updatedAt": "2026-03-11T10:01:00Z",
            "state": "fresh",
            "source": "runtime",
        },
        "expectedSchema": {"typedSchema": {"type": "table", "fields": [{"name": "id", "type": "text"}]}},
    }
    view = canonical_cache_schema_view(node_kind="source", raw_schema=raw_schema)
    assert "observedSchema" not in view
    assert view == {
        "expectedSchema": {"typedSchema": {"fields": [{"name": "id", "type": "text"}], "type": "table"}},
        "inferredSchema": {"typedSchema": {"fields": [], "type": "json"}},
    }


@pytest.mark.parametrize("kind", ["source", "llm", "tool", "component"])
def test_node_state_hash_ignores_observed_schema_only_drift(kind):
    base_schema = {
        "expectedSchema": {"typedSchema": {"type": "text", "fields": []}},
        "observedSchema": {"typedSchema": {"type": "text", "fields": []}, "updatedAt": "2026-03-11T10:00:00Z"},
    }
    drifted_schema = {
        "expectedSchema": {"typedSchema": {"type": "text", "fields": []}},
        "observedSchema": {"typedSchema": {"type": "json", "fields": [{"name": "x", "type": "text"}]}, "updatedAt": "2030-01-01T00:00:00Z"},
    }
    node_a = _node(kind=kind, schema=base_schema)
    node_b = _node(kind=kind, schema=drifted_schema)
    if kind == "source":
        node_a["data"]["sourceKind"] = "file"
        node_b["data"]["sourceKind"] = "file"
        params = {"source_type": "file", "rel_path": ".", "filename": "a.txt", "file_format": "txt"}
    elif kind == "llm":
        params = _llm_params()
    else:
        params = {"provider": "builtin", "builtin": {"toolId": "noop"}}
    h1 = build_node_state_hash(node=node_a, params=params, execution_version="v1")
    h2 = build_node_state_hash(node=node_b, params=params, execution_version="v1")
    assert h1 == h2
