from app.runner.node_state import build_node_state_hash
import pytest


def _node(kind: str = "tool", ports=None, schema=None, settings=None):
    return {
        "id": "n1",
        "data": {
            "kind": kind,
            "ports": ports if ports is not None else {"in": "json", "out": "json"},
            "schema": schema if schema is not None else {},
            "settings": settings if settings is not None else {},
            "sourceKind": "file" if kind == "source" else None,
        },
    }


def test_node_state_hash_changes_when_ports_change():
    params = {"provider": "builtin", "builtin": {"toolId": "noop"}}
    h1 = build_node_state_hash(node=_node(ports={"in": "json", "out": "json"}), params=params, execution_version="v1")
    h2 = build_node_state_hash(node=_node(ports={"in": "text", "out": "json"}), params=params, execution_version="v1")
    assert h1 != h2


def test_node_state_hash_changes_when_execution_version_changes():
    params = {"provider": "builtin", "builtin": {"toolId": "noop"}}
    n = _node()
    h1 = build_node_state_hash(node=n, params=params, execution_version="v1")
    h2 = build_node_state_hash(node=n, params=params, execution_version="v2")
    assert h1 != h2


def test_source_node_state_hash_includes_source_fingerprint():
    source_node = _node(kind="source", ports={"in": None, "out": "text"})
    p1 = {"source_type": "file", "file_path": "/tmp/a.txt", "file_format": "txt"}
    p2 = {"source_type": "file", "file_path": "/tmp/b.txt", "file_format": "txt"}
    h1 = build_node_state_hash(node=source_node, params=p1, execution_version="v1")
    h2 = build_node_state_hash(node=source_node, params=p2, execution_version="v1")
    assert h1 != h2


def test_source_node_state_hash_changes_when_file_stat_changes(tmp_path):
    source_node = _node(kind="source", ports={"in": None, "out": "text"})
    file_path = tmp_path / "state.txt"
    file_path.write_text("a", encoding="utf-8")
    params = {"source_type": "file", "file_path": str(file_path), "file_format": "txt"}
    h1 = build_node_state_hash(node=source_node, params=params, execution_version="v1")

    file_path.write_text("abc", encoding="utf-8")
    h2 = build_node_state_hash(node=source_node, params=params, execution_version="v1")
    assert h1 != h2


def _llm_node(ports=None, schema=None, settings=None):
    return {
        "id": "llm_1",
        "data": {
            "kind": "llm",
            "ports": ports if ports is not None else {"in": "text", "out": "text"},
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
        "thinking": "off",
        "input_encoding": "text",
    }


def test_llm_node_state_hash_ignores_ui_only_ports_schema_settings():
    params = _llm_params()
    h1 = build_node_state_hash(
        node=_llm_node(
            ports={"in": "text", "out": "json", "uiPort": "x"},
            schema={"ui": {"panel": "open"}},
            settings={"editor": {"x": 1, "y": 2}},
        ),
        params=params,
        execution_version="v1",
    )
    h2 = build_node_state_hash(
        node=_llm_node(
            ports={"in": "table", "out": "text", "debug": True},
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
        ("thinking", "off", "auto"),
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
