from app.runner.node_state import build_node_state_hash


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
