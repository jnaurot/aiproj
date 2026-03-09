import importlib
import sys
import types


def _run_module():
    if "duckdb" not in sys.modules:
        sys.modules["duckdb"] = types.SimpleNamespace()
    return importlib.import_module("app.runner.run")


def test_tool_payload_schema_json_recursive_types():
    run_mod = _run_module()
    payload = {
        "user": {
            "id": 7,
            "name": "alice",
            "active": True,
            "tags": [{"label": "x", "score": 1.25}, None],
        },
        "meta": {"count": 2},
    }

    out = run_mod._tool_payload_schema("json", payload)
    assert out is not None
    assert out["type"] == "json"
    assert out["json_shape"] == "object"

    root_schema = out["schema"]
    assert root_schema["type"] == "object"
    assert sorted(root_schema["required"]) == ["meta", "user"]

    user_schema = root_schema["properties"]["user"]
    assert user_schema["type"] == "object"
    assert sorted(user_schema["required"]) == ["active", "id", "name", "tags"]
    assert user_schema["properties"]["id"]["type"] == "integer"
    assert user_schema["properties"]["name"]["type"] == "string"
    assert user_schema["properties"]["active"]["type"] == "boolean"

    tags_schema = user_schema["properties"]["tags"]
    assert tags_schema["type"] == "array"
    assert tags_schema["items"]["type"] == "union"
    item_types = sorted(x.get("type") for x in tags_schema["items"]["anyOf"])
    assert item_types == ["null", "object"]


def test_tool_payload_schema_empty_array_items_are_typed_unknown():
    run_mod = _run_module()
    out = run_mod._tool_payload_schema("json", [])
    assert out is not None
    assert out["schema"]["type"] == "array"
    assert out["schema"]["items"]["type"] == "unknown"


def test_tool_payload_schema_includes_builtin_environment_when_available():
    run_mod = _run_module()
    out = run_mod._tool_payload_schema(
        "json",
        {"ok": True},
        {
            "builtin_environment": {
                "profileId": "data",
                "source": "profile",
                "packages": ["polars", "pandas"],
            }
        },
    )
    assert out is not None
    assert out.get("builtin_environment") == {
        "profileId": "data",
        "source": "profile",
        "packages": ["polars", "pandas"],
    }
