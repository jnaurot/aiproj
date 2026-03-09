from __future__ import annotations

import pytest

from app.runner.schemas import validate_node_params
from app.runner.validator import GraphValidator


def _node(node_id: str, data: dict) -> dict:
    return {"id": node_id, "data": data}


@pytest.mark.parametrize(
    ("name", "node"),
    [
        (
            "source_file",
            _node(
                "n_source_file",
                {
                    "kind": "source",
                    "sourceKind": "file",
                    "label": "Source File",
                    "params": {
                        "snapshot_id": "a" * 64,
                        "file_format": "txt",
                        "output_mode": "text",
                    },
                    "ports": {"in": None, "out": "text"},
                },
            ),
        ),
        (
            "source_database",
            _node(
                "n_source_db",
                {
                    "kind": "source",
                    "sourceKind": "database",
                    "label": "Source DB",
                    "params": {"connection_ref": "conn_main", "table_name": "events"},
                    "ports": {"in": None, "out": "table"},
                },
            ),
        ),
        (
            "source_api",
            _node(
                "n_source_api",
                {
                    "kind": "source",
                    "sourceKind": "api",
                    "label": "Source API",
                    "params": {
                        "url": "https://example.com/data",
                        "method": "GET",
                        "auth_type": "none",
                    },
                    "ports": {"in": None, "out": "json"},
                },
            ),
        ),
        (
            "transform_select",
            _node(
                "n_transform_select",
                {
                    "kind": "transform",
                    "transformKind": "select",
                    "label": "Select",
                    "params": {
                        "op": "select",
                        "select": {"mode": "include", "columns": ["id"], "strict": True},
                    },
                    "ports": {"in": "table", "out": "table"},
                },
            ),
        ),
        (
            "llm_ollama",
            _node(
                "n_llm_ollama",
                {
                    "kind": "llm",
                    "llmKind": "ollama",
                    "label": "LLM Ollama",
                    "params": {
                        "baseUrl": "http://localhost:11434",
                        "model": "gpt-oss:20b",
                        "user_prompt": "Describe the input.",
                        "output": {"mode": "text"},
                    },
                    "ports": {"in": "text", "out": "text"},
                },
            ),
        ),
        (
            "llm_openai_compat",
            _node(
                "n_llm_openai",
                {
                    "kind": "llm",
                    "llmKind": "openai_compat",
                    "label": "LLM OpenAI",
                    "params": {
                        "connectionRef": "openai_prod",
                        "model": "gpt-4.1-mini",
                        "user_prompt": "Return compact JSON.",
                        "output": {"mode": "json", "jsonSchema": {"type": "object"}},
                    },
                    "ports": {"in": "json", "out": "json"},
                },
            ),
        ),
        (
            "tool_mcp",
            _node(
                "n_tool_mcp",
                {
                    "kind": "tool",
                    "label": "Tool MCP",
                    "params": {
                        "provider": "mcp",
                        "name": "MCP tool",
                        "mcp": {"serverId": "local", "toolName": "lookup"},
                    },
                    "ports": {"in": "json", "out": "json"},
                },
            ),
        ),
        (
            "tool_http",
            _node(
                "n_tool_http",
                {
                    "kind": "tool",
                    "label": "Tool HTTP",
                    "params": {
                        "provider": "http",
                        "name": "HTTP tool",
                        "http": {"url": "https://example.com", "method": "GET"},
                    },
                    "ports": {"in": "json", "out": "json"},
                },
            ),
        ),
        (
            "tool_python",
            _node(
                "n_tool_python",
                {
                    "kind": "tool",
                    "label": "Tool Python",
                    "params": {
                        "provider": "python",
                        "name": "Python tool",
                        "python": {"code": "print('ok')"},
                    },
                    "ports": {"in": "json", "out": "json"},
                },
            ),
        ),
        (
            "tool_python_with_builtin_profile",
            _node(
                "n_tool_python_profile",
                {
                    "kind": "tool",
                    "label": "Tool Python",
                    "params": {
                        "provider": "python",
                        "name": "Python tool",
                        "python": {"code": "print('ok')"},
                        "builtin": {"profileId": "data"},
                    },
                    "ports": {"in": "json", "out": "json"},
                },
            ),
        ),
        (
            "tool_db",
            _node(
                "n_tool_db",
                {
                    "kind": "tool",
                    "label": "Tool DB",
                    "params": {
                        "provider": "db",
                        "name": "DB tool",
                        "db": {"connectionRef": "warehouse", "sql": "select 1 as ok"},
                    },
                    "ports": {"in": "json", "out": "table"},
                },
            ),
        ),
        (
            "tool_builtin",
            _node(
                "n_tool_builtin",
                {
                    "kind": "tool",
                    "label": "Tool Builtin",
                    "params": {
                        "provider": "builtin",
                        "name": "Builtin tool",
                        "builtin": {"toolId": "noop", "profileId": "data", "locked": "sha256:abc"},
                    },
                    "ports": {"in": "json", "out": "json"},
                },
            ),
        ),
        (
            "component",
            _node(
                "n_component",
                {
                    "kind": "component",
                    "label": "Component",
                    "params": {
                        "componentRef": {
                            "componentId": "cmp_reader",
                            "revisionId": "crev_1",
                            "apiVersion": "v1",
                        },
                        "bindings": {
                            "inputs": {},
                            "outputs": {"out_data": {"nodeId": "inner_1", "artifact": "current"}},
                            "config": {},
                        },
                        "api": {
                            "inputs": [],
                            "outputs": [
                                {
                                    "name": "out_data",
                                    "portType": "json",
                                    "required": True,
                                    "typedSchema": {
                                        "type": "json",
                                        "fields": [{"name": "text", "type": "text", "nullable": False}],
                                    },
                                }
                            ],
                        },
                        "config": {},
                    },
                    "ports": {"in": None, "out": "json"},
                },
            ),
        ),
    ],
)
def test_validate_node_params_accepts_supported_subtypes(name: str, node: dict) -> None:
    errors = validate_node_params(node)
    assert errors == [], f"{name} should be valid, got: {errors}"


def test_validate_node_params_rejects_invalid_builtin_profile() -> None:
    node = _node(
        "n_tool_builtin_invalid",
        {
            "kind": "tool",
            "label": "Tool Builtin Invalid",
            "params": {
                "provider": "builtin",
                "name": "Builtin tool",
                "builtin": {"toolId": "noop", "profileId": "invalid_profile"},
            },
            "ports": {"in": "json", "out": "json"},
        },
    )
    errors = validate_node_params(node)
    assert any("builtin.profileId" in err for err in errors)


def test_validate_node_params_rejects_invalid_builtin_locked() -> None:
    node = _node(
        "n_tool_builtin_locked_invalid",
        {
            "kind": "tool",
            "label": "Tool Builtin Invalid",
            "params": {
                "provider": "builtin",
                "name": "Builtin tool",
                "builtin": {"toolId": "noop", "profileId": "core", "locked": 123},
            },
            "ports": {"in": "json", "out": "json"},
        },
    )
    errors = validate_node_params(node)
    assert any("builtin.locked" in err for err in errors)


def test_validator_rejects_port_type_mismatch_between_subtypes() -> None:
    graph = {
        "nodes": [
            _node(
                "n_source",
                {
                    "kind": "source",
                    "sourceKind": "file",
                    "label": "Source",
                    "params": {"snapshot_id": "b" * 64, "file_format": "csv", "output_mode": "table"},
                    "ports": {"in": None, "out": "table"},
                },
            ),
            _node(
                "n_llm",
                {
                    "kind": "llm",
                    "llmKind": "ollama",
                    "label": "LLM",
                    "params": {
                        "baseUrl": "http://localhost:11434",
                        "model": "gpt-oss:20b",
                        "user_prompt": "Describe.",
                        "output": {"mode": "text"},
                    },
                    "ports": {"in": "json", "out": "text"},
                },
            ),
        ],
        "edges": [{"id": "e1", "source": "n_source", "target": "n_llm"}],
    }
    result = GraphValidator().validate_pre_execution(graph)
    codes = {e.code for e in result.errors}
    assert "TYPE_MISMATCH" in codes
