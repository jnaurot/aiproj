from app.runner.validator import GraphValidator


def _base_graph(component_params):
    return {
        "nodes": [
            {
                "id": "n_component",
                "data": {
                    "kind": "component",
                    "label": "Component",
                    "params": component_params,
                    "ports": {"in": "json", "out": "json"},
                },
            }
        ],
        "edges": [],
    }


def test_component_validator_requires_revision_id():
    validator = GraphValidator()
    graph = _base_graph(
        {
            "componentRef": {
                "componentId": "cmp_inventory",
                "revisionId": "",
                "apiVersion": "v1",
            },
            "bindings": {"inputs": {}, "config": {}},
            "config": {},
        }
    )
    result = validator.validate_pre_execution(graph)
    assert not result.valid
    assert any(err.code == "MISSING_REVISION_ID" for err in result.errors)


def test_component_validator_requires_component_ref():
    validator = GraphValidator()
    graph = _base_graph({"bindings": {"inputs": {}, "config": {}}, "config": {}})
    result = validator.validate_pre_execution(graph)
    assert not result.valid
    assert any(err.code == "MISSING_COMPONENT_REF" for err in result.errors)


def test_component_validator_rejects_ambiguous_component_output_handle_before_run():
    validator = GraphValidator()
    graph = {
        "nodes": [
            {
                "id": "n_component",
                "data": {
                    "kind": "component",
                    "label": "Component",
                    "params": {
                        "componentRef": {
                            "componentId": "cmp_inventory",
                            "revisionId": "crev_1",
                            "apiVersion": "v1",
                        },
                        "api": {
                            "inputs": [],
                            "outputs": [
                                {"name": "summary", "portType": "text", "required": True},
                                {"name": "source", "portType": "text", "required": True},
                            ],
                        },
                        "bindings": {"inputs": {}, "config": {}, "outputs": {}},
                        "config": {},
                    },
                    "ports": {"in": None, "out": "json"},
                },
            },
            {
                "id": "n_llm",
                "data": {
                    "kind": "llm",
                    "label": "LLM",
                    "llmKind": "ollama",
                    "params": {
                        "baseUrl": "http://localhost:11434",
                        "model": "gpt-oss:20b",
                        "user_prompt": "Describe.",
                        "output": {"mode": "text"},
                    },
                    "ports": {"in": "text", "out": "text"},
                },
            },
        ],
        "edges": [
            {
                "id": "e1",
                "source": "n_component",
                "target": "n_llm",
                "sourceHandle": "out",
                "targetHandle": "in",
            }
        ],
    }

    result = validator.validate_pre_execution(graph)
    assert not result.valid
    assert any(err.code == "COMPONENT_OUTPUT_HANDLE_UNRESOLVED" for err in result.errors)


def test_component_validator_checks_named_output_type_compatibility_before_run():
    validator = GraphValidator()
    graph = {
        "nodes": [
            {
                "id": "n_component",
                "data": {
                    "kind": "component",
                    "label": "Component",
                    "params": {
                        "componentRef": {
                            "componentId": "cmp_inventory",
                            "revisionId": "crev_1",
                            "apiVersion": "v1",
                        },
                        "api": {
                            "inputs": [],
                            "outputs": [
                                {
                                    "name": "summary",
                                    "portType": "text",
                                    "required": True,
                                    "typedSchema": {"type": "text", "fields": []},
                                },
                                {
                                    "name": "payload",
                                    "portType": "json",
                                    "required": True,
                                    "typedSchema": {"type": "json", "fields": []},
                                },
                            ],
                        },
                        "bindings": {"inputs": {}, "config": {}, "outputs": {}},
                        "config": {},
                    },
                    "ports": {"in": None, "out": "json"},
                },
            },
            {
                "id": "n_llm",
                "data": {
                    "kind": "llm",
                    "label": "LLM",
                    "llmKind": "ollama",
                    "params": {
                        "baseUrl": "http://localhost:11434",
                        "model": "gpt-oss:20b",
                        "user_prompt": "Describe.",
                        "output": {"mode": "text"},
                    },
                    "ports": {"in": "text", "out": "text"},
                },
            },
        ],
        "edges": [
            {
                "id": "e1",
                "source": "n_component",
                "target": "n_llm",
                "sourceHandle": "payload",
                "targetHandle": "in",
            }
        ],
    }

    result = validator.validate_pre_execution(graph)
    assert not result.valid
    assert any(err.code == "TYPE_MISMATCH" for err in result.errors)
