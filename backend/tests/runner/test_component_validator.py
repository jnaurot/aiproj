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
