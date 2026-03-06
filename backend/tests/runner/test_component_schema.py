import json

from app.runner.schemas import validate_node_params


def _component_node(params):
	return {
		"id": "n_component",
		"data": {
			"kind": "component",
			"label": "Component",
			"params": params,
			"ports": {"in": "json", "out": "json"},
		},
	}


def test_component_schema_requires_component_ref_machine_error():
	errors = validate_node_params(_component_node({"bindings": {"inputs": {}, "config": {}}, "config": {}}))
	assert errors
	payload = json.loads(errors[0])
	assert payload["errorCode"] == "MISSING_COMPONENT_REF"
	assert payload["paramPath"] == "params.componentRef"


def test_component_schema_requires_revision_id_machine_error():
	errors = validate_node_params(
		_component_node(
			{
				"componentRef": {"componentId": "cmp_1", "revisionId": "", "apiVersion": "v1"},
				"bindings": {"inputs": {}, "config": {}},
				"config": {},
			}
		)
	)
	decoded = [json.loads(e) for e in errors if str(e).startswith("{")]
	assert any(p.get("errorCode") == "MISSING_REVISION_ID" for p in decoded)
	assert any(p.get("paramPath") == "params.componentRef.revisionId" for p in decoded)


def test_component_schema_typed_schema_validation_machine_error():
	errors = validate_node_params(
		_component_node(
			{
				"componentRef": {"componentId": "cmp_1", "revisionId": "crev_1", "apiVersion": "v1"},
				"bindings": {"inputs": {}, "config": {}},
				"config": {},
				"api": {
					"inputs": [
						{
							"name": "in_data",
							"portType": "table",
							"required": True,
							"typedSchema": {"type": "nonsense"},
						}
					],
					"outputs": [],
				},
			}
		)
	)
	decoded = [json.loads(e) for e in errors if str(e).startswith("{")]
	assert any(p.get("errorCode") == "INVALID_TYPED_SCHEMA_TYPE" for p in decoded)
