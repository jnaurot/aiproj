import json
from pathlib import Path

from app.runner.contracts import (
	EMBEDDINGS_ANY_V1,
	JSON_ANY_V1,
	TEXT_V1,
	default_contract_for_node,
)
from app.runner.schemas import validate_node_params


CONTRACT_TO_TYPE = {
	TEXT_V1: "text",
	JSON_ANY_V1: "json",
	EMBEDDINGS_ANY_V1: "embeddings",
}


def _fixture_cases() -> list[dict]:
	fixture_path = (
		Path(__file__).resolve().parents[3] / "shared" / "test_fixtures" / "model_contract_parity.v1.json"
	)
	payload = json.loads(fixture_path.read_text(encoding="utf-8"))
	cases = payload.get("cases")
	return cases if isinstance(cases, list) else []


def test_model_contract_parity_fixture_vectors():
	cases = _fixture_cases()
	assert len(cases) > 0
	for case in cases:
		node_data = dict(case.get("nodeData") or {})
		node = {"id": f"n_{case.get('id')}", "data": node_data}
		errors = validate_node_params(node)
		assert errors == [], f"fixture {case.get('id')} validation mismatch: {errors}"
		contract = default_contract_for_node(node)
		resolved_type = CONTRACT_TO_TYPE.get(contract, "binary")
		assert resolved_type == case.get("expectedOutType"), f"fixture {case.get('id')} mismatch"