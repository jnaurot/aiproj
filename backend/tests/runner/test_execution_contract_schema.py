from __future__ import annotations

from app.runner.execution_contract import (
	EXECUTION_CONTRACT_VERSION,
	validate_execution_contract,
	validate_frontier_identity_basis,
)


def _valid_basis() -> dict:
	return {
		"schemaVersion": 1,
		"graphId": "g1",
		"executionVersion": "v1",
		"environmentHash": "abc123",
		"nodes": {
			"n1": {
				"nodeId": "n1",
				"nodeStateHash": "state",
				"determinismEnvHash": "env",
				"binding": {"currentExecKey": "", "currentArtifactId": ""},
				"upstreamBindings": {},
				"executionVersion": "v1",
			}
		},
	}


def test_validate_frontier_identity_basis_accepts_valid_payload() -> None:
	ok, errors = validate_frontier_identity_basis(_valid_basis())
	assert ok is True
	assert errors == []


def test_validate_execution_contract_accepts_v1_contract() -> None:
	contract = {
		"contractVersion": EXECUTION_CONTRACT_VERSION,
		"graphId": "g1",
		"basis": _valid_basis(),
	}
	ok, errors = validate_execution_contract(contract)
	assert ok is True
	assert errors == []


def test_validate_execution_contract_rejects_unknown_version() -> None:
	contract = {
		"contractVersion": 999,
		"graphId": "g1",
		"basis": _valid_basis(),
	}
	ok, errors = validate_execution_contract(contract)
	assert ok is False
	assert any(str(err).startswith("unsupported_contract_version:") for err in errors)

