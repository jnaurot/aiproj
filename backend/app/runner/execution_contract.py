from __future__ import annotations

from typing import Any, Dict, List, Tuple


EXECUTION_CONTRACT_VERSION = 1


def _is_dict(value: Any) -> bool:
	return isinstance(value, dict)


def _as_str(value: Any) -> str:
	return str(value or "").strip()


def validate_frontier_identity_basis(basis: Dict[str, Any]) -> Tuple[bool, List[str]]:
	errors: List[str] = []
	if not _is_dict(basis):
		return False, ["basis_not_object"]
	if int(basis.get("schemaVersion") or 0) != 1:
		errors.append("basis_schema_version_invalid")
	if not _as_str(basis.get("graphId")):
		errors.append("basis_missing_graph_id")
	if not _as_str(basis.get("executionVersion")):
		errors.append("basis_missing_execution_version")
	if not _as_str(basis.get("environmentHash")):
		errors.append("basis_missing_environment_hash")
	nodes = basis.get("nodes")
	if not _is_dict(nodes):
		errors.append("basis_missing_nodes")
		return len(errors) == 0, errors
	for node_id, raw in nodes.items():
		node_key = str(node_id or "").strip() or "(unknown)"
		if not _is_dict(raw):
			errors.append(f"basis_node_invalid:{node_key}")
			continue
		if not _as_str(raw.get("nodeId")):
			errors.append(f"basis_node_missing_node_id:{node_key}")
		if not _as_str(raw.get("nodeStateHash")):
			errors.append(f"basis_node_missing_state_hash:{node_key}")
		if not _as_str(raw.get("determinismEnvHash")):
			errors.append(f"basis_node_missing_env_hash:{node_key}")
		if not _as_str(raw.get("executionVersion")):
			errors.append(f"basis_node_missing_execution_version:{node_key}")
		binding = raw.get("binding")
		if not _is_dict(binding):
			errors.append(f"basis_node_missing_binding:{node_key}")
		else:
			# binding keys may be blank; presence is required for deterministic comparison.
			if "currentExecKey" not in binding:
				errors.append(f"basis_node_binding_missing_exec_key:{node_key}")
			if "currentArtifactId" not in binding:
				errors.append(f"basis_node_binding_missing_artifact_id:{node_key}")
		if not _is_dict(raw.get("upstreamBindings")):
			errors.append(f"basis_node_missing_upstream_bindings:{node_key}")
	return len(errors) == 0, errors


def validate_execution_contract(contract: Dict[str, Any]) -> Tuple[bool, List[str]]:
	errors: List[str] = []
	if not _is_dict(contract):
		return False, ["contract_not_object"]
	try:
		version = int(contract.get("contractVersion"))
	except Exception:
		version = 0
	if version != EXECUTION_CONTRACT_VERSION:
		errors.append(f"unsupported_contract_version:{version or 'unknown'}")
		return False, errors
	if not _as_str(contract.get("graphId")):
		errors.append("contract_missing_graph_id")
	basis = contract.get("basis")
	basis_ok, basis_errors = validate_frontier_identity_basis(basis if _is_dict(basis) else {})
	if not basis_ok:
		errors.extend(basis_errors)
	return len(errors) == 0, errors

