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


def _normalize_binding_pair(raw: Any) -> Dict[str, str]:
	pair = raw if _is_dict(raw) else {}
	return {
		"currentExecKey": _as_str(pair.get("currentExecKey")),
		"currentArtifactId": _as_str(pair.get("currentArtifactId")),
	}


def _normalize_basis_node(raw: Any) -> Dict[str, Any]:
	node = raw if _is_dict(raw) else {}
	upstream_raw = node.get("upstreamBindings") if _is_dict(node.get("upstreamBindings")) else {}
	upstream: Dict[str, Dict[str, str]] = {}
	for upstream_node_id, pair in upstream_raw.items():
		upstream[str(upstream_node_id)] = _normalize_binding_pair(pair)
	return {
		"nodeId": _as_str(node.get("nodeId")),
		"nodeStateHash": _as_str(node.get("nodeStateHash")),
		"determinismEnvHash": _as_str(node.get("determinismEnvHash")),
		"binding": _normalize_binding_pair(node.get("binding")),
		"upstreamBindings": upstream,
		"executionVersion": _as_str(node.get("executionVersion")),
	}


def compare_execution_contracts(
	*,
	expected_contract: Dict[str, Any],
	current_contract: Dict[str, Any],
) -> Dict[str, Any]:
	def _categories_for_reasons(codes: List[str]) -> List[str]:
		cats: set[str] = set()
		for code in codes:
			key = str(code or "").strip().lower()
			if key in {"graph_changed"}:
				cats.add("graph")
			elif key in {"node_state_changed"}:
				cats.add("node_params")
			elif key in {"env_changed"}:
				cats.add("env")
			elif key in {"dependency_frontier_changed"}:
				cats.add("artifact_lineage")
			elif key in {"execution_version_changed", "contract_version_changed"}:
				cats.add("engine_version")
			elif key:
				cats.add("contract")
		return sorted(cats)

	expected_ok, expected_errors = validate_execution_contract(expected_contract)
	current_ok, current_errors = validate_execution_contract(current_contract)
	if not expected_ok or not current_ok:
		reasons: List[str] = []
		if not expected_ok:
			reasons.extend([f"expected_contract_invalid:{err}" for err in expected_errors])
		if not current_ok:
			reasons.extend([f"current_contract_invalid:{err}" for err in current_errors])
		return {
			"ok": False,
			"reasonCodes": sorted(set(reasons)),
			"categories": _categories_for_reasons(reasons),
			"nodeIds": [],
			"mismatches": [
				{
					"nodeId": None,
					"reasonCode": "execution_contract_invalid",
					"changedFields": ["contract"],
					"expected": expected_contract if _is_dict(expected_contract) else {},
					"actual": current_contract if _is_dict(current_contract) else {},
				}
			],
		}

	mismatches: List[Dict[str, Any]] = []
	reason_codes: set[str] = set()

	expected_basis = expected_contract.get("basis") if _is_dict(expected_contract.get("basis")) else {}
	current_basis = current_contract.get("basis") if _is_dict(current_contract.get("basis")) else {}

	def _add_global_mismatch(reason_code: str, field: str, expected_value: Any, current_value: Any) -> None:
		reason_codes.add(reason_code)
		mismatches.append(
			{
				"nodeId": None,
				"reasonCode": reason_code,
				"changedFields": [field],
				"expected": {field: expected_value},
				"actual": {field: current_value},
			}
		)

	if int(expected_contract.get("contractVersion") or 0) != int(current_contract.get("contractVersion") or 0):
		_add_global_mismatch(
			"contract_version_changed",
			"contractVersion",
			int(expected_contract.get("contractVersion") or 0),
			int(current_contract.get("contractVersion") or 0),
		)
	if _as_str(expected_contract.get("graphId")) != _as_str(current_contract.get("graphId")):
		_add_global_mismatch(
			"graph_changed",
			"graphId",
			_as_str(expected_contract.get("graphId")),
			_as_str(current_contract.get("graphId")),
		)
	if _as_str(expected_basis.get("graphId")) != _as_str(current_basis.get("graphId")):
		_add_global_mismatch(
			"graph_changed",
			"basis.graphId",
			_as_str(expected_basis.get("graphId")),
			_as_str(current_basis.get("graphId")),
		)
	if _as_str(expected_basis.get("executionVersion")) != _as_str(current_basis.get("executionVersion")):
		_add_global_mismatch(
			"execution_version_changed",
			"basis.executionVersion",
			_as_str(expected_basis.get("executionVersion")),
			_as_str(current_basis.get("executionVersion")),
		)
	if _as_str(expected_basis.get("environmentHash")) != _as_str(current_basis.get("environmentHash")):
		_add_global_mismatch(
			"env_changed",
			"basis.environmentHash",
			_as_str(expected_basis.get("environmentHash")),
			_as_str(current_basis.get("environmentHash")),
		)

	expected_nodes = expected_basis.get("nodes") if _is_dict(expected_basis.get("nodes")) else {}
	current_nodes = current_basis.get("nodes") if _is_dict(current_basis.get("nodes")) else {}
	for node_id, raw_expected in expected_nodes.items():
		node_key = str(node_id)
		expected_node = _normalize_basis_node(raw_expected)
		current_node = _normalize_basis_node(current_nodes.get(node_key))
		changed_fields: List[str] = []
		node_reason_codes: set[str] = set()
		if expected_node["nodeStateHash"] != current_node["nodeStateHash"]:
			changed_fields.append("nodeStateHash")
			node_reason_codes.add("node_state_changed")
		if expected_node["determinismEnvHash"] != current_node["determinismEnvHash"]:
			changed_fields.append("determinismEnvHash")
			node_reason_codes.add("env_changed")
		if expected_node["executionVersion"] and expected_node["executionVersion"] != current_node["executionVersion"]:
			changed_fields.append("executionVersion")
			node_reason_codes.add("execution_version_changed")
		if expected_node["binding"] != current_node["binding"]:
			changed_fields.append("binding")
			node_reason_codes.add("dependency_frontier_changed")
		if expected_node["upstreamBindings"] != current_node["upstreamBindings"]:
			changed_fields.append("upstreamBindings")
			node_reason_codes.add("dependency_frontier_changed")
		if changed_fields:
			reason_codes.update(node_reason_codes)
			mismatches.append(
				{
					"nodeId": node_key,
					"reasonCode": sorted(node_reason_codes)[0] if node_reason_codes else "execution_contract_changed",
					"changedFields": changed_fields,
					"expected": expected_node,
					"actual": current_node,
				}
			)

	reason_codes_sorted = sorted(reason_codes)
	return {
		"ok": len(mismatches) == 0,
		"reasonCodes": reason_codes_sorted,
		"categories": _categories_for_reasons(reason_codes_sorted),
		"nodeIds": sorted({str(item.get("nodeId")) for item in mismatches if _as_str(item.get("nodeId"))}),
		"mismatches": mismatches,
	}
