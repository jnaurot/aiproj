from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from .execution_contract import validate_execution_contract


PAUSE_SNAPSHOT_SCHEMA_VERSION = 2


def _is_dict(value: Any) -> bool:
	return isinstance(value, dict)


def _is_list(value: Any) -> bool:
	return isinstance(value, list)


def _as_str(value: Any) -> str:
	return str(value or "").strip()


def _as_str_list(value: Any) -> List[str]:
	if not _is_list(value):
		return []
	return [str(item).strip() for item in value if str(item).strip()]


def validate_pause_snapshot_schema(snapshot: Dict[str, Any]) -> Tuple[bool, List[str]]:
	errors: List[str] = []
	if not _is_dict(snapshot):
		return False, ["snapshot_not_object"]
	if int(snapshot.get("schemaVersion") or 0) != PAUSE_SNAPSHOT_SCHEMA_VERSION:
		errors.append("schema_version_mismatch")
	if not _as_str(snapshot.get("runId")):
		errors.append("missing_run_id")
	if not _as_str(snapshot.get("graphId")):
		errors.append("missing_graph_id")
	if not _is_dict(snapshot.get("graph")):
		errors.append("missing_graph")
	if _as_str(snapshot.get("lifecycleState")) != "paused":
		errors.append("invalid_lifecycle_state")
	if not _is_dict(snapshot.get("state")):
		errors.append("missing_state")
	else:
		state = snapshot.get("state") if _is_dict(snapshot.get("state")) else {}
		control_plane = state.get("controlPlane")
		if control_plane is not None:
			if not _is_dict(control_plane):
				errors.append("state_control_plane_invalid")
			else:
				edge_state = control_plane.get("edgeControlState")
				if not _is_dict(edge_state):
					errors.append("state_control_plane_missing_edge_state")
				else:
					for edge_id, raw_edge in edge_state.items():
						if not _is_dict(raw_edge):
							errors.append(f"state_control_plane_edge_invalid:{edge_id}")
							continue
						try:
							depth = int((raw_edge or {}).get("depth") or 0)
						except Exception:
							depth = -1
						if depth < 0:
							errors.append(f"state_control_plane_edge_depth_invalid:{edge_id}")
						try:
							last_seq = int((raw_edge or {}).get("lastSeq") or 0)
						except Exception:
							last_seq = -1
						if last_seq < 0:
							errors.append(f"state_control_plane_edge_last_seq_invalid:{edge_id}")
				last_seq_raw = control_plane.get("lastSeq")
				if last_seq_raw is not None:
					try:
						last_seq = int(last_seq_raw)
					except Exception:
						last_seq = -1
					if last_seq < 0:
						errors.append("state_control_plane_last_seq_invalid")
				lease_nodes = control_plane.get("activeLeaseNodeIds")
				if lease_nodes is not None and not _is_list(lease_nodes):
					errors.append("state_control_plane_active_lease_nodes_invalid")
		runtime_item_metrics = state.get("runtimeItemMetrics")
		if runtime_item_metrics is not None:
			if not _is_dict(runtime_item_metrics):
				errors.append("state_runtime_item_metrics_invalid")
			else:
				node_counters = runtime_item_metrics.get("nodeCounters")
				if node_counters is not None and not _is_dict(node_counters):
					errors.append("state_runtime_item_metrics_node_counters_invalid")
		runtime_node_metrics = state.get("nodeRuntimeMetrics")
		if runtime_node_metrics is not None and not _is_dict(runtime_node_metrics):
			errors.append("state_runtime_node_metrics_invalid")
		runtime_totals = state.get("runtimeTotals")
		if runtime_totals is not None:
			if not _is_dict(runtime_totals):
				errors.append("state_runtime_totals_invalid")
			else:
				for key in ("cached", "succeeded", "failed", "softFailed", "peakConcurrency"):
					value = runtime_totals.get(key)
					if value is None:
						continue
					try:
						if int(value) < 0:
							errors.append(f"state_runtime_totals_negative:{key}")
					except Exception:
						errors.append(f"state_runtime_totals_invalid_value:{key}")
	basis = snapshot.get("frontierValidationBasis")
	if not _is_dict(basis):
		errors.append("missing_frontier_validation_basis")
	else:
		if not _as_str(basis.get("graphId")):
			errors.append("basis_missing_graph_id")
		if not _as_str(basis.get("executionVersion")):
			errors.append("basis_missing_execution_version")
		nodes = basis.get("nodes")
		if not _is_dict(nodes):
			errors.append("basis_missing_nodes")
		else:
			for node_id, raw in nodes.items():
				if not _is_dict(raw):
					errors.append(f"basis_node_invalid:{node_id}")
					continue
				if not _as_str(raw.get("nodeStateHash")):
					errors.append(f"basis_node_missing_state_hash:{node_id}")
				if not _as_str(raw.get("determinismEnvHash")):
					errors.append(f"basis_node_missing_env_hash:{node_id}")
				if not _is_dict(raw.get("upstreamBindings")):
					errors.append(f"basis_node_missing_upstream_bindings:{node_id}")
				binding = raw.get("binding")
				if not _is_dict(binding):
					errors.append(f"basis_node_missing_binding:{node_id}")
	lease_state = snapshot.get("leaseState")
	if not _is_dict(lease_state):
		errors.append("missing_lease_state")
	else:
		if bool(lease_state.get("released")) is not True:
			errors.append("lease_not_released")
		try:
			active = int(lease_state.get("activeLeases") or 0)
		except Exception:
			active = -1
		if active != 0:
			errors.append("lease_active_count_nonzero")
	execution_contract = snapshot.get("executionContract")
	if _is_dict(execution_contract):
		contract_ok, contract_errors = validate_execution_contract(execution_contract)
		if not contract_ok:
			for err in contract_errors:
				errors.append(f"execution_contract_invalid:{err}")
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


def validate_resume_identity_basis(
	*,
	expected_basis: Dict[str, Any],
	current_basis: Dict[str, Any],
) -> Dict[str, Any]:
	mismatches: List[Dict[str, Any]] = []
	reason_codes: set[str] = set()

	expected_graph_id = _as_str(expected_basis.get("graphId"))
	current_graph_id = _as_str(current_basis.get("graphId"))
	if expected_graph_id != current_graph_id:
		reason_codes.add("graph_changed")
		mismatches.append(
			{
				"nodeId": None,
				"reasonCode": "graph_changed",
				"changedFields": ["graphId"],
				"expected": {"graphId": expected_graph_id},
				"actual": {"graphId": current_graph_id},
			}
		)

	expected_version = _as_str(expected_basis.get("executionVersion"))
	current_version = _as_str(current_basis.get("executionVersion"))
	if expected_version != current_version:
		reason_codes.add("execution_version_changed")
		mismatches.append(
			{
				"nodeId": None,
				"reasonCode": "execution_version_changed",
				"changedFields": ["executionVersion"],
				"expected": {"executionVersion": expected_version},
				"actual": {"executionVersion": current_version},
			}
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
			expected_binding_empty = (
				not _as_str((expected_node.get("binding") or {}).get("currentExecKey"))
				and not _as_str((expected_node.get("binding") or {}).get("currentArtifactId"))
			)
			current_binding_populated = (
				bool(_as_str((current_node.get("binding") or {}).get("currentExecKey")))
				or bool(_as_str((current_node.get("binding") or {}).get("currentArtifactId")))
			)
			if expected_binding_empty and current_binding_populated:
				node_reason_codes.add("snapshot_binding_empty_mismatch")
		if expected_node["upstreamBindings"] != current_node["upstreamBindings"]:
			changed_fields.append("upstreamBindings")
			node_reason_codes.add("dependency_frontier_changed")
			union_upstream_nodes = set((expected_node.get("upstreamBindings") or {}).keys()) | set(
				(current_node.get("upstreamBindings") or {}).keys()
			)
			for upstream_node_id in union_upstream_nodes:
				exp_pair = (expected_node.get("upstreamBindings") or {}).get(str(upstream_node_id), {})
				act_pair = (current_node.get("upstreamBindings") or {}).get(str(upstream_node_id), {})
				exp_empty = (
					not _as_str((exp_pair or {}).get("currentExecKey"))
					and not _as_str((exp_pair or {}).get("currentArtifactId"))
				)
				act_populated = (
					bool(_as_str((act_pair or {}).get("currentExecKey")))
					or bool(_as_str((act_pair or {}).get("currentArtifactId")))
				)
				if exp_empty and act_populated:
					node_reason_codes.add("snapshot_upstream_binding_empty_mismatch")
					break
		if changed_fields:
			reason = sorted(node_reason_codes)[0] if node_reason_codes else "resume_validation_failed"
			reason_codes.update(node_reason_codes)
			mismatches.append(
				{
					"nodeId": node_key,
					"reasonCode": reason,
					"changedFields": changed_fields,
					"expected": expected_node,
					"actual": current_node,
				}
			)

	return {
		"ok": len(mismatches) == 0,
		"reasonCodes": sorted(reason_codes),
		"mismatches": mismatches,
		"nodeIds": sorted({str(item.get("nodeId")) for item in mismatches if _as_str(item.get("nodeId"))}),
	}


def snapshot_resume_failure_details(validation: Dict[str, Any]) -> Dict[str, Any]:
	mismatches = validation.get("mismatches") if _is_list(validation.get("mismatches")) else []
	reason_codes = validation.get("reasonCodes") if _is_list(validation.get("reasonCodes")) else []
	node_ids = validation.get("nodeIds") if _is_list(validation.get("nodeIds")) else []
	return {
		"reasonCodes": [str(code) for code in reason_codes if _as_str(code)],
		"nodeIds": [str(node_id) for node_id in node_ids if _as_str(node_id)],
		"mismatches": mismatches,
	}
