from __future__ import annotations

import copy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .runner.capabilities import allowed_payload_types
from .schema_contracts import canonicalize_schema_envelope


def _normalize_payload_type(value: Any) -> Optional[str]:
	if value is None:
		return None
	norm = str(value).strip().lower()
	if norm == "string":
		norm = "text"
	if not norm:
		return None
	return norm if norm in set(allowed_payload_types()) else None


def _node_data(node: Dict[str, Any]) -> Dict[str, Any]:
	data = node.get("data")
	if not isinstance(data, dict):
		data = {}
		node["data"] = data
	return data


def _infer_edge_mode_from_handles(edge: Dict[str, Any]) -> str:
	source_handle = str(edge.get("sourceHandle") or "").strip().lower()
	target_handle = str(edge.get("targetHandle") or "").strip().lower()
	if (
		source_handle.startswith("control")
		or source_handle.startswith("ctl")
		or target_handle.startswith("control")
		or target_handle.startswith("ctl")
	):
		return "control"
	if source_handle.startswith("param") or target_handle.startswith("param"):
		return "param"
	return "work"


def _canonicalize_node_schema_contract(node: Dict[str, Any], notes: List[Dict[str, Any]]) -> None:
	data = _node_data(node)
	raw_schema = data.get("schema")
	legacy_expected_input_schema = (
		raw_schema.get("expectedInputSchema")
		if isinstance(raw_schema, dict) and isinstance(raw_schema.get("expectedInputSchema"), dict)
		else None
	)
	canonical_schema, changed = canonicalize_schema_envelope(raw_schema)
	if raw_schema is None:
		return
	if canonical_schema is None:
		data.pop("schema", None)
		notes.append(
			{
				"code": "NODE_SCHEMA_CONTRACT_DROPPED",
				"nodeId": str(node.get("id") or ""),
				"message": "Dropped invalid node.data.schema payload (must be an object).",
			}
		)
		return
	expected_input_schemas = (
		canonical_schema.get("expectedInputSchemas")
		if isinstance(canonical_schema.get("expectedInputSchemas"), dict)
		else None
	)
	if legacy_expected_input_schema is not None and expected_input_schemas is None:
		canonical_schema["expectedInputSchemas"] = {"in": copy.deepcopy(legacy_expected_input_schema)}
		changed = True
		notes.append(
			{
				"code": "NODE_SCHEMA_EXPECTED_INPUTS_MIGRATED",
				"nodeId": str(node.get("id") or ""),
				"message": "Migrated schema.expectedInputSchema to schema.expectedInputSchemas.in",
			}
		)
	if "expectedInputSchema" in canonical_schema:
		canonical_schema.pop("expectedInputSchema", None)
		changed = True
	data["schema"] = canonical_schema
	if changed:
		notes.append(
			{
				"code": "NODE_SCHEMA_CONTRACT_CANONICALIZED",
				"nodeId": str(node.get("id") or ""),
				"message": "Canonicalized node.data.schema payload.",
			}
		)


def _component_output_decls(node: Dict[str, Any]) -> List[Dict[str, Any]]:
	data = _node_data(node)
	params = data.get("params") if isinstance(data.get("params"), dict) else {}
	api = params.get("api") if isinstance(params.get("api"), dict) else {}
	outputs = api.get("outputs") if isinstance(api.get("outputs"), list) else []
	return [o for o in outputs if isinstance(o, dict)]


def _canonicalize_component_api_outputs_in_graph(node: Dict[str, Any], notes: List[Dict[str, Any]]) -> None:
	data = _node_data(node)
	if str(data.get("kind") or "").strip().lower() != "component":
		return
	params = data.get("params") if isinstance(data.get("params"), dict) else {}
	if not isinstance(params, dict):
		return
	api = params.get("api") if isinstance(params.get("api"), dict) else {}
	if not isinstance(api, dict):
		return
	outputs = api.get("outputs") if isinstance(api.get("outputs"), list) else []
	changed = False
	next_outputs: List[Dict[str, Any]] = []
	for raw in outputs:
		if not isinstance(raw, dict):
			next_outputs.append(raw)  # keep unknown shape untouched
			continue
		typed_schema = raw.get("typedSchema") if isinstance(raw.get("typedSchema"), dict) else {}
		typed_type = _normalize_payload_type(typed_schema.get("type")) or "json"
		fields = typed_schema.get("fields") if isinstance(typed_schema.get("fields"), list) else []
		if typed_type in {"text", "binary", "embeddings"}:
			fields = []
		next_out = copy.deepcopy(raw)
		for key in list(next_out.keys()):
			if str(key).strip().lower() == "porttype":
				next_out.pop(key, None)
		next_out["typedSchema"] = {"type": typed_type, "fields": fields}
		if next_out != raw:
			changed = True
		next_outputs.append(next_out)
	if changed:
		api["outputs"] = next_outputs
		params["api"] = api
		data["params"] = params
		notes.append(
			{
				"code": "COMPONENT_API_OUTPUTS_CANONICALIZED",
				"nodeId": str(node.get("id") or ""),
				"message": "Canonicalized component api.outputs typedSchema and normalized fields.",
			}
		)


def _component_output_names(node: Dict[str, Any]) -> List[str]:
	out: List[str] = []
	for decl in _component_output_decls(node):
		name = str(decl.get("name") or "").strip()
		if name:
			out.append(name)
	return out


def _component_output_payload_type(node: Dict[str, Any], output_name: str) -> Optional[str]:
	target = str(output_name or "").strip()
	if not target:
		return None
	for decl in _component_output_decls(node):
		if str(decl.get("name") or "").strip() != target:
			continue
		return _normalize_payload_type(((decl.get("typedSchema") or {}) if isinstance(decl.get("typedSchema"), dict) else {}).get("type"))
	return None


def _canonicalize_builtin_tool_params(node: Dict[str, Any], notes: List[Dict[str, Any]]) -> None:
	data = _node_data(node)
	if str(data.get("kind") or "").strip().lower() != "tool":
		return
	params = data.get("params") if isinstance(data.get("params"), dict) else {}
	if not isinstance(params, dict):
		return
	if str(params.get("provider") or "").strip().lower() != "builtin":
		return
	builtin = params.get("builtin") if isinstance(params.get("builtin"), dict) else {}
	if not isinstance(builtin, dict):
		builtin = {}

	changed = False
	profile_id = str(builtin.get("profileId") or "").strip()
	if not profile_id:
		builtin["profileId"] = "core"
		changed = True

	custom_packages = builtin.get("customPackages")
	if custom_packages is None:
		builtin["customPackages"] = []
		changed = True
	elif not isinstance(custom_packages, list):
		builtin["customPackages"] = []
		changed = True
	else:
		normalized_packages = [str(pkg).strip() for pkg in custom_packages if isinstance(pkg, str) and str(pkg).strip()]
		if normalized_packages != custom_packages:
			builtin["customPackages"] = normalized_packages
			changed = True

	locked = builtin.get("locked")
	if locked is not None:
		locked_norm = str(locked).strip() if isinstance(locked, str) else ""
		if locked_norm:
			if locked_norm != locked:
				builtin["locked"] = locked_norm
				changed = True
		else:
			builtin.pop("locked", None)
			changed = True

	if changed:
		params["builtin"] = builtin
		data["params"] = params
		notes.append(
			{
				"code": "TOOL_BUILTIN_PARAMS_CANONICALIZED",
				"nodeId": str(node.get("id") or ""),
				"message": "Normalized builtin profile defaults (profileId/customPackages/locked).",
			}
		)


def canonicalize_graph_payload(raw: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
	notes: List[Dict[str, Any]] = []
	graph = copy.deepcopy(raw if isinstance(raw, dict) else {})
	if not isinstance(graph.get("nodes"), list):
		graph["nodes"] = []
		notes.append({"code": "GRAPH_NODES_DEFAULTED", "message": "graph.nodes defaulted to []"})
	if not isinstance(graph.get("edges"), list):
		graph["edges"] = []
		notes.append({"code": "GRAPH_EDGES_DEFAULTED", "message": "graph.edges defaulted to []"})

	canonical_nodes: List[Dict[str, Any]] = []
	node_map: Dict[str, Dict[str, Any]] = {}
	for idx, node in enumerate(graph.get("nodes", [])):
		if not isinstance(node, dict):
			notes.append({"code": "NODE_DROPPED", "message": f"graph.nodes[{idx}] dropped (not an object)"})
			continue
		next_node = copy.deepcopy(node)
		data = _node_data(next_node)
		kind = str(data.get("kind") or "").strip().lower()
		if kind:
			data["kind"] = kind
		processing_policy = data.get("processingPolicy") if isinstance(data.get("processingPolicy"), dict) else {}
		consume_mode = str(
			processing_policy.get("consume_mode")
			or processing_policy.get("consumeMode")
			or "once"
		).strip().lower() or "once"
		if consume_mode in {"read_once"}:
			consume_mode = "once"
		elif consume_mode in {"continuous"}:
			consume_mode = "single_item"
		elif consume_mode not in {"once", "single_item", "batch"}:
			consume_mode = "once"
		try:
			batch_size = int(
				processing_policy.get("batch_size")
				or processing_policy.get("batchSize")
				or 1
			)
		except Exception:
			batch_size = 1
		try:
			max_inflight = int(
				processing_policy.get("max_inflight")
				or processing_policy.get("maxInflight")
				or 1
			)
		except Exception:
			max_inflight = 1
		input_handles: Dict[str, Dict[str, Any]] = {}
		input_handles_raw = (
			processing_policy.get("input_handles")
			if isinstance(processing_policy.get("input_handles"), dict)
			else {}
		)
		for handle, policy_raw in (input_handles_raw or {}).items():
			if not isinstance(policy_raw, dict):
				continue
			handle_name = str(handle or "").strip()
			if not handle_name:
				continue
			handle_mode = str(
				policy_raw.get("consume_mode")
				or policy_raw.get("consumeMode")
				or consume_mode
			).strip().lower() or consume_mode
			if handle_mode in {"read_once"}:
				handle_mode = "once"
			elif handle_mode in {"continuous"}:
				handle_mode = "single_item"
			elif handle_mode not in {"once", "single_item", "batch"}:
				handle_mode = consume_mode
			try:
				handle_batch = int(
					policy_raw.get("batch_size")
					or policy_raw.get("batchSize")
					or batch_size
				)
			except Exception:
				handle_batch = batch_size
			try:
				handle_inflight = int(
					policy_raw.get("max_inflight")
					or policy_raw.get("maxInflight")
					or max_inflight
				)
			except Exception:
				handle_inflight = max_inflight
			input_handles[handle_name] = {
				"consume_mode": handle_mode,
				"batch_size": max(1, handle_batch),
				"max_inflight": max(1, handle_inflight),
			}
		data["processingPolicy"] = {
			"consume_mode": consume_mode,
			"batch_size": max(1, batch_size),
			"max_inflight": max(1, max_inflight),
			"input_handles": input_handles,
		}
		nid = str(next_node.get("id") or "").strip()
		if nid:
			node_map[nid] = next_node
		_canonicalize_builtin_tool_params(next_node, notes)
		_canonicalize_node_schema_contract(next_node, notes)
		canonical_nodes.append(next_node)
	graph["nodes"] = canonical_nodes

	# Normalize component bindings against declared API outputs.
	for node in canonical_nodes:
		_canonicalize_component_api_outputs_in_graph(node, notes)
		data = _node_data(node)
		if str(data.get("kind") or "").strip().lower() != "component":
			continue
		params = data.get("params") if isinstance(data.get("params"), dict) else {}
		if not isinstance(params, dict):
			continue
		bindings = params.get("bindings") if isinstance(params.get("bindings"), dict) else {}
		output_bindings = bindings.get("outputs") if isinstance(bindings.get("outputs"), dict) else {}
		output_names = _component_output_names(node)
		if not output_names:
			continue
		dangling = [k for k in list(output_bindings.keys()) if k not in set(output_names)]
		for key in dangling:
			output_bindings.pop(key, None)
		if dangling:
			notes.append(
				{
					"code": "COMPONENT_BINDING_PRUNED",
					"nodeId": str(node.get("id") or ""),
					"message": f"Pruned dangling output bindings: {', '.join(sorted(dangling))}",
				}
			)
		bindings["outputs"] = output_bindings
		params["bindings"] = bindings
		data["params"] = params

	# Normalize edges, handles, and contracts.
	canonical_edges: List[Dict[str, Any]] = []
	for idx, edge in enumerate(graph.get("edges", [])):
		if not isinstance(edge, dict):
			notes.append({"code": "EDGE_DROPPED", "message": f"graph.edges[{idx}] dropped (not an object)"})
			continue
		next_edge = copy.deepcopy(edge)
		src_id = str(next_edge.get("source") or "").strip()
		tgt_id = str(next_edge.get("target") or "").strip()
		if not src_id or not tgt_id:
			canonical_edges.append(next_edge)
			continue
		src_node = node_map.get(src_id)
		tgt_node = node_map.get(tgt_id)
		src_kind = str((_node_data(src_node).get("kind") if src_node else "") or "").strip().lower()
		edge_data = next_edge.get("data") if isinstance(next_edge.get("data"), dict) else {}
		raw_mode = str(edge_data.get("mode") or "").strip().lower()
		inferred_mode = _infer_edge_mode_from_handles(next_edge)
		normalized_mode = raw_mode or inferred_mode
		if normalized_mode not in {"work", "param", "control"}:
			normalized_mode = inferred_mode
			notes.append(
				{
					"code": "EDGE_MODE_DEFAULTED",
					"edgeId": str(next_edge.get("id") or ""),
					"message": f"Edge mode defaulted to {normalized_mode}",
				}
			)
		elif not raw_mode:
			notes.append(
				{
					"code": "EDGE_MODE_INFERRED",
					"edgeId": str(next_edge.get("id") or ""),
					"message": f"Inferred edge mode '{normalized_mode}' from handles",
				}
			)
		edge_data["mode"] = normalized_mode
		edge_data["fatal"] = bool(edge_data.get("fatal", False))
		queue_cfg = edge_data.get("queue") if isinstance(edge_data.get("queue"), dict) else {}
		queue_overflow = str(queue_cfg.get("overflow") or "block").strip().lower() or "block"
		if queue_overflow not in {"block", "spill", "error"}:
			queue_overflow = "block"
		try:
			queue_max = int(queue_cfg.get("max", 1000))
		except Exception:
			queue_max = 1000
		queue_cfg["overflow"] = queue_overflow
		queue_cfg["max"] = max(1, queue_max)
		edge_data["queue"] = queue_cfg
		work_cfg = edge_data.get("work") if isinstance(edge_data.get("work"), dict) else {}
		item_mode = str(work_cfg.get("item_mode") or work_cfg.get("itemMode") or "artifact").strip().lower() or "artifact"
		if item_mode not in {"artifact", "json_items", "table_rows"}:
			item_mode = "artifact"
		try:
			work_max_items = int(work_cfg.get("max_items") or work_cfg.get("maxItems") or 256)
		except Exception:
			work_max_items = 256
		work_cfg["item_mode"] = item_mode
		work_cfg["max_items"] = max(1, work_max_items)
		edge_data["work"] = work_cfg
		next_edge["data"] = edge_data

		source_handle = str(next_edge.get("sourceHandle") or "out").strip() or "out"
		if src_kind == "component" and src_node is not None:
			output_names = _component_output_names(src_node)
			output_decls = _component_output_decls(src_node)
			output_by_name = {
				str(decl.get("name") or "").strip(): _normalize_payload_type(
					((decl.get("typedSchema") or {}) if isinstance(decl.get("typedSchema"), dict) else {}).get("type")
				)
				for decl in output_decls
				if isinstance(decl, dict)
			}
			bindings = (
				((_node_data(src_node).get("params") or {}).get("bindings") or {})
				if isinstance((_node_data(src_node).get("params") or {}), dict)
				else {}
			)
			binding_outputs = bindings.get("outputs") if isinstance(bindings, dict) and isinstance(bindings.get("outputs"), dict) else {}
			declared_binding_names = [n for n in output_names if n in set(binding_outputs.keys())]
			canonical_handle = source_handle
			if canonical_handle == "out":
				if len(output_names) == 1:
					canonical_handle = output_names[0]
				elif len(declared_binding_names) == 1:
					canonical_handle = declared_binding_names[0]
				else:
					contract = edge_data.get("contract") if isinstance(edge_data.get("contract"), dict) else {}
					contract_out = _normalize_payload_type(contract.get("out"))
					if contract_out:
						candidates = [name for name in output_names if output_by_name.get(name) == contract_out]
						if len(candidates) == 1:
							canonical_handle = candidates[0]
			elif canonical_handle not in set(output_names):
				if len(output_names) == 1:
					canonical_handle = output_names[0]
				elif len(declared_binding_names) == 1:
					canonical_handle = declared_binding_names[0]
				else:
					contract = edge_data.get("contract") if isinstance(edge_data.get("contract"), dict) else {}
					contract_out = _normalize_payload_type(contract.get("out"))
					if contract_out:
						candidates = [name for name in output_names if output_by_name.get(name) == contract_out]
						if len(candidates) == 1:
							canonical_handle = candidates[0]
			if canonical_handle != source_handle:
				next_edge["sourceHandle"] = canonical_handle
				notes.append(
					{
						"code": "COMPONENT_EDGE_HANDLE_NORMALIZED",
						"edgeId": str(next_edge.get("id") or ""),
						"message": f"Normalized sourceHandle {source_handle} -> {canonical_handle}",
					}
				)
				source_handle = canonical_handle

		canonical_edges.append(next_edge)
	graph["edges"] = canonical_edges
	return graph, notes


def find_component_edge_handle_errors(graph: Dict[str, Any]) -> List[Dict[str, str]]:
	errors: List[Dict[str, str]] = []
	nodes = graph.get("nodes", []) if isinstance(graph, dict) else []
	edges = graph.get("edges", []) if isinstance(graph, dict) else []
	if not isinstance(nodes, list) or not isinstance(edges, list):
		return errors
	node_map: Dict[str, Dict[str, Any]] = {}
	for node in nodes:
		if not isinstance(node, dict):
			continue
		node_id = str(node.get("id") or "").strip()
		if node_id:
			node_map[node_id] = node
	for edge in edges:
		if not isinstance(edge, dict):
			continue
		source_id = str(edge.get("source") or "").strip()
		if not source_id:
			continue
		source_node = node_map.get(source_id)
		if source_node is None:
			continue
		source_kind = str((_node_data(source_node).get("kind") or "")).strip().lower()
		if source_kind != "component":
			continue
		output_names = _component_output_names(source_node)
		if len(output_names) <= 1:
			continue
		handle = str(edge.get("sourceHandle") or "out").strip() or "out"
		if handle == "out" or handle not in set(output_names):
			errors.append(
				{
					"code": "COMPONENT_OUTPUT_HANDLE_UNRESOLVED",
					"edgeId": str(edge.get("id") or ""),
					"sourceNodeId": source_id,
					"sourceHandle": handle,
					"message": "Multi-output component edges must use an explicit declared sourceHandle.",
				}
			)
	return errors


def migrate_llm_nodes_to_model(raw: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
	"""Normalize graph node kind from legacy `llm` to `model` and return a dry-run report."""
	payload = copy.deepcopy(raw if isinstance(raw, dict) else {})
	target_graph = payload.get("graph") if isinstance(payload.get("graph"), dict) else payload
	nodes = target_graph.get("nodes") if isinstance(target_graph, dict) and isinstance(target_graph.get("nodes"), list) else []
	converted_ids: List[str] = []
	already_model_ids: List[str] = []
	llm_count = 0
	for node in nodes:
		if not isinstance(node, dict):
			continue
		data = _node_data(node)
		kind = str(data.get("kind") or "").strip().lower()
		nid = str(node.get("id") or "")
		if kind == "llm":
			llm_count += 1
			data["kind"] = "model"
			converted_ids.append(nid)
		elif kind == "model":
			already_model_ids.append(nid)
	report = {
		"migratedAt": datetime.now(timezone.utc).isoformat(),
		"totalNodes": len([n for n in nodes if isinstance(n, dict)]),
		"llmNodesFound": llm_count,
		"convertedNodeIds": converted_ids,
		"alreadyModelNodeIds": already_model_ids,
		"idempotent": llm_count == 0,
	}
	return payload, report
