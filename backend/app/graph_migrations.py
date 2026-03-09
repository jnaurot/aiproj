from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional, Tuple

from .runner.capabilities import allowed_port_types


def _normalize_port_type(value: Any) -> Optional[str]:
	if value is None:
		return None
	norm = str(value).strip().lower()
	if not norm:
		return None
	return norm if norm in set(allowed_port_types()) else None


def _payload_type_for_port_type(port_type: Optional[str]) -> str:
	pt = str(port_type or "").strip().lower()
	if pt == "text":
		return "string"
	if pt == "json":
		return "json"
	if pt == "table":
		return "table"
	if pt == "binary":
		return "binary"
	if pt == "embeddings":
		return "embeddings"
	return "unknown"


def _node_data(node: Dict[str, Any]) -> Dict[str, Any]:
	data = node.get("data")
	if not isinstance(data, dict):
		data = {}
		node["data"] = data
	return data


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
		port_type = _normalize_port_type(raw.get("portType")) or "json"
		typed_schema = raw.get("typedSchema") if isinstance(raw.get("typedSchema"), dict) else {}
		typed_type = _normalize_port_type(typed_schema.get("type")) or port_type
		fields = typed_schema.get("fields") if isinstance(typed_schema.get("fields"), list) else []
		if typed_type != port_type:
			typed_type = port_type
		if typed_type in {"text", "binary", "embeddings"}:
			fields = []
		next_out = copy.deepcopy(raw)
		next_out["portType"] = port_type
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
				"message": "Aligned component api.outputs typedSchema.type with portType and normalized fields.",
			}
		)


def _component_output_names(node: Dict[str, Any]) -> List[str]:
	out: List[str] = []
	for decl in _component_output_decls(node):
		name = str(decl.get("name") or "").strip()
		if name:
			out.append(name)
	return out


def _component_output_port_type(node: Dict[str, Any], output_name: str) -> Optional[str]:
	target = str(output_name or "").strip()
	if not target:
		return None
	for decl in _component_output_decls(node):
		if str(decl.get("name") or "").strip() != target:
			continue
		return _normalize_port_type(decl.get("portType"))
	return None


def _node_in_port_type(node: Dict[str, Any]) -> Optional[str]:
	data = _node_data(node)
	ports = data.get("ports") if isinstance(data.get("ports"), dict) else {}
	return _normalize_port_type(ports.get("in"))


def _node_out_port_type(node: Dict[str, Any]) -> Optional[str]:
	data = _node_data(node)
	kind = str(data.get("kind") or "").strip().lower()
	if kind == "component":
		# Component source edges are per-output and must use output declarations.
		return None
	ports = data.get("ports") if isinstance(data.get("ports"), dict) else {}
	return _normalize_port_type(ports.get("out"))


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
		ports = data.get("ports")
		if isinstance(ports, dict):
			for direction in ("in", "out"):
				ports[direction] = _normalize_port_type(ports.get(direction))
		if kind == "component":
			# Component output routing/type is declared per API output handle.
			# Keep ports.out non-authoritative in persisted metadata.
			if not isinstance(ports, dict):
				ports = {}
				data["ports"] = ports
			ports["out"] = None
		nid = str(next_node.get("id") or "").strip()
		if nid:
			node_map[nid] = next_node
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
		if len(output_names) == 1:
			only_name = output_names[0]
			if only_name not in output_bindings and isinstance(output_bindings.get("out_data"), dict):
				output_bindings[only_name] = copy.deepcopy(output_bindings["out_data"])
				notes.append(
					{
						"code": "COMPONENT_BINDING_RENAMED",
						"nodeId": str(node.get("id") or ""),
						"message": f"Renamed legacy output binding out_data -> {only_name}",
					}
				)
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

		source_handle = str(next_edge.get("sourceHandle") or "out").strip() or "out"
		if src_kind == "component" and src_node is not None:
			output_names = _component_output_names(src_node)
			output_decls = _component_output_decls(src_node)
			output_by_name = {
				str(decl.get("name") or "").strip(): _normalize_port_type(decl.get("portType"))
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
					contract_out = _normalize_port_type(contract.get("out"))
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
					contract_out = _normalize_port_type(contract.get("out"))
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

		source_port_type = (
			_component_output_port_type(src_node, str(next_edge.get("sourceHandle") or "out"))
			if src_kind == "component" and src_node is not None
			else _node_out_port_type(src_node) if src_node is not None else None
		)
		target_port_type = _node_in_port_type(tgt_node) if tgt_node is not None else None
		if source_port_type and target_port_type:
			contract = edge_data.get("contract") if isinstance(edge_data.get("contract"), dict) else {}
			payload = contract.get("payload") if isinstance(contract.get("payload"), dict) else {}
			source_payload = payload.get("source") if isinstance(payload.get("source"), dict) else {}
			target_payload = payload.get("target") if isinstance(payload.get("target"), dict) else {}
			source_payload["type"] = _payload_type_for_port_type(source_port_type)
			target_payload["type"] = _payload_type_for_port_type(target_port_type)
			payload["source"] = source_payload
			payload["target"] = target_payload
			contract["out"] = source_port_type
			contract["in"] = target_port_type
			contract["payload"] = payload
			edge_data["contract"] = contract
			next_edge["data"] = edge_data
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
