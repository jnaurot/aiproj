from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple


class ComponentExpansionError(RuntimeError):
    def __init__(self, message: str, *, code: str = "COMPONENT_EXPANSION_FAILED", details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.code = code
        self.details = details or {}


@dataclass
class ExpandedComponentGraph:
    graph: Dict[str, Any]
    internal_to_parent: Dict[str, str]
    parent_to_internal: Dict[str, List[str]]
    parent_component_meta: Dict[str, Dict[str, Any]]


def _node_map(graph: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {
        str(n.get("id")): n
        for n in (graph.get("nodes", []) if isinstance(graph, dict) else [])
        if isinstance(n, dict) and str(n.get("id") or "").strip()
    }


def _edge_list(graph: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [e for e in (graph.get("edges", []) if isinstance(graph, dict) else []) if isinstance(e, dict)]


def _roots_and_leaves(nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
    node_ids = [str(n.get("id")) for n in nodes if isinstance(n, dict) and str(n.get("id") or "").strip()]
    in_deg: Dict[str, int] = {nid: 0 for nid in node_ids}
    out_deg: Dict[str, int] = {nid: 0 for nid in node_ids}
    for e in edges:
        src = str(e.get("source") or "")
        tgt = str(e.get("target") or "")
        if src in out_deg:
            out_deg[src] += 1
        if tgt in in_deg:
            in_deg[tgt] += 1
    roots = sorted([nid for nid, deg in in_deg.items() if deg == 0])
    leaves = sorted([nid for nid, deg in out_deg.items() if deg == 0])
    return roots, leaves


def _prefixed_node_id(instance_node_id: str, internal_node_id: str) -> str:
    return f"cmp:{instance_node_id}:{internal_node_id}"


def _prefixed_edge_id(instance_node_id: str, edge_id: str, idx: int) -> str:
    base = str(edge_id or f"edge_{idx}")
    return f"ce:{instance_node_id}:{base}"


def expand_graph_components(
    graph: Dict[str, Any],
    *,
    component_store: Any,
    max_depth: int = 5,
    _depth: int = 0,
    _component_stack: Optional[List[str]] = None,
) -> ExpandedComponentGraph:
    stack = list(_component_stack or [])
    if _depth > max_depth:
        raise ComponentExpansionError(
            f"Component expansion depth exceeds max_depth={max_depth}",
            code="COMPONENT_MAX_DEPTH_EXCEEDED",
            details={"depth": _depth, "maxDepth": max_depth},
        )

    nodes = [copy.deepcopy(n) for n in (graph.get("nodes", []) if isinstance(graph, dict) else []) if isinstance(n, dict)]
    edges = [copy.deepcopy(e) for e in _edge_list(graph)]
    components = [n for n in nodes if str(((n.get("data") or {}).get("kind") or "")) == "component"]
    if component_store is None and components:
        raise ComponentExpansionError(
            "Component revision store unavailable",
            code="COMPONENT_STORE_UNAVAILABLE",
            details={"componentNodeCount": len(components)},
        )
    if component_store is None:
        return ExpandedComponentGraph(
            graph=copy.deepcopy(graph),
            internal_to_parent={},
            parent_to_internal={},
            parent_component_meta={},
        )
    if not components:
        return ExpandedComponentGraph(
            graph={"nodes": nodes, "edges": edges},
            internal_to_parent={},
            parent_to_internal={},
            parent_component_meta={},
        )

    out_nodes: List[Dict[str, Any]] = []
    out_edges: List[Dict[str, Any]] = []
    internal_to_parent: Dict[str, str] = {}
    parent_to_internal: Dict[str, List[str]] = {}
    parent_component_meta: Dict[str, Dict[str, Any]] = {}

    component_ids: Set[str] = {str(c.get("id")) for c in components}
    non_component_nodes = [n for n in nodes if str(n.get("id")) not in component_ids]
    out_nodes.extend(non_component_nodes)

    incoming_by_target: Dict[str, List[Dict[str, Any]]] = {}
    outgoing_by_source: Dict[str, List[Dict[str, Any]]] = {}
    for e in edges:
        src = str(e.get("source") or "")
        tgt = str(e.get("target") or "")
        incoming_by_target.setdefault(tgt, []).append(e)
        outgoing_by_source.setdefault(src, []).append(e)

    for e in edges:
        src = str(e.get("source") or "")
        tgt = str(e.get("target") or "")
        if src in component_ids or tgt in component_ids:
            continue
        out_edges.append(e)

    for component_node in components:
        instance_node_id = str(component_node.get("id") or "").strip()
        data = component_node.get("data", {}) if isinstance(component_node.get("data"), dict) else {}
        params = data.get("params", {}) if isinstance(data.get("params"), dict) else {}
        component_ref = params.get("componentRef") if isinstance(params.get("componentRef"), dict) else {}
        component_id = str(component_ref.get("componentId") or "").strip()
        revision_id = str(component_ref.get("revisionId") or "").strip()

        if not component_id or not revision_id:
            raise ComponentExpansionError(
                f"Component node '{instance_node_id}' is missing componentRef.componentId/revisionId",
                code="MISSING_COMPONENT_REFERENCE",
                details={"nodeId": instance_node_id},
            )
        if component_id in stack:
            raise ComponentExpansionError(
                f"Recursive component reference detected for component '{component_id}'",
                code="COMPONENT_RECURSION_DETECTED",
                details={"componentId": component_id, "stack": stack},
            )

        revision = component_store.get_revision(component_id, revision_id)
        if revision is None:
            raise ComponentExpansionError(
                f"Component revision not found: {component_id}@{revision_id}",
                code="COMPONENT_REVISION_NOT_FOUND",
                details={"componentId": component_id, "revisionId": revision_id},
            )
        definition = revision.definition if isinstance(revision.definition, dict) else {}
        internal_graph = definition.get("graph") if isinstance(definition.get("graph"), dict) else {}
        internal_nodes = [
            copy.deepcopy(n)
            for n in (internal_graph.get("nodes", []) if isinstance(internal_graph, dict) else [])
            if isinstance(n, dict)
        ]
        internal_edges = [copy.deepcopy(e) for e in _edge_list(internal_graph)]

        if any(str(((n.get("data") or {}).get("kind") or "")) == "component" for n in internal_nodes):
            raise ComponentExpansionError(
                f"Nested component nodes are not supported in v1 for component '{component_id}'",
                code="COMPONENT_NESTING_UNSUPPORTED",
                details={"componentId": component_id, "revisionId": revision_id},
            )
        if not internal_nodes:
            raise ComponentExpansionError(
                f"Component definition has no internal nodes: {component_id}@{revision_id}",
                code="COMPONENT_EMPTY_DEFINITION",
                details={"componentId": component_id, "revisionId": revision_id},
            )

        roots, leaves = _roots_and_leaves(internal_nodes, internal_edges)
        if not roots or not leaves:
            raise ComponentExpansionError(
                f"Component definition must have at least one root and one leaf node: {component_id}@{revision_id}",
                code="COMPONENT_INVALID_SHAPE",
                details={"componentId": component_id, "revisionId": revision_id},
            )

        id_map: Dict[str, str] = {}
        parent_meta = {
            "componentId": component_id,
            "componentRevisionId": revision_id,
            "instanceNodeId": instance_node_id,
            "componentConfig": params.get("config") if isinstance(params.get("config"), dict) else {},
            "componentBindings": params.get("bindings") if isinstance(params.get("bindings"), dict) else {},
        }
        parent_component_meta[instance_node_id] = dict(parent_meta)

        for internal_node in internal_nodes:
            internal_id = str(internal_node.get("id") or "").strip()
            if not internal_id:
                raise ComponentExpansionError(
                    f"Component internal node missing id in {component_id}@{revision_id}",
                    code="COMPONENT_INTERNAL_NODE_ID_MISSING",
                    details={"componentId": component_id, "revisionId": revision_id},
                )
            prefixed = _prefixed_node_id(instance_node_id, internal_id)
            if prefixed in id_map.values():
                raise ComponentExpansionError(
                    f"Component node id collision: {prefixed}",
                    code="COMPONENT_ID_COLLISION",
                    details={"nodeId": prefixed},
                )
            id_map[internal_id] = prefixed
            internal_to_parent[prefixed] = instance_node_id
            parent_to_internal.setdefault(instance_node_id, []).append(prefixed)

            node_clone = copy.deepcopy(internal_node)
            node_clone["id"] = prefixed
            data_clone = node_clone.get("data", {}) if isinstance(node_clone.get("data"), dict) else {}
            meta_clone = data_clone.get("meta", {}) if isinstance(data_clone.get("meta"), dict) else {}
            meta_clone["component"] = {
                "componentId": component_id,
                "componentRevisionId": revision_id,
                "instanceNodeId": instance_node_id,
                "internalNodeId": internal_id,
            }
            data_clone["meta"] = meta_clone
            data_clone["_componentContext"] = {
                **parent_meta,
                "internalNodeId": internal_id,
            }
            node_clone["data"] = data_clone
            out_nodes.append(node_clone)

        for idx, internal_edge in enumerate(internal_edges):
            src_internal = str(internal_edge.get("source") or "")
            tgt_internal = str(internal_edge.get("target") or "")
            if src_internal not in id_map or tgt_internal not in id_map:
                raise ComponentExpansionError(
                    f"Component edge references unknown internal node in {component_id}@{revision_id}",
                    code="COMPONENT_EDGE_NODE_NOT_FOUND",
                    details={
                        "componentId": component_id,
                        "revisionId": revision_id,
                        "source": src_internal,
                        "target": tgt_internal,
                    },
                )
            edge_clone = copy.deepcopy(internal_edge)
            edge_clone["id"] = _prefixed_edge_id(instance_node_id, str(internal_edge.get("id") or ""), idx)
            edge_clone["source"] = id_map[src_internal]
            edge_clone["target"] = id_map[tgt_internal]
            out_edges.append(edge_clone)

        ingress_targets = [id_map[r] for r in roots]
        egress_sources = [id_map[l] for l in leaves]
        if len(ingress_targets) > 1:
            ingress_targets = ingress_targets[:1]
        if len(egress_sources) > 1:
            egress_sources = egress_sources[:1]

        for external_in in incoming_by_target.get(instance_node_id, []):
            for target_internal in ingress_targets:
                edge_clone = copy.deepcopy(external_in)
                edge_clone["id"] = f"ce:{instance_node_id}:ingress:{str(external_in.get('id') or '')}:{target_internal}"
                edge_clone["target"] = target_internal
                out_edges.append(edge_clone)

        for external_out in outgoing_by_source.get(instance_node_id, []):
            for source_internal in egress_sources:
                edge_clone = copy.deepcopy(external_out)
                edge_clone["id"] = f"ce:{instance_node_id}:egress:{str(external_out.get('id') or '')}:{source_internal}"
                edge_clone["source"] = source_internal
                out_edges.append(edge_clone)

    return ExpandedComponentGraph(
        graph={"nodes": out_nodes, "edges": out_edges},
        internal_to_parent=internal_to_parent,
        parent_to_internal={k: sorted(v) for k, v in parent_to_internal.items()},
        parent_component_meta=parent_component_meta,
    )
