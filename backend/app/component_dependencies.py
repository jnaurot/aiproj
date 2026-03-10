from __future__ import annotations

from typing import Any, Dict, List, Tuple


def _extract_component_refs_from_graph(
    graph: Dict[str, Any],
    *,
    path_prefix: str = "graph.nodes",
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    refs: List[Dict[str, Any]] = []
    diagnostics: List[Dict[str, Any]] = []
    nodes = graph.get("nodes") if isinstance(graph.get("nodes"), list) else []
    for idx, node in enumerate(nodes):
        if not isinstance(node, dict):
            continue
        node_id = str(node.get("id") or "").strip()
        data = node.get("data") if isinstance(node.get("data"), dict) else {}
        if str(data.get("kind") or "").strip().lower() != "component":
            continue
        params = data.get("params") if isinstance(data.get("params"), dict) else {}
        component_ref = params.get("componentRef") if isinstance(params.get("componentRef"), dict) else {}
        component_id = str(component_ref.get("componentId") or "").strip()
        revision_id = str(component_ref.get("revisionId") or "").strip()
        if not component_id or not revision_id:
            diagnostics.append(
                {
                    "code": "COMPONENT_DEPENDENCY_REFERENCE_INVALID",
                    "path": f"{path_prefix}[{idx}].data.params.componentRef",
                    "message": "Component dependency references must include componentId and revisionId.",
                    "severity": "error",
                }
            )
            continue
        refs.append(
            {
                "componentId": component_id,
                "revisionId": revision_id,
                "nodeId": node_id,
            }
        )
    return refs, diagnostics


def build_component_dependency_manifest(
    definition: Dict[str, Any],
    *,
    component_store: Any,
    root_component_id: str = "",
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    graph = definition.get("graph") if isinstance(definition.get("graph"), dict) else {}
    direct_refs, diagnostics = _extract_component_refs_from_graph(graph)
    if component_store is None:
        return {"schemaVersion": 1, "dependencies": [], "unresolved": []}, diagnostics

    root_component = str(root_component_id or "").strip()
    queue: List[Dict[str, Any]] = []
    for ref in direct_refs:
        component_id = str(ref.get("componentId") or "").strip()
        revision_id = str(ref.get("revisionId") or "").strip()
        if root_component and component_id == root_component:
            diagnostics.append(
                {
                    "code": "COMPONENT_DEPENDENCY_CYCLE",
                    "path": "graph.nodes",
                    "message": (
                        f"Component dependency cycle detected: {root_component} -> "
                        f"{component_id}@{revision_id}"
                    ),
                    "severity": "error",
                }
            )
        queue.append(
            {
                "componentId": component_id,
                "revisionId": revision_id,
                "depth": 1,
                "ancestry": [root_component] if root_component else [],
            }
        )

    seen: set[tuple[str, str]] = set()
    dependencies: List[Dict[str, Any]] = []
    unresolved: List[Dict[str, Any]] = []

    while queue:
        current = queue.pop(0)
        component_id = str(current.get("componentId") or "").strip()
        revision_id = str(current.get("revisionId") or "").strip()
        depth = int(current.get("depth") or 1)
        ancestry = [
            str(item).strip()
            for item in (current.get("ancestry") if isinstance(current.get("ancestry"), list) else [])
            if str(item).strip()
        ]
        if not component_id or not revision_id:
            continue
        key = (component_id, revision_id)
        if key in seen:
            continue
        seen.add(key)
        dependencies.append(
            {
                "componentId": component_id,
                "revisionId": revision_id,
                "depth": depth,
            }
        )

        revision = component_store.get_revision(component_id, revision_id)
        if revision is None:
            unresolved.append(
                {
                    "componentId": component_id,
                    "revisionId": revision_id,
                }
            )
            continue

        child_definition = revision.definition if isinstance(revision.definition, dict) else {}
        child_graph = child_definition.get("graph") if isinstance(child_definition.get("graph"), dict) else {}
        child_refs, _ = _extract_component_refs_from_graph(
            child_graph,
            path_prefix=f"dependency[{component_id}@{revision_id}].graph.nodes",
        )
        for child_ref in child_refs:
            child_component_id = str(child_ref.get("componentId") or "").strip()
            child_revision_id = str(child_ref.get("revisionId") or "").strip()
            if child_component_id in set(ancestry + [component_id]):
                cycle_path = ancestry + [component_id, f"{child_component_id}@{child_revision_id}"]
                diagnostics.append(
                    {
                        "code": "COMPONENT_DEPENDENCY_CYCLE",
                        "path": f"dependency[{component_id}@{revision_id}]",
                        "message": f"Component dependency cycle detected: {' -> '.join(cycle_path)}",
                        "severity": "error",
                    }
                )
                continue
            queue.append(
                {
                    "componentId": child_component_id,
                    "revisionId": child_revision_id,
                    "depth": depth + 1,
                    "ancestry": ancestry + [component_id],
                }
            )

    dependencies.sort(key=lambda item: (int(item.get("depth") or 0), str(item.get("componentId") or ""), str(item.get("revisionId") or "")))
    unresolved.sort(key=lambda item: (str(item.get("componentId") or ""), str(item.get("revisionId") or "")))
    return {
        "schemaVersion": 1,
        "dependencies": dependencies,
        "unresolved": unresolved,
    }, diagnostics
