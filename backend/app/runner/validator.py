# backend/app/runner/validator.py
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
import json
from .schemas import validate_node_params  # Import schema validation
from .schema_diagnostics import (
    SCHEMA_DIAGNOSTIC_CODES,
    TYPE_MISMATCH,
    PAYLOAD_SCHEMA_MISMATCH,
)

from pprint import pformat
from typing import Set

@dataclass
class ValidationError:
    code: str
    message: str
    node_id: Optional[str] = None
    edge_id: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    suggestions: Optional[List[str]] = None

@dataclass
class ValidationResult:
    valid: bool
    errors: List[ValidationError]
    warnings: List[ValidationError]

class GraphValidator:
    """Pre-execution and runtime validation"""
    def __init__(self) -> None:
        self._schema_diagnostic_codes = set(SCHEMA_DIAGNOSTIC_CODES)

    def validate_pre_execution(self, graph: Dict[str, Any]) -> ValidationResult:
        """Comprehensive validation before execution starts"""
        errors = []
        warnings = []
        
        # 1. Structural validation
        errors.extend(self._check_cycles(graph))
        errors.extend(self._check_orphaned_nodes(graph))
        
        # 2. Type validation
        errors.extend(self._validate_payload_types(graph))
        errors.extend(self._validate_transform_join_arity(graph))
        
        # 3. Schema validation
        errors.extend(self._validate_node_params_schema(graph))
        errors.extend(self._validate_component_nodes(graph))
        
        # 4. Resource validation
        warnings.extend(self._check_resource_availability(graph))
        
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )

    def _schema_code(self, code: str) -> str:
        return code if code in self._schema_diagnostic_codes else code

    @staticmethod
    def _stable_json(value: Any) -> str:
        try:
            return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        except Exception:
            return str(value)

    @staticmethod
    def _normalize_payload_type(raw: Any) -> Optional[str]:
        if raw is None:
            return None
        value = str(raw).strip().lower()
        if value == "string":
            value = "text"
        return value or None

    @staticmethod
    def _schema_columns(
        payload: Any,
        *,
        fields_key: str = "fields",
        columns_key: str = "columns",
    ) -> List[str]:
        if not isinstance(payload, dict):
            return []
        out: List[str] = []
        fields = payload.get(fields_key)
        if isinstance(fields, list):
            for f in fields:
                if not isinstance(f, dict):
                    continue
                name = str(f.get("name") or "").strip()
                if name:
                    out.append(name)
        if out:
            return out
        cols = payload.get(columns_key)
        if isinstance(cols, list):
            return [str(c).strip() for c in cols if str(c).strip()]
        return []

    @staticmethod
    def _payload_declared_type(payload: Any) -> Optional[str]:
        if not isinstance(payload, dict):
            return None
        return GraphValidator._normalize_payload_type(payload.get("type"))

    @staticmethod
    def _node_schema_declared_type(node: Dict[str, Any]) -> Optional[str]:
        data = (node.get("data") or {}) if isinstance(node, dict) else {}
        schema_env = data.get("schema") if isinstance(data.get("schema"), dict) else {}
        if not isinstance(schema_env, dict):
            return None
        # Declared contract is expectedSchema only.
        # inferred/observed are advisory and may be stale across file/sample changes.
        obs = schema_env.get("expectedSchema")
        if isinstance(obs, dict):
            typed = obs.get("typedSchema")
            if isinstance(typed, dict):
                resolved = GraphValidator._normalize_payload_type(typed.get("type"))
                if resolved:
                    return resolved
        return None

    @staticmethod
    def _source_default_type(node: Dict[str, Any]) -> Optional[str]:
        data = (node.get("data") or {})
        kind = str(data.get("kind") or "").strip().lower()
        params = (data.get("params") or {})
        if kind == "source":
            source_kind = str(params.get("sourceKind") or params.get("source_type") or "file").strip().lower()
            if source_kind == "file":
                file_format = str(params.get("file_format") or "").strip().lower()
                if file_format in {"csv", "tsv", "parquet", "arrow", "feather", "xlsx", "xls"}:
                    return "table"
                if file_format in {"json", "jsonl"}:
                    return "json"
                return "text"
            if source_kind == "json":
                return "json"
            if source_kind == "table":
                return "table"
            return "text"
        if kind == "transform":
            return "table"
        if kind == "llm":
            return "text"
        if kind == "tool":
            return "json"
        return None

    @staticmethod
    def _target_default_type(node: Dict[str, Any]) -> Optional[str]:
        data = (node.get("data") or {})
        kind = str(data.get("kind") or "").strip().lower()
        if kind == "source":
            return None
        if kind == "transform":
            return "table"
        if kind == "llm":
            return "text"
        if kind == "tool":
            return "json"
        return None

    def _component_output_type(
        self,
        node: Dict[str, Any],
        source_handle: Any,
        edge_id: str,
        source_id: Any,
    ) -> tuple[Optional[str], Optional[ValidationError]]:
        params = ((node.get("data") or {}).get("params") or {})
        api = params.get("api") if isinstance(params.get("api"), dict) else {}
        outputs = api.get("outputs") if isinstance(api.get("outputs"), list) else []
        sh = str(source_handle or "out")
        if sh == "out":
            if len(outputs) == 1 and isinstance(outputs[0], dict):
                typed = outputs[0].get("typedSchema") if isinstance(outputs[0].get("typedSchema"), dict) else {}
                resolved = self._normalize_payload_type(typed.get("type"))
                return resolved, None
            if len(outputs) > 1:
                return None, ValidationError(
                    code="COMPONENT_OUTPUT_HANDLE_UNRESOLVED",
                    message=(
                        "Component edge sourceHandle must name an output when component has multiple outputs"
                    ),
                    edge_id=edge_id,
                    node_id=source_id,
                )
            return None, None
        decl = next(
            (
                o
                for o in outputs
                if isinstance(o, dict) and str(o.get("name") or "").strip() == sh
            ),
            None,
        )
        if not isinstance(decl, dict):
            return None, ValidationError(
                code="COMPONENT_OUTPUT_HANDLE_UNRESOLVED",
                message=f"Component output handle '{sh}' is not declared in component API outputs",
                edge_id=edge_id,
                node_id=source_id,
            )
        typed = decl.get("typedSchema") if isinstance(decl.get("typedSchema"), dict) else {}
        resolved = self._normalize_payload_type(typed.get("type"))
        return resolved, None

    def _component_input_type(self, node: Dict[str, Any], target_handle: Any) -> Optional[str]:
        params = ((node.get("data") or {}).get("params") or {})
        api = params.get("api") if isinstance(params.get("api"), dict) else {}
        inputs = api.get("inputs") if isinstance(api.get("inputs"), list) else []
        th = str(target_handle or "in")
        if th == "in":
            if len(inputs) == 1 and isinstance(inputs[0], dict):
                typed = inputs[0].get("typedSchema") if isinstance(inputs[0].get("typedSchema"), dict) else {}
                resolved = self._normalize_payload_type(typed.get("type"))
                return resolved
            return None
        decl = next(
            (
                i
                for i in inputs
                if isinstance(i, dict) and str(i.get("name") or "").strip() == th
            ),
            None,
        )
        if not isinstance(decl, dict):
            return None
        typed = decl.get("typedSchema") if isinstance(decl.get("typedSchema"), dict) else {}
        resolved = self._normalize_payload_type(typed.get("type"))
        return resolved

    def _adapter_suggestions(
        self,
        source_type: Optional[str],
        target_type: Optional[str],
        target_node: Dict[str, Any],
    ) -> List[str]:
        src = self._normalize_payload_type(source_type)
        tgt = self._normalize_payload_type(target_type)
        if not src or not tgt or src == tgt:
            return []
        if src == "text" and tgt == "table":
            return [
                "Insert a Transform node with op='text_to_table' between source and target.",
            ]
        if src == "json" and tgt == "table":
            return [
                "Insert a Transform node with op='json_to_table' between source and target.",
            ]
        if src == "table" and tgt == "json":
            return [
                "Insert a Transform node with op='table_to_json' between source and target.",
            ]
        if src == "table" and tgt == "text":
            return [
                "Convert table to json first (op='table_to_json'), then map json to text in downstream node.",
            ]
        target_kind = str((target_node.get("data", {}) or {}).get("kind") or "").strip().lower()
        if target_kind == "transform":
            return [
                "Use an adapter transform (text_to_table/json_to_table/table_to_json) to satisfy this edge contract.",
            ]
        return [
            f"Insert an adapter node to convert '{src}' -> '{tgt}' before this target.",
        ]
    
    def _validate_node_params_schema(self, graph: Dict[str, Any]) -> List[ValidationError]:
        """Validate using Pydantic schemas"""
        errors = []
        nodes = graph.get("nodes", [])
        
        for node in nodes:
            node_id = node["id"]
            node_label = node["data"].get("label", node_id)
            
            
            #DEBUGGING LOGS START
            if node.get("data", {}).get("kind") == "llm":
                print("\n[VALIDATOR] LLM NODE RAW:")
                print(pformat({
                    "id": node_id,
                    "llmKind": node.get("data", {}).get("llmKind"),
                    "params": node.get("data", {}).get("params"),
                })[:8000])
            #DEBUGGING LOGS END
            
            # Use schema validation
            param_errors = validate_node_params(node)
            
            for error_msg in param_errors:
                errors.append(ValidationError(
                    code="INVALID_PARAMS",
                    message=f"Node '{node_label}': {error_msg}",
                    node_id=node_id
                ))
        
        return errors

    def _validate_component_nodes(self, graph: Dict[str, Any]) -> List[ValidationError]:
        errors: List[ValidationError] = []
        nodes = graph.get("nodes", [])
        for node in nodes:
            node_id = str(node.get("id") or "")
            data = node.get("data", {}) or {}
            if data.get("kind") != "component":
                continue
            params = data.get("params", {}) or {}
            component_ref = params.get("componentRef")
            if not isinstance(component_ref, dict):
                errors.append(
                    ValidationError(
                        code="MISSING_COMPONENT_REF",
                        message="Component node requires params.componentRef",
                        node_id=node_id,
                    )
                )
                continue
            revision_id = str(component_ref.get("revisionId") or "").strip()
            if not revision_id:
                errors.append(
                    ValidationError(
                        code="MISSING_REVISION_ID",
                        message="Component node requires params.componentRef.revisionId",
                        node_id=node_id,
                    )
                )
        return errors
    
    def _check_cycles(self, graph: Dict[str, Any]) -> List[ValidationError]:
        """Detect cycles using DFS"""
        errors = []
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])
        
        if not nodes:
            return errors
        
        # Build adjacency list
        adj: Dict[str, List[str]] = {n["id"]: [] for n in nodes}
        for e in edges:
            source = e.get("source")
            target = e.get("target")
            if source in adj and target in adj:
                adj[source].append(target)
        
        # DFS cycle detection
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {n["id"]: WHITE for n in nodes}
        
        def has_cycle(node_id: str, path: List[str]) -> bool:
            if color[node_id] == GRAY:
                # Found a back edge - cycle detected
                cycle_start = path.index(node_id)
                cycle_nodes = " -> ".join(path[cycle_start:] + [node_id])
                errors.append(ValidationError(
                    code="CYCLE_DETECTED",
                    message=f"Cycle detected: {cycle_nodes}"
                ))
                return True
            
            if color[node_id] == BLACK:
                return False
            
            color[node_id] = GRAY
            path.append(node_id)
            
            for neighbor in adj.get(node_id, []):
                if has_cycle(neighbor, path):
                    return True
            
            path.pop()
            color[node_id] = BLACK
            return False
        
        for node in nodes:
            if color[node["id"]] == WHITE:
                has_cycle(node["id"], [])
        
        return errors
    
    def _check_orphaned_nodes(self, graph: Dict[str, Any]) -> List[ValidationError]:
        """Check for nodes with no connections"""
        errors = []
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])
        
        if len(nodes) <= 1:
            return errors  # Single node or empty graph is fine
        
        connected_nodes = set()
        for e in edges:
            connected_nodes.add(e.get("source"))
            connected_nodes.add(e.get("target"))
        
        for node in nodes:
            if node["id"] not in connected_nodes:
                errors.append(ValidationError(
                    code="ORPHANED_NODE",
                    message=f"Node '{node['data'].get('label', node['id'])}' has no connections",
                    node_id=node["id"]
                ))
        
        return errors
    
    def _validate_payload_types(self, graph: Dict[str, Any]) -> List[ValidationError]:
        """Ensure all connections have compatible schemas/types."""
        errors = []
        edges = graph.get("edges", [])
        nodes = {n["id"]: n for n in graph.get("nodes", [])}

        for edge in edges:
            edge_id = edge.get("id", "unknown")
            source_id = edge.get("source")
            target_id = edge.get("target")
            
            source_node = nodes.get(source_id)
            target_node = nodes.get(target_id)
            
            if not source_node:
                errors.append(ValidationError(
                    code="MISSING_SOURCE_NODE",
                    message=f"Edge references non-existent source node: {source_id}",
                    edge_id=edge_id
                ))
                continue
            
            if not target_node:
                errors.append(ValidationError(
                    code="MISSING_TARGET_NODE",
                    message=f"Edge references non-existent target node: {target_id}",
                    edge_id=edge_id
                ))
                continue
            
            # Get payload types
            source_handle = edge.get("sourceHandle", "out")
            target_handle = edge.get("targetHandle", "in")

            # Optional payload schema compatibility (forward path)
            contract = (edge.get("data", {}) or {}).get("contract", {}) or {}
            payload = contract.get("payload", {}) if isinstance(contract, dict) else {}
            src_payload = payload.get("source", {}) if isinstance(payload, dict) else {}
            tgt_payload = payload.get("target", {}) if isinstance(payload, dict) else {}
            source_type = self._payload_declared_type(src_payload)
            target_type = self._payload_declared_type(tgt_payload)
            if (source_node.get("data") or {}).get("kind") == "component":
                component_type, component_error = self._component_output_type(
                    source_node,
                    source_handle,
                    edge_id=edge_id,
                    source_id=source_id,
                )
                if component_error:
                    errors.append(component_error)
                    continue
                source_type = source_type or component_type
            if (target_node.get("data") or {}).get("kind") == "component":
                target_type = target_type or self._component_input_type(target_node, target_handle)
            source_type = (
                self._node_schema_declared_type(source_node)
                or self._source_default_type(source_node)
                or source_type
            )
            target_type = (
                self._node_schema_declared_type(target_node)
                or self._target_default_type(target_node)
                or target_type
            )
            source_type = self._normalize_payload_type(source_type)
            target_type = self._normalize_payload_type(target_type)

            normalized_src_payload = dict(src_payload) if isinstance(src_payload, dict) else {}
            normalized_tgt_payload = dict(tgt_payload) if isinstance(tgt_payload, dict) else {}
            if source_type:
                normalized_src_payload["type"] = source_type
            if target_type:
                normalized_tgt_payload["type"] = target_type

            provided_schema = {
                "type": source_type,
                "payload": normalized_src_payload,
            }
            required_schema = {
                "type": target_type,
                "payload": normalized_tgt_payload,
            }

            src_cols = self._schema_columns(normalized_src_payload, fields_key="fields", columns_key="columns")
            req_cols = self._schema_columns(
                normalized_tgt_payload,
                fields_key="required_fields",
                columns_key="required_columns",
            )
            if req_cols and not src_cols:
                errors.append(
                    ValidationError(
                        code="CONTRACT_EDGE_TYPED_SCHEMA_MISSING",
                        message=(
                            "Required typed schema coverage is missing on edge. "
                            f"provided_schema={self._stable_json(provided_schema)} "
                            f"required_schema={self._stable_json(required_schema)}."
                        ),
                        edge_id=edge_id,
                        details={
                            "expected": {
                                "type": self._normalize_payload_type(target_type),
                                "typedSchema": {"fields": "non-empty"},
                            },
                            "actual": {
                                "type": self._normalize_payload_type(source_type),
                                "typedSchema": {"fields": src_cols},
                            },
                            "provided_schema": provided_schema,
                            "required_schema": required_schema,
                        },
                    )
                )
                continue
            if req_cols:
                missing = [c for c in req_cols if c not in src_cols]
                if missing:
                    suggestions = self._adapter_suggestions(source_type, target_type, target_node)
                    suggestion_suffix = (
                        f" Auto-adapter suggestion: {' | '.join(suggestions)}"
                        if suggestions
                        else ""
                    )
                    errors.append(ValidationError(
                        code=self._schema_code(PAYLOAD_SCHEMA_MISMATCH),
                        message=(
                            f"Missing required columns on edge: {missing}. "
                            f"provided_schema={self._stable_json(provided_schema)} "
                            f"required_schema={self._stable_json(required_schema)}."
                            f"{suggestion_suffix}"
                        ),
                        edge_id=edge_id,
                        details={
                            "provided_schema": provided_schema,
                            "required_schema": required_schema,
                            "missing_columns": missing,
                        },
                        suggestions=suggestions or None,
                    ))
                    continue

            # Schema constraint solver (compile-time): schema compatibility + actionable adapter hints.
            if source_type and target_type and source_type != target_type:
                suggestions = self._adapter_suggestions(source_type, target_type, target_node)
                suggestion_suffix = (
                    f" Auto-adapter suggestion: {' | '.join(suggestions)}"
                    if suggestions
                    else ""
                )
                errors.append(
                    ValidationError(
                        code=self._schema_code(TYPE_MISMATCH),
                        message=(
                            f"Incompatible schemas on edge '{edge_id}': "
                            f"provided_schema={self._stable_json(provided_schema)} "
                            f"required_schema={self._stable_json(required_schema)}."
                            f"{suggestion_suffix}"
                        ),
                        edge_id=edge_id,
                        details={
                            "provided_schema": provided_schema,
                            "required_schema": required_schema,
                        },
                        suggestions=suggestions or None,
                    )
                )
                continue
        
        return errors

    def _validate_llm_input_arity(self, graph: Dict[str, Any]) -> List[ValidationError]:
        """Current runtime supports exactly one upstream artifact for each LLM node."""
        errors: List[ValidationError] = []
        nodes = {n["id"]: n for n in graph.get("nodes", [])}
        edges = graph.get("edges", [])

        incoming_counts: Dict[str, int] = {}
        for e in edges:
            tgt = e.get("target")
            if not tgt:
                continue
            incoming_counts[tgt] = incoming_counts.get(tgt, 0) + 1

        for node_id, node in nodes.items():
            if node.get("data", {}).get("kind") != "llm":
                continue
            count = incoming_counts.get(node_id, 0)
            if count > 1:
                errors.append(
                    ValidationError(
                        code="LLM_MULTI_INPUT_UNSUPPORTED",
                        message=f"LLM node '{node['data'].get('label', node_id)}' has {count} inputs; only one is supported",
                        node_id=node_id,
                    )
                )

        return errors

    def _validate_transform_join_arity(self, graph: Dict[str, Any]) -> List[ValidationError]:
        errors: List[ValidationError] = []
        nodes = {n["id"]: n for n in graph.get("nodes", [])}
        edges = graph.get("edges", [])

        incoming_counts: Dict[str, int] = {}
        for e in edges:
            tgt = e.get("target")
            if not tgt:
                continue
            incoming_counts[tgt] = incoming_counts.get(tgt, 0) + 1

        for node_id, node in nodes.items():
            data = node.get("data", {}) or {}
            if data.get("kind") != "transform":
                continue
            params = data.get("params", {}) or {}
            op = str(params.get("op") or data.get("transformKind") or "")
            if op != "join":
                continue
            count = incoming_counts.get(node_id, 0)
            if count < 2:
                errors.append(
                    ValidationError(
                        code="TRANSFORM_JOIN_INPUT_ARITY",
                        message=f"Transform join node '{data.get('label', node_id)}' requires 2 inputs, got {count}",
                        node_id=node_id,
                    )
                )
        return errors
    
    def _validate_node_params(self, graph: Dict[str, Any]) -> List[ValidationError]:
        """Validate each node's parameters against expected schema"""
        errors = []
        nodes = graph.get("nodes", [])
        
        for node in nodes:
            node_id = node["id"]
            node_kind = node["data"].get("kind")
            params = node["data"].get("params", {})
            
            # Basic validation - you can expand this based on your schemas
            if node_kind == "source":
                # Source nodes might need file paths, URLs, etc.
                if not params:
                    errors.append(ValidationError(
                        code="MISSING_PARAMS",
                        message=f"Source node '{node['data'].get('label')}' requires parameters",
                        node_id=node_id
                    ))
            
            elif node_kind == "llm":
                # LLM nodes might need prompts, model names, etc.
                if not params.get("prompt") and not params.get("system"):
                    errors.append(ValidationError(
                        code="MISSING_PROMPT",
                        message=f"LLM node '{node['data'].get('label')}' requires a prompt or system message",
                        node_id=node_id
                    ))
        
        return errors
    
    def _check_resource_availability(self, graph: Dict[str, Any]) -> List[ValidationError]:
        """Check if required resources are available (warnings only)"""
        warnings = []
        nodes = graph.get("nodes", [])
        
        for node in nodes:
            node_id = node["id"]
            params = node["data"].get("params", {})
            
            # Check for file paths
            file_path = params.get("file_path") or params.get("path")
            if file_path:
                # In production, you'd actually check if file exists
                # For now, just warn if it looks suspicious
                if not isinstance(file_path, str) or len(file_path) == 0:
                    warnings.append(ValidationError(
                        code="INVALID_FILE_PATH",
                        message=f"Node '{node['data'].get('label')}' has invalid file path",
                        node_id=node_id
                    ))
        
        return warnings


# Legacy compatibility helpers used by older unit tests
_LEGACY_NODE_KINDS: Set[str] = {"source", "transform", "llm", "tool", "component"}


def validate_node_connections(edge: Dict[str, Any]) -> Dict[str, Any]:
    source = edge.get("from") or edge.get("source")
    target = edge.get("to") or edge.get("target")
    if not source or not target:
        return {"valid": False, "error": "edge must include both source and target"}
    if source == target:
        return {"valid": False, "error": "self connection is not allowed"}
    return {"valid": True}


def validate_parameters(node: Dict[str, Any], _nodes_map: Dict[str, Any]) -> Dict[str, Any]:
    data = node.get("data")
    if not isinstance(data, dict):
        return {"valid": False, "error": "node.data is required"}
    kind = data.get("kind")
    if not isinstance(kind, str) or not kind.strip():
        return {"valid": False, "error": "node.data.kind is required"}
    if kind not in _LEGACY_NODE_KINDS:
        return {"valid": False, "error": f"invalid node kind: {kind}"}
    return {"valid": True}


def validate_pipeline(nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]) -> Dict[str, Any]:
    errors: List[str] = []
    node_ids: Set[str] = set()

    for node in nodes:
        node_id = node.get("id")
        if not isinstance(node_id, str) or not node_id:
            errors.append("node.id is required")
            continue
        if node_id in node_ids:
            errors.append(f"duplicate node id: {node_id}")
            continue
        node_ids.add(node_id)

        result = validate_parameters(node, {})
        if not result.get("valid"):
            errors.append(str(result.get("error") or "invalid node parameters"))

    for edge in edges:
        edge_result = validate_node_connections(edge)
        if not edge_result.get("valid"):
            errors.append(str(edge_result.get("error") or "invalid edge"))
            continue

        source = edge.get("from") or edge.get("source")
        target = edge.get("to") or edge.get("target")
        if source not in node_ids:
            errors.append(f"edge source does not exist: {source}")
        if target not in node_ids:
            errors.append(f"edge target does not exist: {target}")

    return {"valid": len(errors) == 0, "errors": errors}

