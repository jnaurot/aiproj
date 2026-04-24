# backend/app/runner/validator.py
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
import json
from .schemas import validate_node_params  # Import schema validation
from .capabilities import (
    allowed_edge_modes,
    port_contract,
    resolve_node_port_declaration,
    resolve_node_port_declarations,
)
from app.component_contracts import edge_mode_requires_payload_compatibility, normalize_edge_mode
from .contracts import evaluate_schema_coercion, normalize_coercion_policy
from .schema_diagnostics import (
    SCHEMA_DIAGNOSTIC_CODES,
    TYPE_MISMATCH,
    PAYLOAD_SCHEMA_MISMATCH,
)

from typing import Set


def collect_transitive_descendants(
    adjacency: Dict[str, List[str]],
    origin_node_id: str,
    *,
    allowed_nodes: Optional[Set[str]] = None,
) -> List[str]:
    """Return deterministic transitive descendants for localized runtime cascade."""
    origin = str(origin_node_id or "").strip()
    if not origin:
        return []
    if allowed_nodes is not None and origin not in allowed_nodes:
        return []
    visited: Set[str] = set()
    ordered: List[str] = []
    stack: List[str] = [origin]
    while stack:
        node_id = stack.pop()
        downstream = sorted(
            [str(item or "").strip() for item in (adjacency.get(node_id) or []) if str(item or "").strip()]
        )
        for child in downstream:
            if allowed_nodes is not None and child not in allowed_nodes:
                continue
            if child in visited or child == origin:
                continue
            visited.add(child)
            ordered.append(child)
            stack.append(child)
    return ordered

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
        type_errors, type_warnings = self._validate_payload_types(graph)
        errors.extend(type_errors)
        warnings.extend(type_warnings)
        errors.extend(self._validate_transform_join_arity(graph))
        errors.extend(self._validate_port_declaration_constraints(graph))
        errors.extend(self._validate_control_plane_safety(graph))
        
        # 3. Schema validation
        errors.extend(self._validate_node_params_schema(graph))
        errors.extend(self._validate_component_nodes(graph))
        
        # 4. Resource validation
        warnings.extend(self._check_resource_availability(graph))
        warnings.extend(self._validate_edge_contract_snapshots(graph))
        warnings.extend(self._validate_port_runtime_deprecations(graph))
        
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )

    @staticmethod
    def _edge_link_kind(edge: Dict[str, Any]) -> str:
        data = edge.get("data") if isinstance(edge.get("data"), dict) else {}
        raw = str(data.get("linkKind") or data.get("link_kind") or "data_link").strip().lower() or "data_link"
        if raw not in {"data_link", "control_link"}:
            return "data_link"
        return raw

    def _validate_control_plane_safety(self, graph: Dict[str, Any]) -> List[ValidationError]:
        errors: List[ValidationError] = []
        edges = graph.get("edges", []) if isinstance(graph.get("edges"), list) else []
        nodes = {
            str(node.get("id") or "").strip(): node
            for node in (graph.get("nodes", []) if isinstance(graph.get("nodes"), list) else [])
            if isinstance(node, dict) and str(node.get("id") or "").strip()
        }
        control_adj: Dict[str, List[str]] = {}
        control_incoming_ids_by_target_handle: Dict[tuple[str, str], Dict[str, List[str]]] = {}
        control_incoming_count: Dict[str, int] = {}
        work_incoming_count: Dict[str, int] = {}

        for edge in edges:
            if not isinstance(edge, dict):
                continue
            edge_id = str(edge.get("id") or "").strip()
            source_id = str(edge.get("source") or "").strip()
            target_id = str(edge.get("target") or "").strip()
            if not source_id or not target_id:
                continue
            edge_data = edge.get("data") if isinstance(edge.get("data"), dict) else {}
            mode_raw = str(edge_data.get("mode") or "").strip().lower()
            mode = mode_raw if mode_raw in {"work", "param", "control"} else normalize_edge_mode(edge)
            link_kind = self._edge_link_kind(edge)
            if mode == "work":
                work_incoming_count[target_id] = int(work_incoming_count.get(target_id, 0)) + 1
            if mode != "control":
                continue
            target_handle = str(edge.get("targetHandle") or "control_in").strip() or "control_in"
            bucket = control_incoming_ids_by_target_handle.setdefault(
                (target_id, target_handle),
                {"control_link": [], "legacy_control": []},
            )
            if link_kind == "control_link":
                bucket["control_link"].append(edge_id or f"{source_id}->{target_id}")
                control_adj.setdefault(source_id, []).append(target_id)
                control_incoming_count[target_id] = int(control_incoming_count.get(target_id, 0)) + 1
            else:
                bucket["legacy_control"].append(edge_id or f"{source_id}->{target_id}")

        for node_id, outs in control_adj.items():
            control_adj[node_id] = sorted({str(item) for item in outs if str(item).strip()})

        # Conflict rule: do not mix explicit control_link and legacy control-mode links on the same target handle.
        for (target_id, target_handle), bucket in sorted(control_incoming_ids_by_target_handle.items()):
            control_link_ids = [str(item) for item in (bucket.get("control_link") or []) if str(item).strip()]
            legacy_control_ids = [str(item) for item in (bucket.get("legacy_control") or []) if str(item).strip()]
            if control_link_ids and legacy_control_ids:
                errors.append(
                    ValidationError(
                        code="CONTROL_LINK_CONFLICT",
                        message=(
                            "Conflicting control configuration: target handle mixes control_link and legacy control edges."
                        ),
                        node_id=target_id,
                        edge_id=control_link_ids[0],
                        details={
                            "targetNodeId": target_id,
                            "targetHandle": target_handle,
                            "controlLinkEdgeIds": sorted(control_link_ids),
                            "legacyControlEdgeIds": sorted(legacy_control_ids),
                        },
                    )
                )

        # Cycle rule on explicit control_link graph.
        visiting: Set[str] = set()
        visited: Set[str] = set()
        stack: List[str] = []

        def _dfs(node_id: str) -> Optional[List[str]]:
            if node_id in visiting:
                if node_id in stack:
                    cycle_start = stack.index(node_id)
                    return stack[cycle_start:] + [node_id]
                return [node_id, node_id]
            if node_id in visited:
                return None
            visiting.add(node_id)
            stack.append(node_id)
            for child in control_adj.get(node_id, []):
                cycle = _dfs(child)
                if cycle:
                    return cycle
            stack.pop()
            visiting.remove(node_id)
            visited.add(node_id)
            return None

        for candidate in sorted(control_adj.keys()):
            cycle = _dfs(candidate)
            if not cycle:
                continue
            errors.append(
                ValidationError(
                    code="CONTROL_LINK_CYCLE",
                    message="Control-link cycle detected; control plane must be acyclic.",
                    node_id=str(cycle[0] if cycle else candidate),
                    details={"cyclePath": cycle},
                )
            )
            break

        # Deadlock safety: nodes gated by control_link must still have a work-plane path.
        for node_id, control_count in sorted(control_incoming_count.items()):
            if int(control_count) <= 0:
                continue
            work_count = int(work_incoming_count.get(node_id, 0))
            if work_count > 0:
                continue
            node_obj = nodes.get(node_id) or {}
            data = node_obj.get("data") if isinstance(node_obj.get("data"), dict) else {}
            kind = str(data.get("kind") or "").strip().lower()
            if kind == "source":
                continue
            errors.append(
                ValidationError(
                    code="CONTROL_LINK_DEADLOCK_RISK",
                    message=(
                        "Node has control_link gating but no work-plane inbound edge; this can deadlock execution."
                    ),
                    node_id=node_id,
                    details={
                        "nodeId": node_id,
                        "controlIncomingCount": int(control_count),
                        "workIncomingCount": int(work_count),
                    },
                )
            )
        return errors

    def _validate_port_runtime_deprecations(self, graph: Dict[str, Any]) -> List[ValidationError]:
        warnings: List[ValidationError] = []
        nodes = graph.get("nodes", []) if isinstance(graph.get("nodes"), list) else []
        edges = graph.get("edges", []) if isinstance(graph.get("edges"), list) else []
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_id = str(node.get("id") or "").strip() or None
            data = node.get("data") if isinstance(node.get("data"), dict) else {}
            schema = data.get("schema") if isinstance(data.get("schema"), dict) else {}
            if isinstance(schema.get("expectedInputSchema"), dict):
                warnings.append(
                    ValidationError(
                        code="LEGACY_EXPECTED_INPUT_SCHEMA_DEPRECATED",
                        message=(
                            "Legacy data.schema.expectedInputSchema is deprecated; use "
                            "data.schema.expectedInputSchemas.<handle>."
                        ),
                        node_id=node_id,
                        details={
                            "field": "data.schema.expectedInputSchema",
                            "replacement": "data.schema.expectedInputSchemas.<handle>",
                            "removeAfter": "2026-06-30",
                        },
                    )
                )
            port_contracts = data.get("portContracts")
            port_decls = data.get("portDeclarations")
            if isinstance(port_contracts, dict) and port_contracts and not isinstance(port_decls, dict):
                warnings.append(
                    ValidationError(
                        code="LEGACY_PORT_CONTRACTS_DEPRECATED",
                        message=(
                            "Legacy data.portContracts is deprecated as the primary port model; "
                            "declare data.portDeclarations instead."
                        ),
                        node_id=node_id,
                        details={
                            "field": "data.portContracts",
                            "replacement": "data.portDeclarations",
                            "removeAfter": "2026-06-30",
                        },
                    )
                )
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            edge_id = str(edge.get("id") or "").strip() or None
            edge_data = edge.get("data") if isinstance(edge.get("data"), dict) else {}
            queue_cfg = edge_data.get("queue") if isinstance(edge_data.get("queue"), dict) else {}
            queue_policy = str(queue_cfg.get("policy") or "fifo").strip().lower() or "fifo"
            if queue_policy == "round_robin":
                warnings.append(
                    ValidationError(
                        code="EDGE_QUEUE_POLICY_PREVIEW",
                        message=(
                            "queue.policy=round_robin is preview-only; default fifo remains the stable policy."
                        ),
                        edge_id=edge_id,
                        details={
                            "field": "data.queue.policy",
                            "value": "round_robin",
                            "default": "fifo",
                        },
                    )
                )
        return warnings

    def _validate_edge_contract_snapshots(self, graph: Dict[str, Any]) -> List[ValidationError]:
        warnings: List[ValidationError] = []
        nodes = {
            str(n.get("id") or "").strip(): n
            for n in (graph.get("nodes", []) if isinstance(graph.get("nodes"), list) else [])
            if isinstance(n, dict)
        }
        edges = graph.get("edges", []) if isinstance(graph.get("edges"), list) else []
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            edge_id = str(edge.get("id") or "").strip()
            data = edge.get("data") if isinstance(edge.get("data"), dict) else {}
            contract = data.get("contract") if isinstance(data.get("contract"), dict) else {}
            if not contract:
                continue
            snapshot = contract.get("snapshot") if isinstance(contract.get("snapshot"), dict) else {}
            source_fp = str(snapshot.get("sourceSchemaFingerprint") or "").strip()
            target_fp = str(snapshot.get("targetSchemaFingerprint") or "").strip()
            if not (source_fp and target_fp):
                warnings.append(
                    ValidationError(
                        code="EDGE_CONTRACT_SNAPSHOT_MISSING",
                        message="Edge contract snapshot is missing schema fingerprints; graph import should recanonicalize edge contracts.",
                        edge_id=edge_id,
                        details={"edgeId": edge_id},
                    )
                )
                continue

            payload = contract.get("payload") if isinstance(contract.get("payload"), dict) else {}
            source_payload = payload.get("source") if isinstance(payload, dict) and isinstance(payload.get("source"), dict) else {}
            target_payload = payload.get("target") if isinstance(payload, dict) and isinstance(payload.get("target"), dict) else {}

            source_id = str(edge.get("source") or "").strip()
            target_id = str(edge.get("target") or "").strip()
            source_handle = str(edge.get("sourceHandle") or "out").strip() or "out"
            target_handle = str(edge.get("targetHandle") or "in").strip() or "in"
            source_node = nodes.get(source_id)
            target_node = nodes.get(target_id)

            source_type = None
            target_type = None
            if source_node:
                source_type = self._normalize_payload_type(
                    self._node_schema_declared_type(source_node)
                ) or self._normalize_payload_type(self._source_default_type(source_node))
            if target_node:
                target_type = self._normalize_payload_type(
                    self._node_schema_declared_input_type(target_node, target_handle)
                ) or self._normalize_payload_type(self._target_default_type(target_node))

            current_source_payload = dict(source_payload) if isinstance(source_payload, dict) else {}
            current_target_payload = dict(target_payload) if isinstance(target_payload, dict) else {}
            if source_type and not current_source_payload.get("type"):
                current_source_payload["type"] = source_type
            if target_type and not current_target_payload.get("type"):
                current_target_payload["type"] = target_type
            current_source_fp = self._stable_json(current_source_payload)
            current_target_fp = self._stable_json(current_target_payload)

            if current_source_fp == source_fp and current_target_fp == target_fp:
                continue

            warnings.append(
                ValidationError(
                    code="EDGE_CONTRACT_DRIFT",
                    message=(
                        "Edge contract drift detected: current source/target schemas differ from the persisted edge snapshot. "
                        "Refresh edge contract snapshot or rebind the edge."
                    ),
                    edge_id=edge_id,
                    details={
                        "edgeId": edge_id,
                        "sourceNodeId": source_id,
                        "targetNodeId": target_id,
                        "sourceHandle": source_handle,
                        "targetHandle": target_handle,
                        "snapshotSourceSchemaFingerprint": source_fp,
                        "snapshotTargetSchemaFingerprint": target_fp,
                        "currentSourceSchemaFingerprint": current_source_fp,
                        "currentTargetSchemaFingerprint": current_target_fp,
                    },
                    suggestions=[
                        "Open Schema Contract for this edge and refresh the contract snapshot.",
                        "If drift is intentional, accept coercion or insert an adapter transform.",
                        "If drift is accidental, rebind handles or restore expected schemas.",
                    ],
                )
            )
        return warnings

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
    def _contract_param_keys(payload: Any) -> List[str]:
        if not isinstance(payload, dict):
            return []
        required = payload.get("requiredKeys")
        if isinstance(required, list):
            out = [str(item).strip() for item in required if str(item).strip()]
            if out:
                return out
        shape = payload.get("shape")
        if isinstance(shape, dict):
            out = [str(key).strip() for key in shape.keys() if str(key).strip()]
            if out:
                return out
        props = payload.get("properties")
        if isinstance(props, dict):
            out = [str(key).strip() for key in props.keys() if str(key).strip()]
            if out:
                return out
        return []

    @staticmethod
    def _contract_source_param_keys(payload: Any) -> List[str]:
        if not isinstance(payload, dict):
            return []
        keys = payload.get("keys")
        if isinstance(keys, list):
            out = [str(item).strip() for item in keys if str(item).strip()]
            if out:
                return out
        return GraphValidator._contract_param_keys(payload)

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
    def _node_schema_declared_input_type(node: Dict[str, Any], target_handle: Any = None) -> Optional[str]:
        data = (node.get("data") or {}) if isinstance(node, dict) else {}
        schema_env = data.get("schema") if isinstance(data.get("schema"), dict) else {}
        if not isinstance(schema_env, dict):
            return None
        # Input contract is modeled by expectedInputSchemas(handle).
        # expectedSchema is output-side and must not be reused for incoming edge checks.
        target_key = str(target_handle or "in").strip() or "in"
        obs = None
        expected_by_handle = schema_env.get("expectedInputSchemas")
        if isinstance(expected_by_handle, dict):
            if isinstance(expected_by_handle.get(target_key), dict):
                obs = expected_by_handle.get(target_key)
            elif isinstance(expected_by_handle.get("in"), dict):
                obs = expected_by_handle.get("in")
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
            if source_kind in {"file", "object_store"}:
                file_format = str(params.get("file_format") or "").strip().lower()
                if file_format in {"csv", "tsv", "parquet", "arrow", "feather", "xlsx", "xls"}:
                    return "table"
                if file_format in {"json", "jsonl"}:
                    return "json"
                if file_format in {"jpg", "jpeg", "png", "webp", "gif", "svg", "tif", "tiff"}:
                    return "image"
                if file_format in {"mp3", "wav", "flac", "ogg", "m4a", "aac"}:
                    return "audio"
                if file_format in {"mp4", "mov", "webm"}:
                    return "video"
                return "text"
            if source_kind == "api":
                return "json"
            if source_kind in {"database", "warehouse"}:
                return "table"
            if source_kind == "json":
                return "json"
            if source_kind == "table":
                return "table"
            return "text"
        if kind == "transform":
            op = GraphValidator._transform_op(data, params)
            if op in {"table_to_json", "json_filter"}:
                return "json"
            return "table"
        if kind == "model":
            model_kind = str(data.get("modelKind") or "").strip().lower()
            if model_kind == "vision":
                return "image"
            if model_kind == "audio":
                return "audio"
            return "text"
        if kind == "llm":
            return "text"
        if kind == "tool":
            return "json"
        return None

    @staticmethod
    def _target_default_type(node: Dict[str, Any]) -> Optional[str]:
        data = (node.get("data") or {})
        kind = str(data.get("kind") or "").strip().lower()
        params = (data.get("params") or {})
        if kind == "source":
            return None
        if kind == "transform":
            op = GraphValidator._transform_op(data, params)
            if op == "json_to_table":
                return "json"
            if op == "json_filter":
                return "json"
            if op == "text_to_table":
                return "text"
            if op == "table_to_json":
                return "table"
            return "table"
        if kind == "llm":
            return "text"
        if kind == "model":
            model_kind = str(data.get("modelKind") or "").strip().lower()
            if model_kind == "vision":
                return "image"
            if model_kind == "audio":
                return "audio"
            return "text"
        if kind == "tool":
            return "json"
        return None

    @staticmethod
    def _transform_op(data: Dict[str, Any], params: Dict[str, Any]) -> str:
        op = str((params or {}).get("op") or "").strip().lower()
        if op:
            return op
        return str((data or {}).get("transformKind") or "").strip().lower()

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

    @staticmethod
    def _infer_affinity_from_handle(handle: Any, *, direction: str) -> str:
        raw = str(handle or ("in" if direction == "in" else "out")).strip().lower()
        if raw.startswith("param"):
            return "param"
        if raw.startswith("ctl") or raw.startswith("control"):
            return "control"
        return "work"

    def _port_affinity(
        self,
        node: Dict[str, Any],
        *,
        direction: str,
        handle: Any,
    ) -> str:
        inferred = self._infer_affinity_from_handle(handle, direction=direction)
        if inferred != "work":
            return inferred
        data = (node.get("data") or {}) if isinstance(node, dict) else {}
        kind = str(data.get("kind") or "").strip().lower()
        cfg = resolve_node_port_declaration(node, direction, str(handle or "").strip() or "default")
        if not cfg:
            cfg = port_contract(kind, direction, str(handle or "").strip() or "default")
        affinity = str((cfg or {}).get("affinity") or "").strip().lower()
        if affinity in {"work", "param", "control", "any"}:
            return affinity
        return inferred

    def _validate_port_declaration_constraints(self, graph: Dict[str, Any]) -> List[ValidationError]:
        errors: List[ValidationError] = []
        nodes = graph.get("nodes", []) if isinstance(graph.get("nodes"), list) else []
        edges = graph.get("edges", []) if isinstance(graph.get("edges"), list) else []
        incoming_counts: Dict[str, Dict[str, int]] = {}
        outgoing_counts: Dict[str, Dict[str, int]] = {}
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            source = str(edge.get("source") or "").strip()
            target = str(edge.get("target") or "").strip()
            source_handle = str(edge.get("sourceHandle") or "out").strip() or "out"
            target_handle = str(edge.get("targetHandle") or "in").strip() or "in"
            if source:
                outgoing_counts.setdefault(source, {})
                outgoing_counts[source][source_handle] = int(outgoing_counts[source].get(source_handle) or 0) + 1
            if target:
                incoming_counts.setdefault(target, {})
                incoming_counts[target][target_handle] = int(incoming_counts[target].get(target_handle) or 0) + 1

        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_id = str(node.get("id") or "").strip()
            in_decls = resolve_node_port_declarations(node, "in")
            out_decls = resolve_node_port_declarations(node, "out")
            for handle, decl in in_decls.items():
                if not isinstance(decl, dict):
                    continue
                required = bool(decl.get("required", False))
                cardinality = str(decl.get("cardinality") or "many").strip().lower() or "many"
                count = int((incoming_counts.get(node_id, {}) or {}).get(handle) or 0)
                if required and count == 0:
                    errors.append(
                        ValidationError(
                            code="MISSING_REQUIRED_PORT_EDGE",
                            message=f"Required input port '{handle}' has no inbound edge",
                            node_id=node_id,
                            details={"direction": "in", "handle": handle, "required": True},
                        )
                    )
                if cardinality == "one" and count > 1:
                    errors.append(
                        ValidationError(
                            code="PORT_CARDINALITY_EXCEEDED",
                            message=f"Input port '{handle}' allows only one edge (found {count})",
                            node_id=node_id,
                            details={"direction": "in", "handle": handle, "cardinality": "one", "edgeCount": count},
                        )
                    )
            for handle, decl in out_decls.items():
                if not isinstance(decl, dict):
                    continue
                cardinality = str(decl.get("cardinality") or "many").strip().lower() or "many"
                count = int((outgoing_counts.get(node_id, {}) or {}).get(handle) or 0)
                if cardinality == "one" and count > 1:
                    errors.append(
                        ValidationError(
                            code="PORT_CARDINALITY_EXCEEDED",
                            message=f"Output port '{handle}' allows only one edge (found {count})",
                            node_id=node_id,
                            details={"direction": "out", "handle": handle, "cardinality": "one", "edgeCount": count},
                        )
                    )
        return errors

    @staticmethod
    def _edge_mode(edge: Dict[str, Any]) -> str:
        data = edge.get("data", {}) if isinstance(edge.get("data"), dict) else {}
        return normalize_edge_mode(data.get("mode"))

    @staticmethod
    def _mode_affinity_compatible(mode: str, src_affinity: str, dst_affinity: str) -> bool:
        m = str(mode or "work").strip().lower()
        src = str(src_affinity or "work").strip().lower()
        dst = str(dst_affinity or "work").strip().lower()
        if m == "work":
            return src in {"work", "any"} and dst in {"work", "any"}
        if m == "param":
            return src in {"work", "param", "any"} and dst in {"param", "any"}
        if m == "control":
            return src in {"control", "any"} and dst in {"control", "any"}
        return False
    
    def _validate_node_params_schema(self, graph: Dict[str, Any]) -> List[ValidationError]:
        """Validate using Pydantic schemas"""
        errors = []
        nodes = graph.get("nodes", [])
        
        for node in nodes:
            node_id = node["id"]
            node_label = node["data"].get("label", node_id)
            
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
    
    def _target_coercion_policy(self, node: Dict[str, Any]) -> str:
        params = ((node.get("data") or {}).get("params") or {}) if isinstance(node, dict) else {}
        raw = (
            params.get("coercion_policy")
            or params.get("coercionPolicy")
            or ((params.get("coercion") or {}).get("policy") if isinstance(params.get("coercion"), dict) else None)
            or "safe_widening"
        )
        return normalize_coercion_policy(raw)

    def _validate_payload_types(self, graph: Dict[str, Any]) -> tuple[List[ValidationError], List[ValidationError]]:
        """Ensure all connections have compatible schemas/types."""
        errors = []
        warnings = []
        edges = graph.get("edges", [])
        nodes = {n["id"]: n for n in graph.get("nodes", [])}
        work_handle_signatures: Dict[tuple[str, str], Dict[str, Any]] = {}

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
            source_label = str(((source_node.get("data") or {}).get("label") or source_id or "")).strip()
            target_label = str(((target_node.get("data") or {}).get("label") or target_id or "")).strip()
            edge_mode = self._edge_mode(edge)
            valid_modes = allowed_edge_modes()
            if edge_mode not in valid_modes:
                errors.append(
                    ValidationError(
                        code="EDGE_MODE_INVALID",
                        message=f"Edge mode '{edge_mode}' is invalid. Allowed: {sorted(valid_modes)}",
                        edge_id=edge_id,
                    )
                )
                continue
            src_affinity = self._port_affinity(source_node, direction="out", handle=source_handle)
            dst_affinity = self._port_affinity(target_node, direction="in", handle=target_handle)
            if not self._mode_affinity_compatible(edge_mode, src_affinity, dst_affinity):
                if edge_mode == "control":
                    mode_message = (
                        "Control contract mismatch: control edges require control-affinity handles "
                        f"(source='{src_affinity}', target='{dst_affinity}')"
                    )
                elif edge_mode == "param":
                    mode_message = (
                        "Param contract mismatch: param edges require param-affinity target handles "
                        f"(source='{src_affinity}', target='{dst_affinity}')"
                    )
                else:
                    mode_message = (
                        "Work contract mismatch: work edges require work-affinity source/target handles "
                        f"(source='{src_affinity}', target='{dst_affinity}')"
                    )
                errors.append(
                    ValidationError(
                        code="EDGE_MODE_INCOMPATIBLE",
                        message=mode_message,
                        edge_id=edge_id,
                        details={
                            "edgeId": edge_id,
                            "sourceHandle": source_handle,
                            "targetHandle": target_handle,
                            "edgeMode": edge_mode,
                            "sourceAffinity": src_affinity,
                            "targetAffinity": dst_affinity,
                            "sourceNodeId": source_id,
                            "targetNodeId": target_id,
                            "sourceLabel": source_label,
                            "targetLabel": target_label,
                        },
                    )
                )
                continue
            if edge_mode == "param":
                contract = (edge.get("data", {}) or {}).get("contract", {}) or {}
                payload = contract.get("payload", {}) if isinstance(contract, dict) else {}
                src_payload = payload.get("source", {}) if isinstance(payload, dict) else {}
                tgt_payload = payload.get("target", {}) if isinstance(payload, dict) else {}
                required_keys = self._contract_param_keys(tgt_payload)
                if required_keys:
                    available_keys = self._contract_source_param_keys(src_payload)
                    missing = [key for key in required_keys if key not in available_keys]
                    if missing:
                        errors.append(
                            ValidationError(
                                code="PARAM_CONTRACT_MISMATCH",
                                message=(
                                    f"Param shape mismatch on edge '{edge_id}': missing required keys {missing}. "
                                    f"available={available_keys}"
                                ),
                                edge_id=edge_id,
                                details={
                                    "edgeId": edge_id,
                                    "sourceHandle": source_handle,
                                    "targetHandle": target_handle,
                                    "mode": edge_mode,
                                    "requiredKeys": required_keys,
                                    "availableKeys": available_keys,
                                    "missingKeys": missing,
                                    "sourceNodeId": source_id,
                                    "targetNodeId": target_id,
                                    "sourceLabel": source_label,
                                    "targetLabel": target_label,
                                },
                                suggestions=[
                                    "Ensure source payload exposes all requiredKeys for this param edge.",
                                    "Or relax target requiredKeys/shape to match the provided param payload.",
                                ],
                            )
                        )
                continue
            if not edge_mode_requires_payload_compatibility(edge_mode):
                # Param/control edges use affinity and mode-specific semantics instead of payload typing.
                continue

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
                self._node_schema_declared_input_type(target_node, target_handle)
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
            if edge_mode == "work":
                handle_key = (str(target_id), str(target_handle or "in"))
                signature = self._stable_json(provided_schema)
                bucket = work_handle_signatures.get(handle_key)
                if bucket is None:
                    work_handle_signatures[handle_key] = {
                        "targetNodeId": str(target_id),
                        "targetHandle": str(target_handle or "in"),
                        "signatures": {signature: [str(edge_id)]},
                        "schemas": {signature: provided_schema},
                    }
                else:
                    signatures = bucket.get("signatures", {})
                    edge_ids = signatures.get(signature)
                    if isinstance(edge_ids, list):
                        edge_ids.append(str(edge_id))
                    else:
                        signatures[signature] = [str(edge_id)]

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
                            "edgeId": edge_id,
                            "sourceHandle": source_handle,
                            "targetHandle": target_handle,
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
                            "sourceNodeId": source_id,
                            "targetNodeId": target_id,
                            "sourceLabel": source_label,
                            "targetLabel": target_label,
                            "mode": edge_mode,
                            "sourceAffinity": src_affinity,
                            "targetAffinity": dst_affinity,
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
                            f"Work payload mismatch: missing required columns on edge: {missing}. "
                            f"provided_schema={self._stable_json(provided_schema)} "
                            f"required_schema={self._stable_json(required_schema)}."
                            f"{suggestion_suffix}"
                        ),
                        edge_id=edge_id,
                        details={
                            "provided_schema": provided_schema,
                            "required_schema": required_schema,
                            "missing_columns": missing,
                            "edgeId": edge_id,
                            "sourceHandle": source_handle,
                            "targetHandle": target_handle,
                            "sourceNodeId": source_id,
                            "targetNodeId": target_id,
                            "sourceLabel": source_label,
                            "targetLabel": target_label,
                            "mode": edge_mode,
                            "sourceAffinity": src_affinity,
                            "targetAffinity": dst_affinity,
                        },
                        suggestions=suggestions or None,
                    ))
                    continue

            # Schema constraint solver (compile-time): schema compatibility + actionable adapter hints.
            if source_type and target_type and source_type != target_type:
                coercion_policy = self._target_coercion_policy(target_node)
                coercion = evaluate_schema_coercion(source_type, target_type, coercion_policy)
                if coercion.get("allowed"):
                    if coercion.get("lossy"):
                        warnings.append(
                            ValidationError(
                                code="TYPE_COERCION_WARNING",
                                message=(
                                    f"Work payload coercion warning on edge '{edge_id}': lossy coercion "
                                    f"{source_type}->{target_type} is allowed by policy '{coercion_policy}'."
                                ),
                                edge_id=edge_id,
                                details={
                                    "edgeId": edge_id,
                                    "sourceNodeId": source_id,
                                    "targetNodeId": target_id,
                                    "sourceHandle": source_handle,
                                    "targetHandle": target_handle,
                                    "mode": edge_mode,
                                    "coercionMode": coercion.get("mode"),
                                    "coercionPolicy": coercion_policy,
                                    "provided_schema": provided_schema,
                                    "required_schema": required_schema,
                                },
                            )
                        )
                    continue
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
                            f"Work payload mismatch on edge '{edge_id}': "
                            f"provided_schema={self._stable_json(provided_schema)} "
                            f"required_schema={self._stable_json(required_schema)}."
                            f"{suggestion_suffix}"
                        ),
                        edge_id=edge_id,
                        details={
                            "provided_schema": provided_schema,
                            "required_schema": required_schema,
                            "edgeId": edge_id,
                            "sourceHandle": source_handle,
                            "targetHandle": target_handle,
                            "sourceNodeId": source_id,
                            "targetNodeId": target_id,
                            "sourceLabel": source_label,
                            "targetLabel": target_label,
                            "mode": edge_mode,
                            "sourceAffinity": src_affinity,
                            "targetAffinity": dst_affinity,
                        },
                        suggestions=suggestions or None,
                    )
                )
                continue
        for bucket in work_handle_signatures.values():
            signatures = bucket.get("signatures", {})
            if not isinstance(signatures, dict):
                continue
            signature_keys = [k for k in signatures.keys() if str(k)]
            if len(signature_keys) <= 1:
                continue
            edge_ids: List[str] = []
            provided_schemas: List[Dict[str, Any]] = []
            for signature_key in signature_keys:
                ids = signatures.get(signature_key)
                if isinstance(ids, list):
                    edge_ids.extend([str(edge_id) for edge_id in ids if str(edge_id)])
                schema = bucket.get("schemas", {}).get(signature_key)
                if isinstance(schema, dict):
                    provided_schemas.append(schema)
            errors.append(
                ValidationError(
                    code=self._schema_code(TYPE_MISMATCH),
                    message=(
                        "Work payload mismatch: multiple inbound edges to the same target handle must provide "
                        "identical schemas."
                    ),
                    edge_id=edge_ids[0] if edge_ids else None,
                    details={
                        "targetNodeId": bucket.get("targetNodeId"),
                        "targetHandle": bucket.get("targetHandle"),
                        "edgeIds": edge_ids,
                        "providedSchemas": provided_schemas,
                    },
                    suggestions=[
                        "Route heterogeneous payloads to different target handles.",
                        "Or align upstream output contracts so provided schemas are identical for this handle.",
                    ],
                )
            )
        return errors, warnings

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
            if node.get("data", {}).get("kind") not in {"llm", "model"}:
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
            edge_data = e.get("data") if isinstance(e.get("data"), dict) else {}
            mode_raw = str(edge_data.get("mode") or "").strip().lower()
            mode = mode_raw if mode_raw in {"work", "param", "control"} else normalize_edge_mode(e)
            if mode != "work":
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
            
            elif node_kind in {"llm", "model"}:
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
_LEGACY_NODE_KINDS: Set[str] = {"source", "transform", "model", "llm", "tool", "component"}


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

