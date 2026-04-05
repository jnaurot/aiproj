from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Tuple

from .graph_migrations import canonicalize_graph_payload
from .runner.capabilities import normalize_node_port_declarations

COMPONENT_SCHEMA_VERSION = 1
ALLOWED_PAYLOAD_TYPES = {"table", "json", "text", "binary", "embeddings"}
ALLOWED_TYPED_TYPES = {"table", "json", "text", "binary", "embeddings", "unknown"}
ALLOWED_EDGE_MODES = {"work", "param", "control"}
INPUT_CONTRACT_CLASSES = ("workInputs", "paramInputs", "controlInputs")
ALLOWED_EXPOSURE_KINDS = {"data_input", "data_output", "param_input", "control_input"}


@dataclass
class ContractDiagnostic:
    code: str
    path: str
    message: str
    severity: Literal["error", "warning"] = "error"

    def as_dict(self) -> Dict[str, str]:
        return {
            "code": self.code,
            "path": self.path,
            "message": self.message,
            "severity": self.severity,
        }


def _canonical_field(raw: Dict[str, Any]) -> Dict[str, Any]:
    name = str(raw.get("name") or "").strip()
    typed = str(raw.get("type") or "unknown").strip().lower() or "unknown"
    if typed not in ALLOWED_TYPED_TYPES:
        typed = "unknown"
    out: Dict[str, Any] = {
        "name": name,
        "type": typed,
        "nullable": bool(raw.get("nullable", False)),
    }
    native_type = raw.get("nativeType")
    if native_type is not None and str(native_type).strip():
        out["nativeType"] = str(native_type).strip()
    return out


def _canonical_typed_schema(raw: Optional[Dict[str, Any]], fallback_type: str) -> Dict[str, Any]:
    value = raw if isinstance(raw, dict) else {}
    typed = str(value.get("type") or "").strip().lower()
    if typed == "string":
        typed = "text"
    if typed not in ALLOWED_TYPED_TYPES:
        typed = str(fallback_type or "json").strip().lower() or "json"
    if typed not in ALLOWED_TYPED_TYPES:
        typed = "json"
    fields_raw = value.get("fields")
    fields: List[Dict[str, Any]] = []
    if isinstance(fields_raw, list):
        for item in fields_raw:
            if isinstance(item, dict):
                fields.append(_canonical_field(item))
    if typed in {"text", "binary", "embeddings"}:
        fields = []
    return {"type": typed, "fields": fields}


def _canonical_api_entry(raw: Dict[str, Any]) -> Dict[str, Any]:
    fallback = "json"
    typed_schema = _canonical_typed_schema(
        raw.get("typedSchema") if isinstance(raw, dict) else None, fallback
    )
    return {
        "name": str(raw.get("name") or "").strip(),
        "required": bool(raw.get("required", True)),
        "typedSchema": typed_schema,
    }


def _canonical_api_contract(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    value = raw if isinstance(raw, dict) else {}
    outputs_in = value.get("outputs")
    inputs_in = value.get("inputs")
    work_inputs_in = value.get("workInputs")
    param_inputs_in = value.get("paramInputs")
    control_inputs_in = value.get("controlInputs")
    inputs: List[Dict[str, Any]] = []
    outputs: List[Dict[str, Any]] = []
    work_inputs: List[Dict[str, Any]] = []
    param_inputs: List[Dict[str, Any]] = []
    control_inputs: List[Dict[str, Any]] = []
    if isinstance(inputs_in, list):
        for item in inputs_in:
            if isinstance(item, dict):
                inputs.append(_canonical_api_entry(item))
    if isinstance(work_inputs_in, list):
        for item in work_inputs_in:
            if isinstance(item, dict):
                work_inputs.append(_canonical_api_entry(item))
    if isinstance(param_inputs_in, list):
        for item in param_inputs_in:
            if isinstance(item, dict):
                param_inputs.append(_canonical_api_entry(item))
    if isinstance(control_inputs_in, list):
        for item in control_inputs_in:
            if isinstance(item, dict):
                control_inputs.append(_canonical_api_entry(item))
    if isinstance(outputs_in, list):
        for item in outputs_in:
            if isinstance(item, dict):
                outputs.append(_canonical_api_entry(item))
    canonical_work_inputs = work_inputs or inputs
    return {
        "inputs": canonical_work_inputs,
        "workInputs": canonical_work_inputs,
        "paramInputs": param_inputs,
        "controlInputs": control_inputs,
        "outputs": outputs,
    }


def _canonical_exposure_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    value = raw if isinstance(raw, dict) else {}
    kind = str(value.get("kind") or "").strip().lower()
    if kind not in ALLOWED_EXPOSURE_KINDS:
        kind = "data_output"
    handle_id = str(value.get("handle_id") or value.get("handleId") or "").strip()
    alias = str(value.get("alias") or value.get("name") or "").strip()
    internal_source_path = str(
        value.get("internal_source_path") or value.get("internalSourcePath") or ""
    ).strip()
    native_contract = _canonical_typed_schema(
        value.get("native_contract") if isinstance(value.get("native_contract"), dict) else value.get("nativeContract"),
        "json",
    )
    exposed = bool(value.get("exposed", True))
    published = bool(value.get("published", False))
    debug_visible = bool(value.get("debug_visible", value.get("debugVisible", False)))
    if published:
        exposed = True
    return {
        "handle_id": handle_id,
        "alias": alias,
        "internal_source_path": internal_source_path,
        "kind": kind,
        "native_contract": native_contract,
        "exposed": exposed,
        "published": published,
        "debug_visible": debug_visible,
    }


def _derive_default_exposure_registry(api_contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for port in api_contract.get("workInputs", []) if isinstance(api_contract, dict) else []:
        if not isinstance(port, dict):
            continue
        name = str(port.get("name") or "").strip()
        if not name:
            continue
        out.append(
            {
                "handle_id": f"work_in::{name}",
                "alias": name,
                "internal_source_path": f"in:{name}",
                "kind": "data_input",
                "native_contract": _canonical_typed_schema(
                    port.get("typedSchema") if isinstance(port.get("typedSchema"), dict) else None,
                    "json",
                ),
                "exposed": True,
                "published": True,
                "debug_visible": False,
            }
        )
    for port in api_contract.get("paramInputs", []) if isinstance(api_contract, dict) else []:
        if not isinstance(port, dict):
            continue
        name = str(port.get("name") or "").strip()
        if not name:
            continue
        out.append(
            {
                "handle_id": f"param_in::{name}",
                "alias": name,
                "internal_source_path": f"param:{name}",
                "kind": "param_input",
                "native_contract": _canonical_typed_schema(
                    port.get("typedSchema") if isinstance(port.get("typedSchema"), dict) else None,
                    "json",
                ),
                "exposed": True,
                "published": True,
                "debug_visible": False,
            }
        )
    for port in api_contract.get("controlInputs", []) if isinstance(api_contract, dict) else []:
        if not isinstance(port, dict):
            continue
        name = str(port.get("name") or "").strip()
        if not name:
            continue
        out.append(
            {
                "handle_id": f"control_in::{name}",
                "alias": name,
                "internal_source_path": f"control:{name}",
                "kind": "control_input",
                "native_contract": _canonical_typed_schema(
                    port.get("typedSchema") if isinstance(port.get("typedSchema"), dict) else None,
                    "json",
                ),
                "exposed": True,
                "published": False,
                "debug_visible": True,
            }
        )
    for port in api_contract.get("outputs", []) if isinstance(api_contract, dict) else []:
        if not isinstance(port, dict):
            continue
        name = str(port.get("name") or "").strip()
        if not name:
            continue
        out.append(
            {
                "handle_id": f"data_out::{name}",
                "alias": name,
                "internal_source_path": f"out:{name}",
                "kind": "data_output",
                "native_contract": _canonical_typed_schema(
                    port.get("typedSchema") if isinstance(port.get("typedSchema"), dict) else None,
                    "json",
                ),
                "exposed": True,
                "published": True,
                "debug_visible": False,
            }
        )
    return out


def canonicalize_exposure_registry(raw: Any, api_contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    value = raw if isinstance(raw, list) else []
    records = [_canonical_exposure_record(item) for item in value if isinstance(item, dict)]
    if not records:
        records = _derive_default_exposure_registry(api_contract)
    out: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    for idx, rec in enumerate(records):
        handle_id = str(rec.get("handle_id") or "").strip() or f"exposed::{idx + 1}"
        if handle_id in seen_ids:
            suffix = 2
            candidate = f"{handle_id}__{suffix}"
            while candidate in seen_ids:
                suffix += 1
                candidate = f"{handle_id}__{suffix}"
            handle_id = candidate
        seen_ids.add(handle_id)
        next_rec = dict(rec)
        next_rec["handle_id"] = handle_id
        out.append(next_rec)
    return out


def materialize_exposure_profiles(exposure_registry: Any) -> Dict[str, List[Dict[str, Any]]]:
    records = exposure_registry if isinstance(exposure_registry, list) else []
    normalized = [_canonical_exposure_record(item) for item in records if isinstance(item, dict)]
    published_profile = [
        rec for rec in normalized if bool(rec.get("published", False))
    ]
    debug_profile = [
        rec
        for rec in normalized
        if bool(rec.get("published", False)) or bool(rec.get("debug_visible", False))
    ]
    return {"published_profile": published_profile, "debug_profile": debug_profile}


def component_contract_diff(
    from_published: List[Dict[str, Any]], to_published: List[Dict[str, Any]]
) -> Dict[str, Any]:
    before = {
        str(item.get("handle_id") or "").strip(): item
        for item in (from_published or [])
        if isinstance(item, dict) and str(item.get("handle_id") or "").strip()
    }
    after = {
        str(item.get("handle_id") or "").strip(): item
        for item in (to_published or [])
        if isinstance(item, dict) and str(item.get("handle_id") or "").strip()
    }
    removed = sorted([hid for hid in before.keys() if hid not in after])
    added = sorted([hid for hid in after.keys() if hid not in before])
    retyped: List[Dict[str, str]] = []
    for handle_id in sorted(set(before.keys()).intersection(after.keys())):
        left = before[handle_id]
        right = after[handle_id]
        left_kind = str(left.get("kind") or "").strip()
        right_kind = str(right.get("kind") or "").strip()
        left_type = str(((left.get("native_contract") or {}).get("type") or "")).strip().lower()
        right_type = str(((right.get("native_contract") or {}).get("type") or "")).strip().lower()
        if left_kind != right_kind or left_type != right_type:
            retyped.append(
                {
                    "handle_id": handle_id,
                    "before_kind": left_kind,
                    "after_kind": right_kind,
                    "before_type": left_type,
                    "after_type": right_type,
                }
            )
    breaking = bool(removed or retyped)
    return {
        "breaking": breaking,
        "removed": removed,
        "added": added,
        "retyped": retyped,
    }


def build_component_migration_report(
    from_published: List[Dict[str, Any]],
    to_published: List[Dict[str, Any]],
    compatibility_mapping: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    mapping = compatibility_mapping if isinstance(compatibility_mapping, dict) else {}
    diff = component_contract_diff(from_published, to_published)
    actions: List[Dict[str, Any]] = []
    for handle_id in diff.get("removed", []):
        mapped = str(mapping.get(handle_id) or "").strip()
        actions.append(
            {
                "kind": "removed",
                "from_handle_id": handle_id,
                "to_handle_id": mapped or None,
                "status": "mapped" if mapped else "unmapped",
            }
        )
    for item in diff.get("retyped", []):
        if not isinstance(item, dict):
            continue
        handle_id = str(item.get("handle_id") or "").strip()
        mapped = str(mapping.get(handle_id) or "").strip()
        actions.append(
            {
                "kind": "retyped",
                "from_handle_id": handle_id,
                "to_handle_id": mapped or handle_id,
                "status": "mapped" if mapped else "unmapped",
                "before_type": str(item.get("before_type") or ""),
                "after_type": str(item.get("after_type") or ""),
            }
        )
    return {
        "breaking": bool(diff.get("breaking")),
        "diff": diff,
        "actions": actions,
    }


def migrate_component_definition(
    definition: Dict[str, Any],
    from_schema_version: int,
    to_schema_version: int = COMPONENT_SCHEMA_VERSION,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Migration hook for component definitions.
    Currently canonicalization-only because schema version is still 1.
    Returns (migrated_definition, migration_notes[]).
    """
    current = copy.deepcopy(definition if isinstance(definition, dict) else {})
    notes: List[Dict[str, Any]] = []
    if int(from_schema_version) != int(to_schema_version):
        notes.append(
            {
                "fromSchemaVersion": int(from_schema_version),
                "toSchemaVersion": int(to_schema_version),
                "action": "canonicalize",
            }
        )
    current["api"] = _canonical_api_contract(current.get("api"))
    current["exposureRegistry"] = canonicalize_exposure_registry(
        current.get("exposureRegistry"), current["api"]
    )
    current.update(materialize_exposure_profiles(current.get("exposureRegistry")))
    normalized_graph, graph_notes = canonicalize_graph_payload(current.get("graph"))
    current["graph"] = normalized_graph
    for note in graph_notes:
        notes.append({"action": "graph_canonicalize", **note})
    config_schema = current.get("configSchema")
    if not isinstance(config_schema, dict):
        current["configSchema"] = {}
    return current, notes


def validate_component_definition(definition: Dict[str, Any]) -> List[ContractDiagnostic]:
    diagnostics: List[ContractDiagnostic] = []
    if not isinstance(definition, dict):
        return [ContractDiagnostic("INVALID_DEFINITION", "definition", "definition must be an object")]

    graph = definition.get("graph")
    if not isinstance(graph, dict):
        diagnostics.append(ContractDiagnostic("INVALID_GRAPH", "graph", "graph must be an object"))
    else:
        if not isinstance(graph.get("nodes"), list):
            diagnostics.append(ContractDiagnostic("INVALID_GRAPH_NODES", "graph.nodes", "graph.nodes must be an array"))
        if not isinstance(graph.get("edges"), list):
            diagnostics.append(ContractDiagnostic("INVALID_GRAPH_EDGES", "graph.edges", "graph.edges must be an array"))
        nodes = graph.get("nodes") if isinstance(graph.get("nodes"), list) else []
        for idx, raw_node in enumerate(nodes):
            if not isinstance(raw_node, dict):
                continue
            data = raw_node.get("data") if isinstance(raw_node.get("data"), dict) else {}
            if str(data.get("kind") or "").strip().lower() != "component":
                continue
            params = data.get("params") if isinstance(data.get("params"), dict) else {}
            bindings = params.get("bindings") if isinstance(params.get("bindings"), dict) else {}
            outputs = bindings.get("outputs") if isinstance(bindings.get("outputs"), dict) else {}
            if outputs:
                diagnostics.append(
                    ContractDiagnostic(
                        "COMPONENT_LEGACY_OUTPUT_BINDINGS_DEPRECATED",
                        f"graph.nodes[{idx}].data.params.bindings.outputs",
                        "Legacy component output bindings are deprecated; use API Contract exposure mappings only.",
                    )
                )
            for out_name, binding in outputs.items():
                if not isinstance(binding, dict):
                    continue
                mode = str(binding.get("artifact") or "current").strip().lower() or "current"
                if mode != "current":
                    diagnostics.append(
                        ContractDiagnostic(
                            "COMPONENT_OUTPUT_ARTIFACT_MODE_UNSUPPORTED",
                            f"graph.nodes[{idx}].data.params.bindings.outputs.{str(out_name)}.artifact",
                            "Only artifact mode 'current' is supported.",
                        )
                    )

    api = definition.get("api")
    if not isinstance(api, dict):
        diagnostics.append(ContractDiagnostic("INVALID_API", "api", "api must be an object"))
        return diagnostics

    seen_names: set[str] = set()
    for section in ("inputs", "workInputs", "paramInputs", "controlInputs", "outputs"):
        entries = api.get(section)
        if not isinstance(entries, list):
            if section in {"workInputs", "paramInputs", "controlInputs"} and entries is None:
                continue
            diagnostics.append(
                ContractDiagnostic("INVALID_API_SECTION", f"api.{section}", f"api.{section} must be an array")
            )
            continue
        for idx, entry in enumerate(entries):
            path = f"api.{section}[{idx}]"
            if not isinstance(entry, dict):
                diagnostics.append(ContractDiagnostic("INVALID_API_ENTRY", path, "entry must be an object"))
                continue
            name = str(entry.get("name") or "").strip()
            if not name:
                diagnostics.append(ContractDiagnostic("MISSING_ENTRY_NAME", f"{path}.name", "name is required"))
            elif section == "outputs":
                if name in seen_names:
                    diagnostics.append(
                        ContractDiagnostic("DUPLICATE_OUTPUT_NAME", f"{path}.name", f"duplicate output name '{name}'")
                    )
                seen_names.add(name)
            typed_schema = entry.get("typedSchema")
            if not isinstance(typed_schema, dict):
                diagnostics.append(ContractDiagnostic("MISSING_TYPED_SCHEMA", f"{path}.typedSchema", "typedSchema is required"))
                continue
            typed = str(typed_schema.get("type") or "").strip().lower()
            if typed not in ALLOWED_TYPED_TYPES:
                diagnostics.append(
                    ContractDiagnostic(
                        "INVALID_TYPED_SCHEMA_TYPE",
                        f"{path}.typedSchema.type",
                        "typedSchema.type must be one of: table, json, text, binary, embeddings, unknown",
                    )
                )
            if typed == "string":
                typed = "text"
            fields = typed_schema.get("fields", [])
            if fields is not None and not isinstance(fields, list):
                diagnostics.append(
                    ContractDiagnostic(
                        "INVALID_TYPED_SCHEMA_FIELDS",
                        f"{path}.typedSchema.fields",
                        "typedSchema.fields must be an array",
                    )
                )

    exposure_registry = definition.get("exposureRegistry")
    if exposure_registry is not None and not isinstance(exposure_registry, list):
        diagnostics.append(
            ContractDiagnostic(
                "INVALID_EXPOSURE_REGISTRY",
                "exposureRegistry",
                "exposureRegistry must be an array",
            )
        )
        return diagnostics

    if isinstance(exposure_registry, list):
        seen_ids: set[str] = set()
        for idx, raw in enumerate(exposure_registry):
            path = f"exposureRegistry[{idx}]"
            if not isinstance(raw, dict):
                diagnostics.append(
                    ContractDiagnostic("INVALID_EXPOSURE_ENTRY", path, "exposure entry must be an object")
                )
                continue
            handle_id = str(raw.get("handle_id") or raw.get("handleId") or "").strip()
            alias = str(raw.get("alias") or raw.get("name") or "").strip()
            kind = str(raw.get("kind") or "").strip().lower()
            if not handle_id:
                diagnostics.append(
                    ContractDiagnostic("MISSING_EXPOSURE_HANDLE_ID", f"{path}.handle_id", "handle_id is required")
                )
            elif handle_id in seen_ids:
                diagnostics.append(
                    ContractDiagnostic(
                        "DUPLICATE_EXPOSURE_HANDLE_ID",
                        f"{path}.handle_id",
                        f"duplicate handle_id '{handle_id}'",
                    )
                )
            else:
                seen_ids.add(handle_id)
            if not alias:
                diagnostics.append(
                    ContractDiagnostic("MISSING_EXPOSURE_ALIAS", f"{path}.alias", "alias is required")
                )
            if kind not in ALLOWED_EXPOSURE_KINDS:
                diagnostics.append(
                    ContractDiagnostic(
                        "INVALID_EXPOSURE_KIND",
                        f"{path}.kind",
                        "kind must be one of: data_input, data_output, param_input, control_input",
                    )
                )
            exposed = bool(raw.get("exposed", True))
            published = bool(raw.get("published", False))
            if published and not exposed:
                diagnostics.append(
                    ContractDiagnostic(
                        "INVALID_EXPOSURE_LIFECYCLE",
                        f"{path}.published",
                        "published handle must also be exposed",
                    )
                )
    return diagnostics


def canonicalize_component_definition(
    definition: Dict[str, Any],
    schema_version: int = COMPONENT_SCHEMA_VERSION,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    return migrate_component_definition(definition, int(schema_version), COMPONENT_SCHEMA_VERSION)


def normalize_edge_mode(raw: Any) -> str:
    mode = str(raw or "work").strip().lower() or "work"
    return mode if mode in ALLOWED_EDGE_MODES else "work"


def edge_mode_requires_payload_compatibility(raw: Any) -> bool:
    return normalize_edge_mode(raw) == "work"


def _canonical_input_contract_class(raw: Any) -> Dict[str, Any]:
    value = raw if isinstance(raw, dict) else {}
    default_schema = value.get("defaultSchema") if isinstance(value.get("defaultSchema"), dict) else None
    handles_raw = value.get("handles") if isinstance(value.get("handles"), dict) else {}
    handles: Dict[str, Dict[str, Any]] = {}
    for key, entry in handles_raw.items():
        handle = str(key or "").strip()
        if not handle or not isinstance(entry, dict):
            continue
        handles[handle] = entry
    out: Dict[str, Any] = {"handles": handles}
    if default_schema is not None:
        out["defaultSchema"] = default_schema
    return out


def canonicalize_input_contracts(raw: Any) -> Dict[str, Dict[str, Any]]:
    value = raw if isinstance(raw, dict) else {}
    return {
        key: _canonical_input_contract_class(value.get(key))
        for key in INPUT_CONTRACT_CLASSES
    }


def canonicalize_port_declarations(kind: str, raw: Any) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Canonicalize per-node port declarations for FE/BE parity.
    """
    return normalize_node_port_declarations(str(kind or "").strip().lower(), raw)
