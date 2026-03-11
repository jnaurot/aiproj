from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Tuple

from .graph_migrations import canonicalize_graph_payload

COMPONENT_SCHEMA_VERSION = 1
ALLOWED_PORT_TYPES = {"table", "json", "text", "binary", "embeddings"}
ALLOWED_TYPED_TYPES = {"table", "json", "text", "binary", "embeddings", "unknown"}


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


def _canonical_api_port(raw: Dict[str, Any]) -> Dict[str, Any]:
    raw_port_type = str(raw.get("portType") or "").strip().lower()
    fallback = raw_port_type if raw_port_type in ALLOWED_PORT_TYPES else "json"
    typed_schema = _canonical_typed_schema(
        raw.get("typedSchema") if isinstance(raw, dict) else None, fallback
    )
    typed_type = str(typed_schema.get("type") or "").strip().lower()
    port_type = typed_type if typed_type in ALLOWED_PORT_TYPES else fallback
    return {
        "name": str(raw.get("name") or "").strip(),
        "portType": port_type,
        "required": bool(raw.get("required", True)),
        "typedSchema": typed_schema,
    }


def _canonical_api_contract(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    value = raw if isinstance(raw, dict) else {}
    outputs_in = value.get("outputs")
    inputs_in = value.get("inputs")
    inputs: List[Dict[str, Any]] = []
    outputs: List[Dict[str, Any]] = []
    if isinstance(inputs_in, list):
        for item in inputs_in:
            if isinstance(item, dict):
                inputs.append(_canonical_api_port(item))
    if isinstance(outputs_in, list):
        for item in outputs_in:
            if isinstance(item, dict):
                outputs.append(_canonical_api_port(item))
    return {"inputs": inputs, "outputs": outputs}


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

    api = definition.get("api")
    if not isinstance(api, dict):
        diagnostics.append(ContractDiagnostic("INVALID_API", "api", "api must be an object"))
        return diagnostics

    seen_names: set[str] = set()
    for section in ("inputs", "outputs"):
        ports = api.get(section)
        if not isinstance(ports, list):
            diagnostics.append(
                ContractDiagnostic("INVALID_API_SECTION", f"api.{section}", f"api.{section} must be an array")
            )
            continue
        for idx, port in enumerate(ports):
            path = f"api.{section}[{idx}]"
            if not isinstance(port, dict):
                diagnostics.append(ContractDiagnostic("INVALID_API_PORT", path, "port must be an object"))
                continue
            name = str(port.get("name") or "").strip()
            if not name:
                diagnostics.append(ContractDiagnostic("MISSING_PORT_NAME", f"{path}.name", "name is required"))
            elif section == "outputs":
                if name in seen_names:
                    diagnostics.append(
                        ContractDiagnostic("DUPLICATE_OUTPUT_NAME", f"{path}.name", f"duplicate output name '{name}'")
                    )
                seen_names.add(name)
            port_type = str(port.get("portType") or "").strip().lower()
            if port_type and port_type not in ALLOWED_PORT_TYPES:
                diagnostics.append(
                    ContractDiagnostic(
                        "INVALID_PORT_TYPE",
                        f"{path}.portType",
                        "portType must be one of: table, json, text, binary, embeddings",
                    )
                )
            typed_schema = port.get("typedSchema")
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
            if port_type and typed in ALLOWED_PORT_TYPES and typed != port_type:
                diagnostics.append(
                    ContractDiagnostic(
                        "PORT_TYPE_DERIVED_FROM_TYPED_SCHEMA",
                        f"{path}.portType",
                        "portType differs from typedSchema.type; canonicalization will derive portType from typedSchema.type",
                        severity="warning",
                    )
                )
            fields = typed_schema.get("fields", [])
            if fields is not None and not isinstance(fields, list):
                diagnostics.append(
                    ContractDiagnostic(
                        "INVALID_TYPED_SCHEMA_FIELDS",
                        f"{path}.typedSchema.fields",
                        "typedSchema.fields must be an array",
                    )
                )
    return diagnostics


def canonicalize_component_definition(
    definition: Dict[str, Any],
    schema_version: int = COMPONENT_SCHEMA_VERSION,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    return migrate_component_definition(definition, int(schema_version), COMPONENT_SCHEMA_VERSION)
