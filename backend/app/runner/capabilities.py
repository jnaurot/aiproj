from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Literal, Set

_DEFAULT_CAPABILITIES: Dict[str, Any] = {
    "schemaVersion": 1,
    "edgeModes": ["work", "param", "control"],
    "allowedPayloadTypes": ["table", "json", "text", "binary", "embeddings", "image", "audio", "video"],
    "nodes": {
        "model": {
            "in": ["text", "json", "table", "image", "audio", "video"],
            "out": ["text", "json", "embeddings", "image", "audio", "video"],
        },
        "llm": {"in": ["text", "json", "table"], "out": ["text", "json", "embeddings"]},
        "transform": {"in": ["table"], "out": ["table"]},
        "source": {"in": [], "out": ["table", "json", "text", "binary"]},
        "component": {
            "in": ["table", "json", "text", "binary", "embeddings"],
            "out": ["table", "json", "text", "binary", "embeddings"],
        },
        "tool": {
            "in": ["table", "json", "text", "binary", "embeddings"],
            "out": ["text", "json", "binary"],
            "byProvider": {},
        },
    },
}


def _shared_caps_path() -> Path:
    # backend/app/runner -> repo root
    return Path(__file__).resolve().parents[3] / "shared" / "schema_capabilities.v1.json"


@lru_cache(maxsize=1)
def load_schema_capabilities() -> Dict[str, Any]:
    path = _shared_caps_path()
    if not path.exists():
        return dict(_DEFAULT_CAPABILITIES)
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(parsed, dict):
            return dict(_DEFAULT_CAPABILITIES)
        return parsed
    except Exception:
        return dict(_DEFAULT_CAPABILITIES)


def allowed_payload_types() -> Set[str]:
    caps = load_schema_capabilities()
    values = caps.get("allowedPayloadTypes")
    if not isinstance(values, list):
        return set(_DEFAULT_CAPABILITIES["allowedPayloadTypes"])
    return {str(v) for v in values}


def _node_caps(kind: str) -> Dict[str, Any]:
    caps = load_schema_capabilities()
    nodes = caps.get("nodes")
    if not isinstance(nodes, dict):
        return {}
    data = nodes.get(kind)
    return data if isinstance(data, dict) else {}


def allowed_ports(kind: str, direction: str, provider: str | None = None) -> Set[str]:
    node = _node_caps(kind)
    if kind != "tool":
        values = node.get(direction, [])
        return {str(v) for v in values if isinstance(v, str)}

    by_provider = node.get("byProvider")
    if isinstance(by_provider, dict) and provider:
        p = by_provider.get(provider)
        if isinstance(p, dict):
            values = p.get(direction, [])
            provider_set = {str(v) for v in values if isinstance(v, str)}
            if provider_set:
                return provider_set

    values = node.get(direction, [])
    return {str(v) for v in values if isinstance(v, str)}


def capabilities_response() -> Dict[str, Any]:
    caps = load_schema_capabilities()
    # Return parsed JSON as-is for FE parity checks.
    return caps


def allowed_edge_modes() -> Set[str]:
    caps = load_schema_capabilities()
    values = caps.get("edgeModes")
    if not isinstance(values, list):
        return {"work", "param", "control"}
    return {str(v).strip().lower() for v in values if str(v).strip()}


def port_contract(kind: str, direction: str, handle: str | None = None) -> Dict[str, Any]:
    node = _node_caps(kind)
    ports = node.get("ports") if isinstance(node.get("ports"), dict) else {}
    by_dir = ports.get(direction) if isinstance(ports, dict) and isinstance(ports.get(direction), dict) else {}
    h = str(handle or "default").strip() or "default"
    entry = by_dir.get(h)
    if not isinstance(entry, dict):
        entry = by_dir.get("default") if isinstance(by_dir.get("default"), dict) else {}
    out = dict(entry) if isinstance(entry, dict) else {}
    out.setdefault("affinity", "work")
    if direction == "in":
        out.setdefault("behavior", "single_item")
    return out


PortDirection = Literal["in", "out"]
PortPlane = Literal["work", "param", "control"]
PortCardinality = Literal["one", "many"]


def _normalize_plane(raw: Any, default: PortPlane = "work") -> PortPlane:
    value = str(raw or "").strip().lower()
    if value in {"work", "param", "control"}:
        return value  # type: ignore[return-value]
    return default


def _normalize_cardinality(raw: Any, default: PortCardinality = "many") -> PortCardinality:
    value = str(raw or "").strip().lower()
    if value in {"one", "many"}:
        return value  # type: ignore[return-value]
    return default


def _infer_plane_from_handle(handle: str) -> PortPlane:
    h = str(handle or "").strip().lower()
    if h.startswith("param"):
        return "param"
    if h.startswith("control") or h.startswith("ctl"):
        return "control"
    return "work"


def _default_port_behavior_for_plane(plane: PortPlane) -> str:
    if plane == "param":
        return "once"
    return "single_item"


def _normalize_port_declaration_entry(
    handle: str,
    entry: Dict[str, Any] | None,
    *,
    direction: PortDirection,
    fallback_plane: PortPlane,
) -> Dict[str, Any]:
    payload = entry if isinstance(entry, dict) else {}
    plane = _normalize_plane(payload.get("plane"), fallback_plane)
    affinity = _normalize_plane(payload.get("affinity"), plane)
    required = bool(payload.get("required", False))
    cardinality = _normalize_cardinality(payload.get("cardinality"), "many")
    out: Dict[str, Any] = {
        "plane": plane,
        "affinity": affinity,
        "required": required,
        "cardinality": cardinality,
    }
    if direction == "in":
        behavior = str(payload.get("behavior") or "").strip().lower() or _default_port_behavior_for_plane(plane)
        if behavior not in {"once", "single_item", "batch"}:
            behavior = _default_port_behavior_for_plane(plane)
        out["behavior"] = behavior
    return out


def default_node_port_declarations(kind: str) -> Dict[str, Dict[str, Dict[str, Any]]]:
    node = _node_caps(kind)
    ports = node.get("ports") if isinstance(node.get("ports"), dict) else {}
    in_ports_cfg = ports.get("in") if isinstance(ports.get("in"), dict) else {}
    out_ports_cfg = ports.get("out") if isinstance(ports.get("out"), dict) else {}
    input_contracts = node.get("inputContracts") if isinstance(node.get("inputContracts"), dict) else {}

    out: Dict[str, Dict[str, Dict[str, Any]]] = {"in": {}, "out": {}}

    # Out ports: always provide at least "out" unless explicitly empty for a kind.
    declared_out_handles = [h for h in out_ports_cfg.keys() if isinstance(h, str) and h.strip() and h != "default"]
    if "default" in out_ports_cfg or not declared_out_handles:
        declared_out_handles.insert(0, "out")
    for raw_handle in declared_out_handles:
        handle = str(raw_handle).strip() or "out"
        cfg = out_ports_cfg.get(raw_handle) if isinstance(out_ports_cfg.get(raw_handle), dict) else {}
        plane = _normalize_plane((cfg or {}).get("affinity"), _infer_plane_from_handle(handle))
        out["out"][handle] = _normalize_port_declaration_entry(
            handle,
            cfg,
            direction="out",
            fallback_plane=plane,
        )

    # In ports from capability input contracts first, then explicit entries.
    by_plane = {
        "work": input_contracts.get("workInputs"),
        "param": input_contracts.get("paramInputs"),
        "control": input_contracts.get("controlInputs"),
    }
    for plane, handles in by_plane.items():
        if not isinstance(handles, list):
            continue
        for raw_handle in handles:
            handle = str(raw_handle or "").strip()
            if not handle:
                continue
            cfg = in_ports_cfg.get(handle) if isinstance(in_ports_cfg.get(handle), dict) else {}
            out["in"][handle] = _normalize_port_declaration_entry(
                handle,
                cfg,
                direction="in",
                fallback_plane=_normalize_plane((cfg or {}).get("affinity"), plane),  # type: ignore[arg-type]
            )

    for raw_handle, raw_cfg in in_ports_cfg.items():
        if not isinstance(raw_handle, str) or not raw_handle.strip() or raw_handle == "default":
            continue
        handle = raw_handle.strip()
        if handle in out["in"]:
            continue
        cfg = raw_cfg if isinstance(raw_cfg, dict) else {}
        plane = _normalize_plane((cfg or {}).get("affinity"), _infer_plane_from_handle(handle))
        out["in"][handle] = _normalize_port_declaration_entry(
            handle,
            cfg,
            direction="in",
            fallback_plane=plane,
        )

    if not out["in"] and kind not in {"source"}:
        # Most node kinds accept work payload on "in" by default.
        out["in"]["in"] = _normalize_port_declaration_entry(
            "in",
            in_ports_cfg.get("default") if isinstance(in_ports_cfg.get("default"), dict) else {},
            direction="in",
            fallback_plane="work",
        )
    return out


def normalize_node_port_declarations(
    kind: str,
    raw: Any,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    defaults = default_node_port_declarations(kind)
    if not isinstance(raw, dict):
        return defaults
    out: Dict[str, Dict[str, Dict[str, Any]]] = {"in": {}, "out": {}}
    for direction in ("in", "out"):
        raw_dir = raw.get(direction)
        if not isinstance(raw_dir, dict):
            out[direction] = dict(defaults.get(direction, {}))
            continue
        for raw_handle, raw_decl in raw_dir.items():
            handle = str(raw_handle or "").strip()
            if not handle:
                continue
            base = defaults.get(direction, {}).get(handle)
            fallback_plane = _normalize_plane(
                (base or {}).get("plane"),
                _infer_plane_from_handle(handle),
            )
            out[direction][handle] = _normalize_port_declaration_entry(
                handle,
                raw_decl if isinstance(raw_decl, dict) else {},
                direction=direction,  # type: ignore[arg-type]
                fallback_plane=fallback_plane,
            )
        for handle, decl in defaults.get(direction, {}).items():
            out[direction].setdefault(handle, dict(decl))
    return out


def resolve_node_port_declarations(node: Dict[str, Any], direction: PortDirection) -> Dict[str, Dict[str, Any]]:
    data = (node.get("data") or {}) if isinstance(node, dict) else {}
    kind = str(data.get("kind") or "").strip().lower()
    raw = data.get("portDeclarations") if isinstance(data.get("portDeclarations"), dict) else None
    normalized = normalize_node_port_declarations(kind, raw)
    by_dir = normalized.get(direction)
    return by_dir if isinstance(by_dir, dict) else {}


def resolve_node_port_declaration(
    node: Dict[str, Any],
    direction: PortDirection,
    handle: str | None = None,
) -> Dict[str, Any]:
    by_dir = resolve_node_port_declarations(node, direction)
    h = str(handle or ("in" if direction == "in" else "out")).strip() or ("in" if direction == "in" else "out")
    decl = by_dir.get(h)
    if isinstance(decl, dict):
        return dict(decl)
    if h != "default" and isinstance(by_dir.get("default"), dict):
        return dict(by_dir.get("default"))  # type: ignore[arg-type]
    return {}


def capability_signature() -> str:
    caps = load_schema_capabilities()
    payload = json.dumps(caps, sort_keys=True, separators=(",", ":")).encode("utf-8")
    import hashlib

    return hashlib.sha256(payload).hexdigest()
