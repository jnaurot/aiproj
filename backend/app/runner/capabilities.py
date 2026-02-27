from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Set

_DEFAULT_CAPABILITIES: Dict[str, Any] = {
    "schemaVersion": 1,
    "allowedPortTypes": ["table", "json", "text", "binary", "embeddings"],
    "nodes": {
        "llm": {"in": ["text", "json", "table"], "out": ["text", "json", "embeddings"]},
        "transform": {"in": ["table"], "out": ["table"]},
        "source": {"in": [], "out": ["table", "json", "text", "binary"]},
        "tool": {
            "in": ["table", "json", "text", "binary", "embeddings"],
            "out": ["text", "json", "binary"],
            "byProvider": {},
        },
    },
}


def _shared_caps_path() -> Path:
    # backend/app/runner -> repo root
    return Path(__file__).resolve().parents[3] / "shared" / "port_capabilities.v1.json"


@lru_cache(maxsize=1)
def load_port_capabilities() -> Dict[str, Any]:
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


def allowed_port_types() -> Set[str]:
    caps = load_port_capabilities()
    values = caps.get("allowedPortTypes")
    if not isinstance(values, list):
        return set(_DEFAULT_CAPABILITIES["allowedPortTypes"])
    return {str(v) for v in values}


def _node_caps(kind: str) -> Dict[str, Any]:
    caps = load_port_capabilities()
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
    caps = load_port_capabilities()
    # Return parsed JSON as-is for FE parity checks.
    return caps


def capability_signature() -> str:
    caps = load_port_capabilities()
    payload = json.dumps(caps, sort_keys=True, separators=(",", ":")).encode("utf-8")
    import hashlib

    return hashlib.sha256(payload).hexdigest()
