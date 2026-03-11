from __future__ import annotations

import os
from typing import Dict


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def get_feature_flags() -> Dict[str, bool]:
    # Phase 8 hard cut: graph revision + package paths are always on.
    return {
        "GRAPH_STORE_V2_READ": True,
        "GRAPH_STORE_V2_WRITE": True,
        "GRAPH_EXPORT_V2": True,
        # TKT-023 rollout toggles for runtime contract strictness.
        "STRICT_SCHEMA_EDGE_CHECKS": _env_bool("STRICT_SCHEMA_EDGE_CHECKS", True),
        "STRICT_SCHEMA_EDGE_CHECKS_V2": _env_bool("STRICT_SCHEMA_EDGE_CHECKS_V2", True),
        "STRICT_COERCION_POLICY": _env_bool("STRICT_COERCION_POLICY", True),
    }
