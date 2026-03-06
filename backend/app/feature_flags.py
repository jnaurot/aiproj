from __future__ import annotations

import os
from typing import Dict


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    value = str(raw).strip().lower()
    return value in {"1", "true", "yes", "on"}


def get_feature_flags() -> Dict[str, bool]:
    return {
        "GRAPH_STORE_V2_READ": _env_bool("GRAPH_STORE_V2_READ", False),
        "GRAPH_STORE_V2_WRITE": _env_bool("GRAPH_STORE_V2_WRITE", False),
        "GRAPH_EXPORT_V2": _env_bool("GRAPH_EXPORT_V2", False),
    }

