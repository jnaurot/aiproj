from __future__ import annotations

from typing import Dict


def get_feature_flags() -> Dict[str, bool]:
    # Phase 8 hard cut: graph revision + package paths are always on.
    return {
        "GRAPH_STORE_V2_READ": True,
        "GRAPH_STORE_V2_WRITE": True,
        "GRAPH_EXPORT_V2": True,
    }
