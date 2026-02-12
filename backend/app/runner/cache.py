# backend/app/runner/cache.py
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional, Tuple


def _canon_json(obj: Any) -> str:
    # Stable canonical JSON for hashing
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


class ExecutionCache:
    """
    Artifact-keyed cache index.

    This is NOT the ArtifactStore.
    It's an index that maps an execution key -> artifact_id
    so the scheduler can skip execution and just bind the artifact.

    You can keep it in-memory for now.
    Later this becomes a DB table.
    """

    def __init__(self) -> None:
        # execution_key -> artifact_id
        self._index: Dict[str, str] = {}

    def params_hash(self, params: Dict[str, Any]) -> str:
        return sha256_hex(_canon_json(params))

    def execution_key(
        self,
        node_kind: str,
        params: Dict[str, Any],
        upstream_artifact_ids: List[str],
        execution_version: str,
    ) -> str:
        """
        Deterministic key for a node execution.

        Note: upstream ids must be sorted to avoid nondeterminism due to edge ordering.
        """
        ph = self.params_hash(params)
        upstream_sorted = sorted(upstream_artifact_ids or [])
        key_obj = {
            "node_kind": node_kind,
            "params_hash": ph,
            "upstream_ids": upstream_sorted,
            "execution_version": execution_version,
        }
        return sha256_hex(_canon_json(key_obj))

    async def get_artifact_id(self, execution_key: str) -> Optional[str]:
        return self._index.get(execution_key)

    async def store_artifact_id(self, execution_key: str, artifact_id: str) -> None:
        self._index[execution_key] = artifact_id
