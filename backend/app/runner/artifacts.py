from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Protocol, Tuple

from pydantic import BaseModel


# ----------------------------
# Models
# ----------------------------

class Artifact(BaseModel):
    artifact_id: str  # content-addressed hash
    node_kind: str
    params_hash: str
    upstream_ids: List[str]
    created_at: datetime
    execution_version: str

    mime_type: str
    size_bytes: int

    storage_uri: str  # memory://<id>, file://..., s3://...

    payload_schema: Optional[Dict[str, Any]] = None


class RunArtifactBinding(BaseModel):
    run_id: str
    node_id: str
    artifact_id: str
    status: str  # "computed" | "cached" | "reused"
    bound_at: datetime


# ----------------------------
# Store interface
# ----------------------------

class ArtifactStore(Protocol):
    async def exists(self, artifact_id: str) -> bool: ...
    async def get(self, artifact_id: str) -> Artifact: ...
    async def read(self, artifact_id: str) -> bytes: ...
    async def write(self, artifact: Artifact, data: bytes) -> None: ...


# ----------------------------
# In-memory implementation
# ----------------------------

class MemoryArtifactStore:
    """
    Minimal, correct, async-compatible artifact store.
    - Metadata stored separately from bytes
    - storage_uri uses memory://<artifact_id>
    """
    def __init__(self) -> None:
        self._meta: Dict[str, Artifact] = {}
        self._blob: Dict[str, bytes] = {}

    async def exists(self, artifact_id: str) -> bool:
        return artifact_id in self._meta

    async def get(self, artifact_id: str) -> Artifact:
        if artifact_id not in self._meta:
            raise KeyError(f"Artifact not found: {artifact_id}")
        return self._meta[artifact_id]

    async def read(self, artifact_id: str) -> bytes:
        if artifact_id not in self._blob:
            raise KeyError(f"Artifact bytes not found: {artifact_id}")
        return self._blob[artifact_id]

    async def write(self, artifact: Artifact, data: bytes) -> None:
        # Enforce immutability: don't overwrite
        if artifact.artifact_id in self._meta:
            return
        self._meta[artifact.artifact_id] = artifact
        self._blob[artifact.artifact_id] = data


# ----------------------------
# Run bindings (run_id + node_id -> artifact_id)
# ----------------------------

class RunBindings:
    """
    Minimal binding map for a single run.
    If you want cross-run bindings later, move to a repo/db.
    """
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self._bindings: Dict[str, RunArtifactBinding] = {}

    def bind(self, node_id: str, artifact_id: str, status: str = "computed") -> RunArtifactBinding:
        b = RunArtifactBinding(
            run_id=self.run_id,
            node_id=node_id,
            artifact_id=artifact_id,
            status=status,
            bound_at=datetime.now(timezone.utc),
        )
        self._bindings[node_id] = b
        return b

    def get(self, node_id: str) -> Optional[RunArtifactBinding]:
        return self._bindings.get(node_id)

    def artifact_id_for(self, node_id: str) -> Optional[str]:
        b = self._bindings.get(node_id)
        return b.artifact_id if b else None

    def all(self) -> List[RunArtifactBinding]:
        return list(self._bindings.values())
