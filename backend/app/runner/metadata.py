# backend/app/runner/metadata.py
from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional, Literal, Dict, Any
from datetime import datetime, timezone
from dataclasses import dataclass, field

from .artifacts import ArtifactStore, RunBindings
from .events import RunEventBus

class FileMetadata(BaseModel):
    """Metadata for data passing between nodes"""
    
    # File identification
    file_path: str
    file_type: Literal["csv", "tsv", "parquet", "json", "txt", "excel", "pdf", "binary", "image"]
    mime_type: str
    size_bytes: int = 0
    
    # Data schema
    data_schema: Optional[Dict[str, Any]] = None
    row_count: Optional[int] = None
    
    # Access control
    access_method: Literal["local", "s3", "postgres", "http"] = "local"
    credentials_key: Optional[str] = None
    
    # Caching & staleness
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    content_hash: str

    
    # Lineage
    node_id: str = ""
    params_hash: str = ""
    input_metadata_hash: Optional[str] = None  # Hash of upstream metadata
    
    # Performance
    estimated_memory_mb: float = 0.0
    is_partitioned: bool = False
    partition_key: Optional[str] = None

class NodeOutput(BaseModel):
    """What each node produces"""
    status: Literal["succeeded", "failed", "skipped"]
    metadata: Optional[FileMetadata] = None
    data: Optional[Any] = None
    execution_time_ms: float
    error: Optional[str] = None
    
    # Staleness
    is_stale: bool = False
    stale_reason: Optional[str] = None

@dataclass
class GraphContext:
    """Per-graph execution context passed to all node executors."""
    run_id: str
    bus: RunEventBus
    artifact_store: ArtifactStore
    bindings: RunBindings
    graph_id: str = ""
    runtime_ref: Optional[Any] = None
    planner_ref: Optional[Any] = None
    outputs: Dict[str, NodeOutput] = field(default_factory=dict)
    metadata_cache: Dict[str, FileMetadata] = field(default_factory=dict)
    execution_version: str = "v1"


# Backward-compatible alias while the codebase migrates to GraphContext.
ExecutionContext = GraphContext
