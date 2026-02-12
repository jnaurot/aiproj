from typing import Any, Dict
from ..runner.events import RunEventBus
from datetime import datetime, timezone

# print("[exec_transform] has bus?", hasattr(context, "bus"), type(context.bus))


def iso_now():
    return datetime.now(timezone.utc).isoformat()

async def exec_transform(
    run_id: str,
    node: Dict[str, Any],
    context: ExecutionContext,
    # bus: RunEventBus,
    input_metadata: Optional[FileMetadata],  # ⚠️ ADD THIS
    upstream_artifact_ids: Optional[list[str]] = None
) -> NodeOutput:
    """Execute transform node"""
    node_id = node.get("id", "<missing-node-id>")

    upstream_artifact_ids = upstream_artifact_ids or []
    assert context is not None, "context is None"
    assert hasattr(context, "bus"), "context missing bus"

    if not input_metadata:
        return NodeOutput(
            status="failed",
            metadata=None,
            execution_time_ms=0.0,
            error="Transform node requires input data"
        )
    
    # Read input file from metadata
    input_path = Path(input_metadata.file_path)
    # ... rest of transform logic
    
    params = node["data"].get("params", {})
    await context.bus.emit({"type":"log","runId":run_id,"at":iso_now(),"level":"info","message":f"Transform op: {params.get('op')}", "nodeId": node["id"]})
