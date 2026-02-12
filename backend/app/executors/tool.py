from typing import Any, Dict
from ..runner.events import RunEventBus
from datetime import datetime, timezone
from ..tools.providers.mcp import invoke_mcp

# print("[exec_tool] has bus?", hasattr(context, "bus"), type(context.bus))


def iso_now():
    return datetime.now(timezone.utc).isoformat()

async def exec_tool(
    run_id: str,
    node: Dict[str, Any],
    context: ExecutionContext,
    # bus: RunEventBus,
    input_metadata: Optional[FileMetadata],  # ⚠️ ADD THIS
    upstream_artifact_ids: Optional[list[str]] = None
) -> NodeOutput:
    """Execute tool node"""
    node_id = node.get("id", "<missing-node-id>")

    upstream_artifact_ids = upstream_artifact_ids or []
    assert context is not None, "context is None"
    assert hasattr(context, "bus"), "context missing bus"

    if not input_metadata:
        return NodeOutput(
            status="failed",
            metadata=None,
            execution_time_ms=0.0,
            error="Tool node requires input data"
        )
    params = node["data"].get("params", {})
    provider = params.get("provider")
    await context.bus.emit({"type":"log","runId":run_id,"at":iso_now(),"level":"info","message":f"Tool provider: {provider}", "nodeId": node["id"]})

    if provider == "mcp":
        result = await invoke_mcp(run_id, params, bus)
        await context.bus.emit({"type":"log","runId":run_id,"at":iso_now(),"level":"info","message":f"MCP result: {result}", "nodeId": node["id"]})
