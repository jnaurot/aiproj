from typing import Any, Dict
from ...runner.events import RunEventBus
from datetime import datetime, timezone

def iso_now():
    return datetime.now(timezone.utc).isoformat()

async def invoke_mcp(run_id: str, tool_params: Dict[str, Any], bus: RunEventBus) -> Any:
    mcp = tool_params.get("mcp", {})
    server_id = mcp.get("serverId")
    tool_name = mcp.get("toolName")
    args = mcp.get("args", {})

    await bus.emit({
        "type":"log","runId":run_id,"at":iso_now(),
        "level":"info","message":f"Invoking MCP {server_id}:{tool_name} args={args}"
    })

    # TODO: replace with your FastMCP client call.
    # Return something JSON-serializable.
    return {"ok": True, "serverId": server_id, "toolName": tool_name, "echo": args}
