from typing import Any, Dict, Optional
from ..runner.events import RunEventBus
from ..runner.metadata import ExecutionContext, FileMetadata, NodeOutput, ArtifactStore
from datetime import datetime, timezone

print("[exec_tool] Module loaded")


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def exec_tool(
    run_id: str,
    node: Dict[str, Any],
    context: ExecutionContext,
    input_metadata: Optional[FileMetadata],  # Added
    upstream_artifact_ids: Optional[list[str]] = None
) -> NodeOutput:
    """Execute tool node"""
    node_id = node.get("id", "<missing-node-id>")

    upstream_artifact_ids = upstream_artifact_ids or []
    assert context is not None, "context is None"
    assert hasattr(context, "bus"), "context missing bus"
    assert hasattr(context, "artifact_store"), "context missing artifact_store"

    if not input_metadata:
        await context.bus.emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "error",
            "message": "Tool node requires input data",
            "nodeId": node_id
        })
        return NodeOutput(
            status="failed",
            metadata=None,
            execution_time_ms=0.0,
            error="Tool node requires input data"
        )

    params = node["data"].get("params", {})
    provider = params.get("provider")

    await context.bus.emit({
        "type": "log",
        "runId": run_id,
        "at": iso_now(),
        "level": "info",
        "message": f"Tool provider: {provider}",
        "nodeId": node_id
    })

    if provider == "mcp":
        # Import only when needed
        try:
            from ..tools.providers.mcp import invoke_mcp
        except ImportError:
            await context.bus.emit({
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "error",
                "message": "MCP provider not available. Ensure tools.providers.mcp is imported in tools/__init__.py",
                "nodeId": node_id
            })
            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=0.0,
                error="MCP provider not available"
            )

        try:
            # Call MCP tool
            result = await invoke_mcp(run_id, params, bus=context.bus, run_id=run_id)

            await context.bus.emit({
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "info",
                "message": f"MCP tool executed successfully: {result}",
                "nodeId": node_id
            })

            return NodeOutput(
                status="succeeded",
                metadata=None,
                execution_time_ms=0.0,
                data=str(result)
            )

        except Exception as e:
            error_msg = f"Tool execution failed: {str(e)}"
            await context.bus.emit({
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "error",
                "message": error_msg,
                "nodeId": node_id
            })

            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=0.0,
                error=error_msg
            )

    elif provider == "python":
        # Python tool execution
        try:
            import numpy as np

            code = params.get("python_code", "")
            if not code:
                error_msg = "Python code not provided"
                await context.bus.emit({
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "error",
                    "message": error_msg,
                    "nodeId": node_id
                })
                return NodeOutput(status="failed", metadata=None, execution_time_ms=0.0, error=error_msg)

            # Execute in isolated namespace
            result = exec(code, {"input": None, "np": np, "pd": None})
            output = str(result)

            await context.bus.emit({
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "info",
                "message": "Python tool executed successfully",
                "nodeId": node_id
            })

            return NodeOutput(
                status="succeeded",
                metadata=None,
                execution_time_ms=0.0,
                data=output
            )

        except Exception as e:
            error_msg = f"Python tool execution failed: {str(e)}"
            await context.bus.emit({
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "error",
                "message": error_msg,
                "nodeId": node_id
            })

            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=0.0,
                error=error_msg
            )

    elif provider == "api":
        # API tool execution
        try:
            import aiohttp

            url = params.get("url", "")
            method = params.get("method", "GET")
            headers = params.get("headers", {})
            body = params.get("body", None)

            if not url:
                error_msg = "API URL not provided"
                await context.bus.emit({
                    "type": "log",
                    "runId": run_id,
                    "at": iso_now(),
                    "level": "error",
                    "message": error_msg,
                    "nodeId": node_id
                })
                return NodeOutput(status="failed", metadata=None, execution_time_ms=0.0, error=error_msg)

            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method,
                    url,
                    headers=headers,
                    json=body
                ) as response:
                    result_data = await response.text()
                    status = response.status

                    await context.bus.emit({
                        "type": "log",
                        "runId": run_id,
                        "at": iso_now(),
                        "level": "info",
                        "message": f"API tool executed with status {status}",
                        "nodeId": node_id
                    })

                    return NodeOutput(
                        status="succeeded" if status in [200, 201, 204] else "failed",
                        metadata=None,
                        execution_time_ms=0.0,
                        data=result_data,
                        error=f"API returned status {status}" if status not in [200, 201, 204] else None
                    )

        except Exception as e:
            error_msg = f"API tool execution failed: {str(e)}"
            await context.bus.emit({
                "type": "log",
                "runId": run_id,
                "at": iso_now(),
                "level": "error",
                "message": error_msg,
                "nodeId": node_id
            })

            return NodeOutput(
                status="failed",
                metadata=None,
                execution_time_ms=0.0,
                error=error_msg
            )

    elif provider in ["webhook", "builtin", "script"]:
        # Other builtin tool types
        await context.bus.emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "error",
            "message": f"Tool provider '{provider}' implementation pending",
            "nodeId": node_id
        })

        return NodeOutput(
            status="failed",
            metadata=None,
            execution_time_ms=0.0,
            error=f"Tool provider '{provider}' not yet implemented"
        )

    else:
        error_msg = f"Unsupported tool provider: {provider}"
        await context.bus.emit({
            "type": "log",
            "runId": run_id,
            "at": iso_now(),
            "level": "error",
            "message": error_msg,
            "nodeId": node_id
        })

        return NodeOutput(
            status="failed",
            metadata=None,
            execution_time_ms=0.0,
            error=error_msg
        )
