# executors/llm_openai_compat.py
from __future__ import annotations

from typing import Any, Dict, Optional
from datetime import datetime, timezone
import asyncio

# Keep imports minimal to avoid circular / path issues
from ..runner.events import RunEventBus
from ..runner.metadata import ExecutionContext
from ..runner.metadata import FileMetadata  # adjust if needed
from ..runner.metadata import NodeOutput      # adjust if needed


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def exec_llm_openai_compat(
    run_id: str,
    node: Dict[str, Any],
    context: ExecutionContext,
    # bus: RunEventBus,
    input_metadata: Optional[FileMetadata],
    params: Any,  # intentionally loose for now
    upstream_artifact_ids: Optional[list[str]] = None
) -> NodeOutput:
    """
    STUB: OpenAI-compatible LLM executor.

    This exists only to validate wiring + dispatch.
    Replace with real implementation later.
    """
    node_id = node.get("id", "<missing-node-id>")

    upstream_artifact_ids = upstream_artifact_ids or []
    assert context is not None, "context is None"
    assert hasattr(context, "bus"), "context missing bus"

    node_id = node.get("id")
    t0 = asyncio.get_event_loop().time()

    await context.bus.emit(
        {
            "type": "log",
            "runId": run_id,
            "nodeId": node_id,
            "at": iso_now(),
            "level": "info",
            "message": f"[STUB] OpenAI-compatible LLM executor invoked (model={getattr(params, 'model', None)})",
        }
    )

    # Fake output so downstream nodes don't explode
    fake_output = {
        "text": "OpenAI-compatible LLM stub response (no API call made)"
    }

    return NodeOutput(
        status="succeeeded",
        metadata=None,
        execution_time_ms=(asyncio.get_event_loop().time() - t0) * 1000.0,
        value=fake_output,  # change to `output=` or `data=` if your NodeOutput uses a different field
        error=None,
    )
