from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ..runner.metadata import GraphContext, NodeOutput


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def exec_transform(
    run_id: str,
    node: Dict[str, Any],
    context: GraphContext,
    input_metadata: Optional[Any],
    upstream_artifact_ids: Optional[list[str]] = None,
) -> NodeOutput:
    """Legacy transform executor entrypoint.

    Transform execution is owned by `app.runner.nodes.transform` via `app.runner.run`.
    This shim remains only to fail fast for stale imports.
    """
    raise RuntimeError(
        "Legacy transform executor is disabled. Use runner.nodes.transform via runner.run."
    )
