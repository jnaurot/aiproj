from typing import Any, Dict
from .metadata import ExecutionContext

async def emit(ctx: ExecutionContext, event: Dict[str, Any]) -> None:
    # Guardrail: catches “ctx is None” or wrong object passed
    assert hasattr(ctx, "bus") and ctx.bus is not None, "ExecutionContext.bus missing"
    await ctx.bus.emit(event)
