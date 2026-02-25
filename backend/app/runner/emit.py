from typing import Any, Dict

from .metadata import GraphContext


async def emit(ctx: GraphContext, event: Dict[str, Any]) -> None:
    # Guardrail: catches incorrect context objects early.
    assert hasattr(ctx, "bus") and ctx.bus is not None, "GraphContext.bus missing"
    await ctx.bus.emit(event)
