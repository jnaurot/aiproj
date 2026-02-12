import asyncio
from typing import Any, Callable, Dict, Optional

class RunEventBus:
    def __init__(self, run_id: str, on_emit: Optional[Callable[[Dict[str, Any]], None]] = None):
        self.run_id = run_id
        self._seq = 0
        self.q = asyncio.Queue()
        self._on_emit = on_emit

    async def emit(self, evt: dict):
        if self._on_emit:
            try:
                self._on_emit(evt)   # sync hook
            except Exception as e:
                # NEVER let state projection kill the runtime
                print("[RunEventBus] on_emit error:", e, "evt=", evt)

        self._seq += 1
        evt["seq"] = self._seq
        await self.q.put(evt)


    async def next_event(self) -> Dict[str, Any]:
        return await self.q.get()