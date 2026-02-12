import asyncio
from typing import Any, Dict

class RunEventBus:
    def __init__(self, run_id: str):
        self.run_id = run_id
        self._seq = 0
        self.q: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()

    async def emit(self, evt: Dict[str, Any]):
        self._seq += 1
        evt["seq"] = self._seq
        await self.q.put(evt)

    async def next_event(self) -> Dict[str, Any]:
        return await self.q.get()