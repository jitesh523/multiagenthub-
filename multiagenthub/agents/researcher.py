from __future__ import annotations
import asyncio
from typing import Any, Dict

from .base import AgentBase
from ..models import Task


class ResearcherAgent(AgentBase):
    def __init__(self, agent_id: str, skills, hub) -> None:
        super().__init__(agent_id, skills, hub)
        self._cache: Dict[str, Dict[str, Any]] = {}

    async def on_task(self, task: Task) -> Dict[str, Any]:
        query = task.payload.get("query", "")
        if query in self._cache:
            await asyncio.sleep(0)
            return self._cache[query]
        await asyncio.sleep(0.05)
        findings = [
            f"Key climate impact on oceans related to {query}",
            f"Economic risks from extreme weather re: {query}",
            f"Health outcomes linked to heatwaves and {query}",
        ]
        out = {"findings": findings}
        self._cache[query] = out
        return out
