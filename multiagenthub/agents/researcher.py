from __future__ import annotations
import asyncio
from typing import Any, Dict

from .base import AgentBase
from ..models import Task


class ResearcherAgent(AgentBase):
    async def on_task(self, task: Task) -> Dict[str, Any]:
        query = task.payload.get("query", "")
        await asyncio.sleep(0.05)
        findings = [
            f"Key climate impact on oceans related to {query}",
            f"Economic risks from extreme weather re: {query}",
            f"Health outcomes linked to heatwaves and {query}",
        ]
        return {"findings": findings}
