from __future__ import annotations
import asyncio
from collections import Counter
from typing import Any, Dict, List

from .base import AgentBase
from ..models import Task


class AnalyzerAgent(AgentBase):
    async def on_task(self, task: Task) -> Dict[str, Any]:
        data: List[str] = task.payload.get("findings", [])
        await asyncio.sleep(0.02)
        tokens = []
        for s in data:
            tokens.extend([t.strip(',.').lower() for t in s.split()])
        counts = Counter(tokens)
        top = counts.most_common(5)
        return {"tokens": dict(counts), "top": top}
