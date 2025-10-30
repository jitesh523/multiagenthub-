from __future__ import annotations
from typing import Dict, Any

from .base import AgentBase
from ..models import Task


class PlannerAgent(AgentBase):
    async def on_task(self, task: Task) -> Dict[str, Any]:
        # Very simple planner: given a goal/query, propose a 3-step plan
        goal = task.payload.get("goal") or task.payload.get("query") or ""
        plan = {
            "goal": goal,
            "steps": [
                {"id": "research", "skill": "research", "deps": []},
                {"id": "analyze", "skill": "analyze", "deps": ["research"]},
                {"id": "synthesize", "skill": "synthesize", "deps": ["analyze"]},
            ],
        }
        return {"plan": plan}
