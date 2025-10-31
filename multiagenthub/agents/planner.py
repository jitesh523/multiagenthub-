from __future__ import annotations
from typing import Dict, Any, List

from .base import AgentBase
from ..models import Task


class PlannerAgent(AgentBase):
    async def on_task(self, task: Task) -> Dict[str, Any]:
        # Enhanced planner: detect list/comparison hints and propose map + conditional retry
        goal = task.payload.get("goal") or task.payload.get("query") or ""
        text = goal.lower()
        items: List[str] = []
        if "," in goal or "compare" in text:
            # naive split by comma for sources/items
            parts = [p.strip() for p in goal.split(",")]
            # if first chunk contains 'compare', drop it from items list
            if parts:
                items = [p for p in parts if p and "compare" not in p.lower()]

        steps: List[Dict[str, Any]] = []
        if items:
            steps.append({
                "id": "research_each",
                "type": "map",
                "skill": "research",
                "deps": [],
                "map_key": "query",
                "items": items,
            })
            steps.append({"id": "analyze", "skill": "analyze", "deps": ["research_each"]})
            # conditional retry analyze if verifier not ok
            steps.append({"id": "verify", "skill": "verify", "deps": ["analyze"]})
            steps.append({
                "id": "analyze_retry",
                "type": "conditional",
                "skill": "analyze",
                "deps": ["verify"],
                "condition": "prev.get('ok') is False",
            })
            steps.append({"id": "synthesize", "skill": "synthesize", "deps": ["analyze", "analyze_retry"]})
        else:
            steps = [
                {"id": "research", "skill": "research", "deps": []},
                {"id": "analyze", "skill": "analyze", "deps": ["research"]},
                {"id": "synthesize", "skill": "synthesize", "deps": ["analyze"]},
            ]
        return {"plan": {"goal": goal, "steps": steps}}
