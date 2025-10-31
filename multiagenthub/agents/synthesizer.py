from __future__ import annotations
import asyncio
import os
from typing import Any, Dict, List

from .base import AgentBase
from ..models import Task
from ..llm import LLMClient


class SynthesizerAgent(AgentBase):
    def __init__(self, agent_id: str, skills, hub) -> None:
        super().__init__(agent_id, skills, hub)
        self._artifacts: Dict[str, Any] = {}
    async def on_task(self, task: Task) -> Dict[str, Any]:
        analysis = task.payload.get("analysis", {})
        query = task.payload.get("query", "")
        await asyncio.sleep(0.02)
        # Provider-agnostic client with stub fallback
        client = LLMClient()
        prompt = f"Synthesize a brief report for: {query}. Analysis: {analysis}"
        content = client.generate(prompt, max_tokens=200)
        if not content:
            text = self._fallback_text(query, analysis)
            report_obj = {"query": query, "summary": text, "highlights": analysis.get("top", [])}
        else:
            report_obj = {"report": content}
        # persist artifact
        import uuid
        art_id = str(uuid.uuid4())
        self._artifacts[art_id] = report_obj
        return {"artifact_id": art_id, **report_obj}

    def _fallback_text(self, query: str, analysis: Dict[str, Any]) -> str:
        top = ", ".join([w for w, _ in analysis.get("top", [])])
        return f"Summary for {query}: key themes include {top}."
