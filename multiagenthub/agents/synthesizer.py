from __future__ import annotations
import asyncio
import os
from typing import Any, Dict, List

from .base import AgentBase
from ..models import Task
from ..llm import LLMClient


class SynthesizerAgent(AgentBase):
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
            return {"report": {"query": query, "summary": text, "highlights": analysis.get("top", [])}}
        return {"report": content}

    def _fallback_text(self, query: str, analysis: Dict[str, Any]) -> str:
        top = ", ".join([w for w, _ in analysis.get("top", [])])
        return f"Summary for {query}: key themes include {top}."
