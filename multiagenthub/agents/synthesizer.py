from __future__ import annotations
import asyncio
import os
from typing import Any, Dict, List

from .base import AgentBase
from ..models import Task


class SynthesizerAgent(AgentBase):
    async def on_task(self, task: Task) -> Dict[str, Any]:
        analysis = task.payload.get("analysis", {})
        query = task.payload.get("query", "")
        await asyncio.sleep(0.02)
        api_key = os.getenv("OPENAI_API_KEY")
        if api_key:
            try:
                import openai
                client = openai.OpenAI(api_key=api_key)  # type: ignore[attr-defined]
                prompt = f"Summarize impacts for '{query}' given token frequencies: {list(analysis.get('top', []))[:5]}"
                resp = client.chat.completions.create(
                    model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2,
                    max_tokens=200,
                )
                text = resp.choices[0].message.content  # type: ignore[index]
            except Exception:
                text = self._fallback_text(query, analysis)
        else:
            text = self._fallback_text(query, analysis)
        return {"report": {"query": query, "summary": text, "highlights": analysis.get("top", [])}}

    def _fallback_text(self, query: str, analysis: Dict[str, Any]) -> str:
        top = ", ".join([w for w, _ in analysis.get("top", [])])
        return f"Summary for {query}: key themes include {top}."
