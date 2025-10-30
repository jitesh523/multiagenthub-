from __future__ import annotations
import os
from typing import Optional

class LLMClient:
    """
    Minimal provider-agnostic LLM client.
    Supports: 'openai' via OPENAI_API_KEY and OPENAI_MODEL; default is 'stub'.
    """

    def __init__(self, provider: Optional[str] = None) -> None:
        self.provider = (provider or os.getenv("LLM_PROVIDER", "stub")).lower()
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.api_key = os.getenv("OPENAI_API_KEY")
        self._openai = None
        if self.provider == "openai" and self.api_key:
            try:
                from openai import OpenAI  # type: ignore
                self._openai = OpenAI(api_key=self.api_key)
            except Exception:
                self.provider = "stub"

    def generate(self, prompt: str, max_tokens: int = 256) -> str:
        if self.provider == "openai" and self._openai is not None:
            try:
                resp = self._openai.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=max_tokens,
                    temperature=0.3,
                )
                return (resp.choices[0].message.content or "").strip()
            except Exception:
                # fallback to stub
                pass
        # Stub/deterministic fallback
        return f"[STUB REPORT]\n{prompt[:200]}\n..."
