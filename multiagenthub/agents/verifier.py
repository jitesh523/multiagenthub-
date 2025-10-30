from __future__ import annotations
from typing import Dict, Any

from .base import AgentBase
from ..models import Task


class VerifierAgent(AgentBase):
    async def on_task(self, task: Task) -> Dict[str, Any]:
        """
        Minimal verifier: checks if an analysis contains at least a few tokens
        or a synth result contains a 'report'. Returns an ok flag and reason.
        """
        analysis = task.payload.get("analysis") or {}
        report = task.payload.get("report") or {}
        ok = False
        reason = ""
        if isinstance(analysis, dict) and analysis.get("tokens"):
            ok = len(analysis.get("tokens", [])) >= 3
            reason = "enough_tokens" if ok else "too_few_tokens"
        elif isinstance(report, dict) and report.get("report"):
            ok = len(str(report.get("report"))) > 20
            reason = "report_present" if ok else "report_too_short"
        else:
            reason = "no_known_fields"
        return {"ok": ok, "reason": reason}
