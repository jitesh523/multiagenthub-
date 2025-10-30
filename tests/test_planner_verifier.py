from __future__ import annotations
import asyncio
import uuid

import pytest

from multiagenthub.hub import Hub
from multiagenthub.orchestrator import Orchestrator
from multiagenthub.models import Task
from multiagenthub.agents.planner import PlannerAgent
from multiagenthub.agents.verifier import VerifierAgent


@pytest.mark.asyncio
async def test_planner_returns_three_step_plan():
    hub = Hub()
    planner = PlannerAgent("planner", ["plan"], hub)
    t = asyncio.create_task(planner.start())

    orch = Orchestrator(hub)
    tp = Task(id=str(uuid.uuid4()), type="sequential", payload={"skill": "plan", "goal": "test goal"})
    orch.add_task(tp)

    res = await asyncio.wait_for(orch.execute(), timeout=5)
    plan = res[tp.id]["plan"]
    assert isinstance(plan, dict)
    assert [s["id"] for s in plan["steps"]] == ["research", "analyze", "synthesize"]

    t.cancel()


@pytest.mark.asyncio
async def test_verifier_ok_on_enough_tokens():
    hub = Hub()
    verifier = VerifierAgent("verifier", ["verify"], hub)
    t = asyncio.create_task(verifier.start())

    orch = Orchestrator(hub)
    tv = Task(
        id=str(uuid.uuid4()),
        type="sequential",
        payload={"skill": "verify", "analysis": {"tokens": [1, 2, 3, 4]}},
    )
    orch.add_task(tv)

    res = await asyncio.wait_for(orch.execute(), timeout=5)
    out = res[tv.id]
    assert out.get("ok") is True

    t.cancel()
