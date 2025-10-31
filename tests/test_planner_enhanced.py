from __future__ import annotations
import asyncio
import uuid

import pytest

from multiagenthub.hub import Hub
from multiagenthub.orchestrator import Orchestrator
from multiagenthub.models import Task
from multiagenthub.agents.planner import PlannerAgent


@pytest.mark.asyncio
async def test_planner_emits_map_and_conditional_for_list_goal():
    hub = Hub()
    planner = PlannerAgent("planner", ["plan"], hub)
    t = asyncio.create_task(planner.start())

    orch = Orchestrator(hub)
    goal = "compare apples, bananas, cherries"
    tp = Task(id=str(uuid.uuid4()), type="sequential", payload={"skill": "plan", "goal": goal})
    orch.add_task(tp)

    res = await asyncio.wait_for(orch.execute(), timeout=5)
    plan = res[tp.id]["plan"]
    steps = plan["steps"]
    ids = [s.get("id") for s in steps]

    assert "research_each" in ids
    assert any(s.get("type") == "map" for s in steps)
    assert any(s.get("id") == "analyze_retry" and s.get("type") == "conditional" for s in steps)
    # ensure items parsed
    m = next(s for s in steps if s.get("id") == "research_each")
    assert m.get("items") and len(m["items"]) >= 2

    t.cancel()
