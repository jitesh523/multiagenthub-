from __future__ import annotations
import asyncio
import uuid

import pytest

from multiagenthub.hub import Hub
from multiagenthub.orchestrator import Orchestrator
from multiagenthub.models import Task
from multiagenthub.agents.researcher import ResearcherAgent
from multiagenthub.agents.synthesizer import SynthesizerAgent


@pytest.mark.asyncio
async def test_researcher_uses_cache_for_same_query():
    hub = Hub()
    r = ResearcherAgent("researcher", ["research"], hub)
    t_run = asyncio.create_task(r.start())

    orch = Orchestrator(hub)
    q = "cache-test"
    t1 = Task(id=str(uuid.uuid4()), type="sequential", payload={"skill": "research", "query": q})
    t2 = Task(id=str(uuid.uuid4()), type="sequential", payload={"skill": "research", "query": q})
    orch.add_task(t1)
    orch.add_task(t2)

    res = await asyncio.wait_for(orch.execute(), timeout=5)
    f1 = res[t1.id]["findings"]
    f2 = res[t2.id]["findings"]
    assert f1 == f2

    t_run.cancel()


@pytest.mark.asyncio
async def test_synthesizer_emits_artifact_id():
    hub = Hub()
    s = SynthesizerAgent("synth", ["synthesize"], hub)
    t_run = asyncio.create_task(s.start())

    orch = Orchestrator(hub)
    t = Task(
        id=str(uuid.uuid4()),
        type="sequential",
        payload={"skill": "synthesize", "analysis": {"top": [("a",1),("b",2)]}, "query": "x"},
    )
    orch.add_task(t)

    res = await asyncio.wait_for(orch.execute(), timeout=5)
    out = res[t.id]
    assert "artifact_id" in out and isinstance(out["artifact_id"], str) and out["artifact_id"]

    t_run.cancel()
