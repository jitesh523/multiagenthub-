from __future__ import annotations
import asyncio
import uuid

import pytest

from multiagenthub.hub import Hub
from multiagenthub.orchestrator import Orchestrator
from multiagenthub.models import Task
from multiagenthub.agents.base import AgentBase


class EchoAgent(AgentBase):
    async def on_task(self, task: Task):
        return {"echo": task.payload}


@pytest.mark.asyncio
async def test_orchestrator_simple_flow():
    hub = Hub()
    a = EchoAgent("a", ["echo"], hub)
    task = asyncio.create_task(a.start())

    orch = Orchestrator(hub)
    t1 = Task(id=str(uuid.uuid4()), type="sequential", payload={"skill": "echo", "x": 1})
    orch.add_task(t1)

    res = await asyncio.wait_for(orch.execute(), timeout=5)
    assert t1.id in res and "echo" in res[t1.id]
    task.cancel()
