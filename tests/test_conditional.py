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
        return task.payload


@pytest.mark.asyncio
async def test_conditional_skip_and_run():
    hub = Hub()
    a = EchoAgent("a", ["echo"], hub)
    task = asyncio.create_task(a.start())

    orch = Orchestrator(hub)

    t1 = Task(id=str(uuid.uuid4()), type="sequential", payload={"skill": "echo", "x": 1})
    # This should SKIP because prev['x'] == 1 -> condition False
    t2 = Task(
        id=str(uuid.uuid4()),
        type="conditional",
        deps=[t1.id],
        payload={"skill": "echo", "y": 2},
        condition="prev.get('x', 0) > 10",
    )
    # This should RUN because condition is True based on t1
    t3 = Task(
        id=str(uuid.uuid4()),
        type="conditional",
        deps=[t1.id],
        payload={"skill": "echo", "z": 3},
        condition="prev.get('x', 0) == 1",
    )

    for t in (t1, t2, t3):
        orch.add_task(t)

    await asyncio.wait_for(orch.execute(), timeout=5)

    assert orch.tasks[t2.id].state.name.lower() == "skipped"
    assert orch.tasks[t3.id].state.name.lower() == "completed"

    task.cancel()
