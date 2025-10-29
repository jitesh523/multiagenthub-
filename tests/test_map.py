from __future__ import annotations
import asyncio
import uuid

import pytest

from multiagenthub.hub import Hub
from multiagenthub.orchestrator import Orchestrator
from multiagenthub.models import Task
from multiagenthub.agents.base import AgentBase


class MapAgent(AgentBase):
    async def on_task(self, task: Task):
        # echo back the item with simple transform
        item = task.payload.get("part") or task.payload.get("item")
        return {"out": item}


@pytest.mark.asyncio
async def test_map_task_aggregates_results():
    hub = Hub()
    a = MapAgent("a", ["map"], hub)
    task = asyncio.create_task(a.start())

    orch = Orchestrator(hub)
    t_map = Task(
        id=str(uuid.uuid4()),
        type="map",
        payload={
            "skill": "map",
            "base": {},
            "items": [1, 2, 3],
            "map_key": "part",
        },
    )
    orch.add_task(t_map)

    res = await asyncio.wait_for(orch.execute(), timeout=5)
    assert t_map.id in res
    agg = res[t_map.id]
    assert "map" in agg and len(agg["map"]) == 3
    outs = [r.get("out") for r in agg["map"]]
    assert outs == [1, 2, 3]

    task.cancel()
