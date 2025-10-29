from __future__ import annotations
import asyncio
import json
import uuid

from multiagenthub.hub import Hub
from multiagenthub.orchestrator import Orchestrator
from multiagenthub.models import Task
from multiagenthub.agents.researcher import ResearcherAgent
from multiagenthub.agents.analyzer import AnalyzerAgent
from multiagenthub.agents.synthesizer import SynthesizerAgent


async def main() -> None:
    hub = Hub()

    researcher = ResearcherAgent("researcher", ["research"], hub)
    analyzer = AnalyzerAgent("analyzer", ["analyze"], hub)
    synthesizer = SynthesizerAgent("synthesizer", ["synthesize"], hub)

    tasks = [
        asyncio.create_task(researcher.start()),
        asyncio.create_task(analyzer.start()),
        asyncio.create_task(synthesizer.start()),
    ]

    orch = Orchestrator(hub)

    t1 = Task(id=str(uuid.uuid4()), type="sequential", payload={"skill": "research", "query": "climate change impacts"})
    t2 = Task(id=str(uuid.uuid4()), type="sequential", deps=[t1.id], payload={"skill": "analyze"})
    t3 = Task(id=str(uuid.uuid4()), type="sequential", deps=[t2.id], payload={"skill": "synthesize"})
    # Conditional that will SKIP: requires prev['nonexistent'] > 0
    t_skip = Task(
        id=str(uuid.uuid4()),
        type="conditional",
        deps=[t2.id],
        payload={"skill": "analyze"},
        condition="prev.get('nonexistent', 0) > 0",
    )
    # Conditional that will RUN: checks that analysis produced 'tokens'
    t_run = Task(
        id=str(uuid.uuid4()),
        type="conditional",
        deps=[t2.id],
        payload={"skill": "synthesize"},
        condition="'tokens' in prev",
    )

    orch.add_task(t1)
    orch.add_task(t2)
    orch.add_task(t3)
    orch.add_task(t_skip)
    orch.add_task(t_run)

    # Execute the DAG; orchestrator will propagate outputs to dependents
    await asyncio.wait_for(orch.execute(), timeout=20)
    # Extract final report from t3 and show conditional outcomes
    final = orch.tasks[t3.id].result or {}
    print(json.dumps(final, indent=2))
    print("Conditional t_skip state:", orch.tasks[t_skip.id].state)
    print("Conditional t_run state:", orch.tasks[t_run.id].state)

    # Demonstrate a simple map task that echoes items via analyzer agent
    t_map = Task(
        id=str(uuid.uuid4()),
        type="map",
        payload={
            "skill": "analyze",
            "base": {},
            "items": ["alpha", "beta", "gamma"],
            "map_key": "item",
        },
    )
    orch.add_task(t_map)
    await asyncio.wait_for(orch.execute(), timeout=10)
    print("Map task aggregated:", orch.tasks[t_map.id].result)

    for t in tasks:
        t.cancel()


if __name__ == "__main__":
    asyncio.run(main())
