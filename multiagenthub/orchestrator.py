from __future__ import annotations
import asyncio
import logging
import uuid
from typing import Any, Dict, List, Optional

import networkx as nx

from .hub import Hub
from .models import Message, MessageType, Task, TaskState

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Executes a DAG of tasks with retries, timeouts, and dynamic assignment.
    """
    def __init__(self, hub: Hub):
        self.hub = hub
        self.graph = nx.DiGraph()
        self.tasks: Dict[str, Task] = {}
        self.heartbeats: Dict[str, float] = {}
        self.heartbeat_timeout_s: float = 5.0

    def add_task(self, task: Task) -> None:
        self.tasks[task.id] = task
        self.graph.add_node(task.id)
        for dep in task.deps:
            self.graph.add_edge(dep, task.id)

    def ready_tasks(self) -> List[Task]:
        ready: List[Task] = []
        for node in self.graph.nodes:
            t = self.tasks[node]
            if t.state == TaskState.pending:
                preds = list(self.graph.predecessors(node))
                if all(self.tasks[p].state in (TaskState.completed, TaskState.skipped) for p in preds):
                    ready.append(t)
        return ready

    async def execute(self) -> Dict[str, Any]:
        while not all(t.state in (TaskState.completed, TaskState.failed) for t in self.tasks.values()):
            await self._drain_heartbeats()
            self._retry_stale_running_tasks()
            batch = self.ready_tasks()
            if not batch:
                await asyncio.sleep(0.05)
                continue
            await asyncio.gather(*(self._run_task(t) for t in batch))
        return {tid: (t.result if t.result else {"error": t.error}) for tid, t in self.tasks.items()}

    async def _run_task(self, task: Task) -> None:
        if task.state != TaskState.pending:
            return
        # Evaluate condition, if present, using merged predecessor results
        if task.condition:
            prev = self._merge_predecessor_results(task.id)
            allow = False
            try:
                # Safe eval context with only 'prev' available
                allow = bool(eval(task.condition, {"__builtins__": {}}, {"prev": prev}))
            except Exception as e:
                logger.error("task_condition_error id=%s error=%s", task.id, e)
                allow = False
            if not allow:
                task.state = TaskState.skipped
                task.result = {"skipped": True, "reason": "condition_false"}
                logger.info("task_skipped id=%s condition=%s", task.id, task.condition)
                return
        task.state = TaskState.running
        task.attempts += 1
        logger.info(
            "task_start id=%s attempts=%d type=%s deps=%s", task.id, task.attempts, task.type, task.deps
        )
        required_skill = task.payload.get("skill")
        agent_id: Optional[str] = self.hub.get_agent_by_skill(required_skill) if required_skill else None
        if not agent_id:
            task.state, task.error = TaskState.failed, f"No agent for skill '{required_skill}'"
            logger.error("task_assignment_failed id=%s reason=no_agent skill=%s", task.id, required_skill)
            return
        task.owner = agent_id
        logger.debug("task_assigned id=%s owner=%s skill=%s", task.id, agent_id, required_skill)
        # Lightweight map support: fan-out items to the same agent and aggregate
        if task.type == "map":
            items = task.payload.get("items", [])
            base_payload = task.payload.get("base", {})
            map_key = task.payload.get("map_key", "item")
            coros = []
            msg_ids: List[str] = []
            for idx, it in enumerate(items):
                child_task = Task(
                    id=f"{task.id}:{idx}",
                    type="sequential",
                    payload={**base_payload, "skill": required_skill, map_key: it},
                    timeout_s=task.timeout_s,
                )
                m_id = str(uuid.uuid4())
                msg_ids.append(m_id)
                m = Message(
                    id=m_id,
                    sender="orchestrator",
                    recipient=agent_id,
                    type=MessageType.request,
                    method="run_task",
                    params={"task": child_task.dict()},
                )
                coros.append(self.hub.send(m))
            # Send all requests
            await asyncio.gather(*coros)
            # Collect responses
            results: List[Dict[str, Any]] = []
            pending_ids = set(msg_ids)
            end_time = asyncio.get_event_loop().time() + task.timeout_s
            while pending_ids and asyncio.get_event_loop().time() < end_time:
                resp = await self.hub.recv("orchestrator", timeout=0.1)
                if not resp:
                    continue
                if resp.id in pending_ids:
                    if resp.error:
                        results.append({"error": resp.error})
                    else:
                        results.append(resp.result or {})
                    pending_ids.remove(resp.id)
            if pending_ids:
                logger.warning("map_task_partial_timeout id=%s missing=%d", task.id, len(pending_ids))
            task.state = TaskState.completed
            task.result = {"map": results}
        else:
            msg = Message(
                id=str(uuid.uuid4()),
                sender="orchestrator",
                recipient=agent_id,
                type=MessageType.request,
                method="run_task",
                params={"task": task.dict()},
            )
            await self.hub.send(msg)
            try:
                resp = await asyncio.wait_for(self.hub.recv("orchestrator", timeout=task.timeout_s), timeout=task.timeout_s)
            except asyncio.TimeoutError:
                resp = None
            if not resp or resp.error:
                if task.attempts <= task.max_retries:
                    task.state = TaskState.pending
                    logger.warning(
                        "task_retry id=%s attempt=%d max=%d reason=%s",
                        task.id,
                        task.attempts,
                        task.max_retries,
                        (resp.error if resp else {"message": "timeout"}),
                    )
                    await asyncio.sleep(0)
                    return
                task.state = TaskState.failed
                task.error = (resp.error if resp else {"message": "timeout"}).__str__()
                logger.error("task_failed id=%s error=%s", task.id, task.error)
                return
            task.state = TaskState.completed
            task.result = resp.result or {}
        logger.info("task_completed id=%s owner=%s", task.id, task.owner)
        # Propagate outputs to dependent tasks (supports fan-in by merging predecessors)
        for succ in self.graph.successors(task.id):
            s = self.tasks[succ]
            if not isinstance(s.payload, dict):
                continue
            merged_prev = self._merge_predecessor_results(succ)
            # If successor expects analysis and merged has tokens/top, wire it
            if s.payload.get("skill") == "analyze" and "findings" not in s.payload:
                findings = merged_prev.get("findings")
                if findings:
                    s.payload["findings"] = findings
            if s.payload.get("skill") == "synthesize" and "analysis" not in s.payload:
                if isinstance(merged_prev, dict) and ("tokens" in merged_prev or "top" in merged_prev):
                    s.payload["analysis"] = merged_prev
            if s.payload.get("skill") == "synthesize" and "query" not in s.payload:
                q = task.payload.get("query") or merged_prev.get("query") if isinstance(merged_prev, dict) else None
                if q:
                    s.payload["query"] = q
            # If successor is a map task and has no items, derive from merged prev
            if s.type == "map" and "items" not in s.payload:
                items = merged_prev.get("items") or merged_prev.get("findings")
                if items:
                    s.payload["items"] = items

    def _merge_predecessor_results(self, task_id: str) -> Dict[str, Any]:
        merged: Dict[str, Any] = {}
        for pred in self.graph.predecessors(task_id):
            t = self.tasks[pred]
            if isinstance(t.result, dict):
                for k, v in t.result.items():
                    # last-writer wins
                    merged[k] = v
        return merged

    async def _drain_heartbeats(self) -> None:
        while True:
            msg = await self.hub.recv("orchestrator:hb", timeout=0)
            if not msg:
                break
            if msg.type == MessageType.event and msg.method == "heartbeat":
                agent_id = (msg.params or {}).get("agent_id")
                if agent_id:
                    self.heartbeats[agent_id] = asyncio.get_event_loop().time()
                    logger.debug("heartbeat agent=%s", agent_id)

    def _retry_stale_running_tasks(self) -> None:
        now = asyncio.get_event_loop().time()
        for t in self.tasks.values():
            if t.state == TaskState.running and t.owner:
                last = self.heartbeats.get(t.owner, 0.0)
                if now - last > self.heartbeat_timeout_s and t.attempts < t.max_retries:
                    logger.warning(
                        "task_owner_stale_retry id=%s owner=%s last_hb=%.2fs ago",
                        t.id,
                        t.owner,
                        now - last,
                    )
                    t.state = TaskState.pending
