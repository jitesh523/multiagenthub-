from __future__ import annotations
import asyncio
import logging
import uuid
from typing import Any, Dict, List, Optional

import networkx as nx

from .hub import Hub
from .persistence import BasePersistence, InMemoryPersistence
from .events import EventServer
from .models import Message, MessageType, Task, TaskState

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Executes a DAG of tasks with retries, timeouts, and dynamic assignment.
    """
    def __init__(
        self,
        hub: Hub,
        persistence: BasePersistence | None = None,
        event_server: EventServer | None = None,
        max_concurrent: int | None = None,
        default_timeout_s: float = 30.0,
        default_max_retries: int = 1,
    ):
        self.hub = hub
        self.graph = nx.DiGraph()
        self.tasks: Dict[str, Task] = {}
        self.heartbeats: Dict[str, float] = {}
        self.heartbeat_timeout_s: float = 5.0
        self.persistence: BasePersistence = persistence or InMemoryPersistence()
        self.event_server: EventServer | None = event_server
        self.max_concurrent = max_concurrent
        self.default_timeout_s = default_timeout_s
        self.default_max_retries = default_max_retries
        # metrics: per task.type histogram buckets and totals
        self._task_start_times: Dict[str, float] = {}
        self._hist_buckets = [0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        self._hist: Dict[str, Dict[str, float]] = {}

    def add_task(self, task: Task) -> None:
        self.tasks[task.id] = task
        # apply defaults if caller used model defaults
        if task.timeout_s == 30.0:
            task.timeout_s = self.default_timeout_s
        if task.max_retries == 1:
            task.max_retries = self.default_max_retries
        self.graph.add_node(task.id)
        for dep in task.deps:
            self.graph.add_edge(dep, task.id)
        # persist structure
        asyncio.create_task(self._persist())

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
        running: set[asyncio.Task] = set()
        while not all(t.state in (TaskState.completed, TaskState.failed) for t in self.tasks.values()) or running:
            await self._drain_heartbeats()
            self._retry_stale_running_tasks()
            batch = self.ready_tasks()
            # schedule up to available slots
            slots = None if self.max_concurrent is None else max(0, self.max_concurrent - len(running))
            to_schedule = batch if slots is None else batch[:slots]
            for t in to_schedule:
                running.add(asyncio.create_task(self._run_task(t)))
            if running:
                done, pending = await asyncio.wait(running, timeout=0.05, return_when=asyncio.FIRST_COMPLETED)
                running = set(pending)
                if done:
                    await self._persist()
                continue
            if not to_schedule:
                await asyncio.sleep(0.05)
        return {tid: (t.result if t.result else {"error": t.error}) for tid, t in self.tasks.items()}

    async def _run_task(self, task: Task) -> None:
        if task.state != TaskState.pending:
            await self._persist()
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
                await self._emit({"event": "task_skipped", "task_id": task.id})
                await self._persist()
                return
        task.state = TaskState.running
        task.attempts += 1
        logger.info(
            "task_start id=%s attempts=%d type=%s deps=%s", task.id, task.attempts, task.type, task.deps
        )
        await self._emit({"event": "task_start", "task_id": task.id, "attempt": task.attempts, "type": task.type, "trace_id": task.trace_id})
        # record start time
        self._task_start_times[task.id] = asyncio.get_event_loop().time()
        required_skill = task.payload.get("skill")
        agent_id: Optional[str] = self.hub.get_agent_by_skill(required_skill) if required_skill else None
        if not agent_id:
            task.state, task.error = TaskState.failed, f"No agent for skill '{required_skill}'"
            logger.error("task_assignment_failed id=%s reason=no_agent skill=%s", task.id, required_skill)
            await self._emit({"event": "task_failed", "task_id": task.id, "error": "no_agent", "skill": required_skill})
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
                    trace_id=task.trace_id,
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
                    trace_id=task.trace_id,
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
                trace_id=task.trace_id,
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
                    await self._emit({"event": "task_retry", "task_id": task.id, "trace_id": task.trace_id})
                    await asyncio.sleep(0)
                    return
                task.state = TaskState.failed
                task.error = (resp.error if resp else {"message": "timeout"}).__str__()
                logger.error("task_failed id=%s error=%s", task.id, task.error)
                await self._emit({"event": "task_failed", "task_id": task.id, "error": task.error, "trace_id": task.trace_id})
                return
            task.state = TaskState.completed
            task.result = resp.result or {}
        logger.info("task_completed id=%s owner=%s", task.id, task.owner)
        await self._emit({"event": "task_completed", "task_id": task.id, "trace_id": task.trace_id})
        await self._persist()
        # record duration into histogram
        self._observe_duration(task)
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

    async def _persist(self) -> None:
        data = {
            "tasks": {tid: t.dict() for tid, t in self.tasks.items()},
            "edges": [(u, v) for u, v in self.graph.edges()],
        }
        await self.persistence.save("orchestrator_state", data)

    async def load(self) -> None:
        data = await self.persistence.load("orchestrator_state")
        if not data:
            return
        self.tasks = {tid: Task(**td) for tid, td in data.get("tasks", {}).items()}
        self.graph = nx.DiGraph()
        self.graph.add_nodes_from(self.tasks.keys())
        for u, v in data.get("edges", []):
            self.graph.add_edge(u, v)

    async def _emit(self, payload: Dict[str, Any]) -> None:
        if self.event_server is not None:
            try:
                await self.event_server.broadcast(payload)
            except Exception:  # pragma: no cover - event errors shouldn't break execution
                logger.debug("event_emit_failed payload=%s", payload)

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

    def _observe_duration(self, task: Task) -> None:
        t0 = self._task_start_times.pop(task.id, None)
        if t0 is None:
            return
        dur = asyncio.get_event_loop().time() - t0
        ttype = task.type or "unknown"
        if ttype not in self._hist:
            self._hist[ttype] = {f"le_{b}": 0.0 for b in self._hist_buckets}
            self._hist[ttype]["count"] = 0.0
            self._hist[ttype]["sum"] = 0.0
        h = self._hist[ttype]
        for b in self._hist_buckets:
            if dur <= b:
                h[f"le_{b}"] += 1.0
        h["count"] += 1.0
        h["sum"] += dur

    def export_histogram(self) -> Dict[str, Dict[str, float]]:
        return self._hist

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
