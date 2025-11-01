from __future__ import annotations
import asyncio
import logging
from typing import Any, Dict, List

from ..hub import Hub
from ..models import Message, MessageType, Task

logger = logging.getLogger(__name__)


class AgentBase:
    def __init__(self, agent_id: str, skills: List[str], hub: Hub):
        self.id = agent_id
        self.skills = skills
        self.hub = hub
        self._task = None

    async def start(self) -> None:
        # Local registry for in-process hub users
        self.hub.register_agent(self.id, {"skills": self.skills})
        # Cross-process registration for remote orchestrators
        try:
            reg = Message(
                id="reg",
                sender=self.id,
                recipient="orchestrator:reg",
                type=MessageType.event,
                method="register",
                params={"agent_id": self.id, "skills": self.skills},
            )
            await self.hub.send(reg)
        except Exception:
            pass
        last_hb = 0.0
        while True:
            msg = await self.hub.recv(self.id, timeout=1.0)
            if not msg:
                # idle heartbeat every ~1s
                now = asyncio.get_event_loop().time()
                if now - last_hb > 1.0:
                    await self._send_heartbeat()
                    last_hb = now
                await asyncio.sleep(0.01)
                continue
            if msg.type == MessageType.request and msg.method == "run_task":
                await self._handle_run_task(msg)

    async def _handle_run_task(self, msg: Message) -> None:
        task = Task(**msg.params["task"])  # type: ignore[index]
        try:
            result = await self.on_task(task)
            resp = Message(
                id=msg.id,
                sender=self.id,
                recipient="orchestrator",
                type=MessageType.response,
                result=result,
            )
        except Exception as e:
            resp = Message(
                id=msg.id,
                sender=self.id,
                recipient="orchestrator",
                type=MessageType.response,
                error={"message": str(e)},
            )
        await self.hub.send(resp)

    async def on_task(self, task: Task) -> Dict[str, Any]:
        raise NotImplementedError

    async def _send_heartbeat(self) -> None:
        hb = Message(
            id="hb",
            sender=self.id,
            recipient="orchestrator:hb",
            type=MessageType.event,
            method="heartbeat",
            params={"agent_id": self.id},
        )
        await self.hub.send(hb)
