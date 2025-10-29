from __future__ import annotations
import asyncio
import logging
import time
from typing import Any, Dict, Optional

import os
from .bus import InMemoryBus
from .redis_bus import RedisBus  # optional backend
from .models import Message
from .security import CryptoContext

logger = logging.getLogger(__name__)


class Hub:
    def __init__(self, crypto: Optional[CryptoContext] = None):
        self.crypto = crypto or CryptoContext()
        backend = os.getenv("BUS", "memory").lower()
        if backend == "redis":
            url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
            try:
                self.bus = RedisBus(self.crypto, url=url)
                logger.info("hub_bus_backend backend=redis url=%s", url)
            except Exception as e:
                logger.warning("hub_bus_fallback backend=memory reason=%s", e)
                self.bus = InMemoryBus(self.crypto)
        else:
            self.bus = InMemoryBus(self.crypto)
            logger.info("hub_bus_backend backend=memory")
        self.registry: Dict[str, Dict[str, Any]] = {}
        self.metrics: Dict[str, float] = {
            "messages_total": 0.0,
            "errors_total": 0.0,
            "task_complete_total": 0.0,
        }
        self.timings: Dict[str, float] = {}

    def register_agent(self, agent_id: str, skills: Dict[str, Any]) -> None:
        self.registry[agent_id] = skills
        logger.info("agent_registered id=%s skills=%s", agent_id, skills.get("skills", []))

    def get_agent_by_skill(self, skill: str) -> Optional[str]:
        for aid, s in self.registry.items():
            if skill in s.get("skills", []):
                return aid
        return None

    async def send(self, msg: Message) -> None:
        self.metrics["messages_total"] += 1
        self.timings[msg.id] = time.time()
        if msg.recipient == "broadcast":
            await self.bus.publish("topic:all", msg.dict())
        else:
            await self.bus.send(msg.recipient, msg.dict())
        logger.debug(
            "message_sent id=%s type=%s sender=%s recipient=%s", msg.id, msg.type, msg.sender, msg.recipient
        )

    async def recv(self, agent_id: str, timeout: Optional[float] = None):
        raw = await self.bus.receive_one(agent_id, timeout)
        if not raw:
            return None
        msg = Message(**raw)
        logger.debug("message_received id=%s recipient=%s sender=%s", msg.id, agent_id, msg.sender)
        return msg

    def record_completion(self, msg_id: str, success: bool) -> None:
        if success:
            self.metrics["task_complete_total"] += 1
        else:
            self.metrics["errors_total"] += 1
        if msg_id in self.timings:
            latency = time.time() - self.timings.pop(msg_id)
            logger.debug("message_round_trip id=%s latency_s=%.3f", msg_id, latency)
