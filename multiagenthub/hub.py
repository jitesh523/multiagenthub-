from __future__ import annotations
import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, Optional, List

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
        self.metrics: Dict[str, Any] = {
            "messages_total": 0.0,
            "errors_total": 0.0,
            "task_complete_total": 0.0,
            "inflight": 0.0,
            "tasks_total_by_skill": {},  # skill -> count
        }
        self.timings: Dict[str, float] = {}
        # Artifact store: in-memory index; optional fs persistence
        self._artifacts: Dict[str, Dict[str, Any]] = {}
        self._artifact_backend = os.getenv("ARTIFACTS", "memory").lower()
        self._artifact_dir = os.getenv("ARTIFACTS_DIR", "artifacts")
        if self._artifact_backend == "fs":
            os.makedirs(self._artifact_dir, exist_ok=True)

    def register_agent(self, agent_id: str, skills: Dict[str, Any]) -> None:
        self.registry[agent_id] = skills
        logger.info("agent_registered id=%s skills=%s", agent_id, skills.get("skills", []))

    # Artifact store APIs
    def save_artifact(self, data: Any, *, content_type: str = "application/json", trace_id: Optional[str] = None) -> str:
        import uuid
        art_id = str(uuid.uuid4())
        created_at = time.time()
        size = 0
        if self._artifact_backend == "fs":
            path = os.path.join(self._artifact_dir, f"{art_id}.json")
            try:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False)
                size = os.path.getsize(path)
            except Exception as e:
                logger.exception("artifact_fs_write_error id=%s error=%s", art_id, e)
        else:
            try:
                payload = json.dumps(data)
                size = len(payload.encode("utf-8"))
            except Exception:
                pass
            self._artifacts[art_id] = {"data": data}
        meta = {
            "id": art_id,
            "content_type": content_type,
            "trace_id": trace_id,
            "backend": self._artifact_backend,
            "created_at": created_at,
            "size": size,
        }
        self._artifacts[art_id] = {**self._artifacts.get(art_id, {}), **meta}
        return art_id

    def get_artifact(self, art_id: str) -> Optional[Dict[str, Any]]:
        meta = self._artifacts.get(art_id)
        if not meta:
            return None
        if meta.get("backend") == "fs":
            path = os.path.join(self._artifact_dir, f"{art_id}.json")
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return {**meta, "data": data}
            except Exception as e:
                logger.exception("artifact_fs_read_error id=%s error=%s", art_id, e)
                return None
        return meta

    def list_artifacts(self, *, trace_id: Optional[str] = None) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for v in self._artifacts.values():
            if trace_id and v.get("trace_id") != trace_id:
                continue
            items.append({k: v[k] for k in ("id", "content_type", "trace_id", "backend", "created_at", "size") if k in v})
        items.sort(key=lambda x: x.get("created_at", 0), reverse=True)
        return items

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
