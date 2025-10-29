from __future__ import annotations
import asyncio
import json
import logging
from typing import Any, Dict, Optional

try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover - optional dep
    aioredis = None

from .security import CryptoContext

logger = logging.getLogger(__name__)


class RedisBus:
    """
    Redis-backed transport implementing simple P2P mailboxes via lists and
    topic broadcast via pub/sub channels. Messages are encrypted and signed
    at the bus boundary using the provided CryptoContext.
    Keys:
      - mailbox list: mb:{recipient}
      - topic channel: {topic}
    """

    def __init__(self, crypto: CryptoContext, url: str = "redis://localhost:6379/0") -> None:
        if aioredis is None:
            raise RuntimeError("redis package not available; install 'redis' to use RedisBus")
        self.crypto = crypto
        self._redis = aioredis.from_url(url, decode_responses=True)

    async def publish(self, topic: str, message: Dict[str, Any]) -> None:
        ct, sig = self.crypto.encrypt_and_sign(message)
        payload = json.dumps({"ct": ct, "sig": sig}, separators=(",", ":"))
        await self._redis.publish(topic, payload)

    async def send(self, recipient: str, message: Dict[str, Any]) -> None:
        ct, sig = self.crypto.encrypt_and_sign(message)
        payload = json.dumps({"ct": ct, "sig": sig}, separators=(",", ":"))
        key = f"mb:{recipient}"
        await self._redis.rpush(key, payload)

    async def receive_one(self, recipient: str, timeout: Optional[float] = None) -> Optional[Dict[str, Any]]:
        key = f"mb:{recipient}"
        # BLPOP timeout is seconds (integer). Use 0 for block forever; None -> 0
        to = 0 if timeout is None else max(1, int(timeout))
        res = await self._redis.blpop(key, timeout=to)
        if not res:
            return None
        _k, payload = res
        try:
            data = json.loads(payload)
            return self.crypto.decrypt_and_verify(data["ct"], data["sig"])  # type: ignore[index]
        except Exception as e:  # corrupt message
            logger.exception("redis_bus_receive_error recipient=%s error=%s", recipient, e)
            return None
