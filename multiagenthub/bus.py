from __future__ import annotations
import asyncio
import logging
from typing import AsyncIterator, Dict, Optional, Set, Tuple

from .security import CryptoContext

logger = logging.getLogger(__name__)


class InMemoryBus:
    def __init__(self, crypto: CryptoContext):
        self.crypto = crypto
        self.topics: Dict[str, Set[asyncio.Queue[Tuple[str, str]]]] = {}
        self.mailboxes: Dict[str, asyncio.Queue[Tuple[str, str]]] = {}
        self._lock = asyncio.Lock()

    async def subscribe(self, topic_or_agent: str) -> AsyncIterator[Tuple[str, str]]:
        q: asyncio.Queue[Tuple[str, str]]
        async with self._lock:
            if topic_or_agent.startswith("topic:"):
                q = asyncio.Queue()
                self.topics.setdefault(topic_or_agent, set()).add(q)
            else:
                q = self.mailboxes.setdefault(topic_or_agent, asyncio.Queue())
        try:
            while True:
                yield await q.get()
        finally:
            if topic_or_agent.startswith("topic:"):
                async with self._lock:
                    self.topics.get(topic_or_agent, set()).discard(q)

    async def publish(self, topic: str, message: Dict) -> None:
        ct, sig = self.crypto.encrypt_and_sign(message)
        for q in list(self.topics.get(topic, set())):
            await q.put((ct, sig))

    async def send(self, recipient: str, message: Dict) -> None:
        ct, sig = self.crypto.encrypt_and_sign(message)
        async with self._lock:
            q = self.mailboxes.setdefault(recipient, asyncio.Queue())
        await q.put((ct, sig))

    async def receive_one(self, recipient: str, timeout: Optional[float] = None):
        async with self._lock:
            q = self.mailboxes.setdefault(recipient, asyncio.Queue())
        try:
            ct, sig = await asyncio.wait_for(q.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        return self.crypto.decrypt_and_verify(ct, sig)
