from __future__ import annotations
import asyncio
import json
import logging
from typing import Set

logger = logging.getLogger(__name__)


class EventServer:
    """
    Minimal WebSocket broadcast server for task lifecycle events.
    Dependence on `websockets` is optional; server only runs if available.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 8765) -> None:
        self.host = host
        self.port = port
        self._srv = None
        self._clients: Set[object] = set()
        try:
            import websockets  # type: ignore
        except Exception as e:  # pragma: no cover - optional
            self._ws_mod = None
            logger.warning("websockets_not_available: %s", e)
        else:
            self._ws_mod = websockets

    async def start(self) -> None:
        if not self._ws_mod:
            return

        async def handler(ws):
            self._clients.add(ws)
            try:
                async for _ in ws:
                    pass
            finally:
                self._clients.discard(ws)

        self._srv = await self._ws_mod.serve(handler, self.host, self.port)  # type: ignore[attr-defined]
        logger.info("EventServer running at ws://%s:%d", self.host, self.port)

    async def stop(self) -> None:
        if self._srv:
            self._srv.close()
            await self._srv.wait_closed()
            self._srv = None

    async def broadcast(self, event: dict) -> None:
        if not self._clients:
            return
        msg = json.dumps(event, separators=(",", ":"))
        senders = [c.send(msg) for c in list(self._clients)]
        await asyncio.gather(*senders, return_exceptions=True)
