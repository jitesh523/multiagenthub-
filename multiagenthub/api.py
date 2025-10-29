from __future__ import annotations
import asyncio
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict

from .hub import Hub
from .orchestrator import Orchestrator
from .models import Task
from .agents.researcher import ResearcherAgent
from .agents.analyzer import AnalyzerAgent
from .agents.synthesizer import SynthesizerAgent


async def run_demo_flow(query: str = "climate change impacts") -> Dict[str, Any]:
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
    import uuid

    t1 = Task(id=str(uuid.uuid4()), type="sequential", payload={"skill": "research", "query": query})
    t2 = Task(id=str(uuid.uuid4()), type="sequential", deps=[t1.id], payload={"skill": "analyze"})
    t3 = Task(id=str(uuid.uuid4()), type="sequential", deps=[t2.id], payload={"skill": "synthesize"})

    orch.add_task(t1)
    orch.add_task(t2)
    orch.add_task(t3)

    await asyncio.wait_for(orch.execute(), timeout=20)
    final = orch.tasks[t3.id].result or {}

    for t in tasks:
        t.cancel()

    return final


class App(BaseHTTPRequestHandler):
    def _json(self, code: int, obj: Dict[str, Any]) -> None:
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        if self.path == "/metrics":
            # simple static response for now
            self._json(200, {"status": "ok"})
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):  # noqa: N802
        if self.path == "/run-demo":
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length).decode("utf-8") if length else "{}"
            try:
                payload = json.loads(raw) if raw else {}
            except Exception:
                payload = {}
            query = payload.get("query", "climate change impacts")
            try:
                result = asyncio.run(run_demo_flow(query))
                self._json(200, result)
            except Exception as e:  # pragma: no cover - network/server errors
                self._json(500, {"error": str(e)})
        else:
            self._json(404, {"error": "not found"})


def serve(host: str = "127.0.0.1", port: int = 8000) -> None:
    httpd = HTTPServer((host, port), App)
    print(f"Serving MultiAgentHub API at http://{host}:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    serve()
