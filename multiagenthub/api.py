from __future__ import annotations
import asyncio
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict

import os
from .hub import Hub
from .orchestrator import Orchestrator
from .models import Task
from .agents.researcher import ResearcherAgent
from .agents.analyzer import AnalyzerAgent
from .agents.synthesizer import SynthesizerAgent
from .persistence import InMemoryPersistence, RedisPersistence


LAST_METRICS: Dict[str, Any] = {}
LAST_HIST: Dict[str, Dict[str, float]] = {}


async def run_demo_flow(query: str = "climate change impacts") -> Dict[str, Any]:
    hub = Hub()
    # choose persistence backend
    persistence = None
    if os.getenv("PERSIST", "memory").lower() == "redis":
        try:
            persistence = RedisPersistence(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
        except Exception:
            persistence = InMemoryPersistence()
    else:
        persistence = InMemoryPersistence()
    researcher = ResearcherAgent("researcher", ["research"], hub)
    analyzer = AnalyzerAgent("analyzer", ["analyze"], hub)
    synthesizer = SynthesizerAgent("synthesizer", ["synthesize"], hub)

    tasks = [
        asyncio.create_task(researcher.start()),
        asyncio.create_task(analyzer.start()),
        asyncio.create_task(synthesizer.start()),
    ]

    orch = Orchestrator(hub, persistence=persistence)
    import uuid

    t1 = Task(id=str(uuid.uuid4()), type="sequential", payload={"skill": "research", "query": query})
    t2 = Task(id=str(uuid.uuid4()), type="sequential", deps=[t1.id], payload={"skill": "analyze"})
    t3 = Task(id=str(uuid.uuid4()), type="sequential", deps=[t2.id], payload={"skill": "synthesize"})

    orch.add_task(t1)
    orch.add_task(t2)
    orch.add_task(t3)

    # Attempt to resume if state exists
    try:
        await orch.load()
    except Exception:
        pass
    await asyncio.wait_for(orch.execute(), timeout=20)
    final = orch.tasks[t3.id].result or {}
    # capture metrics snapshot
    global LAST_METRICS, LAST_HIST
    LAST_METRICS = dict(hub.metrics)
    LAST_HIST = orch.export_histogram()

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
            # Expose minimal Prometheus metrics from last run snapshot
            lines = []
            m = LAST_METRICS or {}
            lines.append("# HELP mah_messages_total Total messages sent via hub")
            lines.append("# TYPE mah_messages_total counter")
            lines.append(f"mah_messages_total {float(m.get('messages_total', 0.0))}")
            lines.append("# HELP mah_task_complete_total Completed tasks")
            lines.append("# TYPE mah_task_complete_total counter")
            lines.append(f"mah_task_complete_total {float(m.get('task_complete_total', 0.0))}")
            lines.append("# HELP mah_errors_total Errors observed")
            lines.append("# TYPE mah_errors_total counter")
            lines.append(f"mah_errors_total {float(m.get('errors_total', 0.0))}")
            # Histograms per task type: mah_task_duration_seconds_bucket{type,le}
            for ttype, h in (LAST_HIST or {}).items():
                # buckets: keys with prefix le_
                total = 0.0
                for k, v in h.items():
                    if k.startswith("le_"):
                        le = k.split("le_",1)[1]
                        lines.append(f"mah_task_duration_seconds_bucket{{type=\"{ttype}\",le=\"{le}\"}} {float(v)}")
                        total = max(total, float(v))
                # +Inf bucket equals total count
                lines.append(f"mah_task_duration_seconds_bucket{{type=\"{ttype}\",le=\"+Inf\"}} {float(h.get('count', 0.0))}")
                lines.append(f"mah_task_duration_seconds_count{{type=\"{ttype}\"}} {float(h.get('count', 0.0))}")
                lines.append(f"mah_task_duration_seconds_sum{{type=\"{ttype}\"}} {float(h.get('sum', 0.0))}")
            body = ("\n".join(lines) + "\n").encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/state":
            # Return last persisted orchestrator state if available via Redis/memory
            try:
                # mirror run_demo_flow persistence selection
                if os.getenv("PERSIST", "memory").lower() == "redis":
                    p = RedisPersistence(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
                else:
                    p = InMemoryPersistence()
                data = asyncio.run(p.load("orchestrator_state"))  # type: ignore[arg-type]
            except Exception:
                data = None
            self._json(200, data or {"status": "empty"})
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
