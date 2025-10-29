# MultiAgentHub

A modular multi-agent collaboration system enabling agents to communicate, coordinate, and solve tasks using a DAG-based orchestrator.

## Architecture

```
+------------------------------ MultiAgentHub ------------------------------+
|  Agent Registry  |  Message Bus (encrypted)  |  Orchestrator (DAG engine) |
+------------------+---------------------------+-----------------------------+
      | register            | route P2P/pub-sub            | assign tasks       
      v                     v                               v                   
  Researcher --------> Analyzer --------> Synthesizer  ---> Results             
```

- All messages are encrypted (Fernet) and signed (HMAC).
- Orchestrator builds a DAG, assigns tasks based on required `skill`, handles retries/timeouts, and propagates results to dependent tasks.

## Features

- Encrypted in-memory bus (pub-sub, P2P, broadcast-ready)
- DAG orchestration with retries, timeouts, dependency management
- Dynamic skill-based assignment via Hub registry
- Example agents: Researcher, Analyzer, Synthesizer
- Metrics counters and basic logging
- Unit tests and runnable demo

## Project Structure

```
multiagenthub/
  models.py            # Message/Task schemas
  security.py          # CryptoContext (Fernet + HMAC)
  bus.py               # InMemoryBus
  hub.py               # Registry, routing, metrics
  orchestrator.py      # DAG engine with result propagation
  agents/
    base.py            # AgentBase
    researcher.py      # ResearcherAgent
    analyzer.py        # AnalyzerAgent
    synthesizer.py     # SynthesizerAgent (LLM or fallback)
examples/
  demo.py              # Climate impacts workflow demo
tests/
  test_models.py
  test_orchestrator.py
```

## Setup

Requirements: Python 3.10+

```
pip install -r requirements.txt
```

Optional for LLM:

```
export OPENAI_API_KEY=...         # enables real LLM calls in SynthesizerAgent
export OPENAI_MODEL=gpt-4o-mini   # optional (default shown)
```

## Run Demo

```
python -m examples.demo
```

Expected output (example):

```json
{
  "report": {
    "query": "climate change impacts",
    "summary": "Summary for climate change impacts: key themes include ...",
    "highlights": [["climate", 3], ["impacts", 2], ...]
  }
}
```

## Tests

```
pip install -r requirements.txt
pip install pytest
pytest -q
```

## Docker

Build and run:

```
docker build -t multiagenthub .
docker run --rm -e OPENAI_API_KEY -it multiagenthub
```

## Configuration

- `OPENAI_API_KEY` — enables real LLM summarization in `SynthesizerAgent`.
- `OPENAI_MODEL` — optional model override.

## Roadmap (v2 ideas)

- WebSocket JSON-RPC server and Redis transport for distributed runs
- Agent plugin discovery via entry points
- Persistent task/event store and crash recovery
- Rich metrics (Prometheus) and structured logging
- Advanced branching: conditional tasks and dynamic DAG rewiring
