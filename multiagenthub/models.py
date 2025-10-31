from __future__ import annotations
import time
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class MessageType(str, Enum):
    request = "request"
    response = "response"
    event = "event"


class Message(BaseModel):
    id: str
    sender: str
    recipient: str
    type: MessageType
    method: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None
    timestamp: float = Field(default_factory=lambda: time.time())
    trace_id: Optional[str] = None


class TaskState(str, Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"
    skipped = "skipped"


class Task(BaseModel):
    id: str
    type: str
    payload: Dict[str, Any]
    state: TaskState = TaskState.pending
    attempts: int = 0
    max_retries: int = 1
    timeout_s: float = 30.0
    deps: List[str] = Field(default_factory=list)
    owner: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    condition: Optional[str] = None  # Python expression evaluated with 'prev' (merged predecessor results)
    trace_id: Optional[str] = None
