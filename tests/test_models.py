from __future__ import annotations
from multiagenthub.models import Message, MessageType, Task, TaskState


def test_message_roundtrip():
    m = Message(id="1", sender="a", recipient="b", type=MessageType.request, method="x", params={"k": 1})
    d = m.dict()
    m2 = Message(**d)
    assert m2.id == "1" and m2.method == "x" and m2.params == {"k": 1}


def test_task_defaults():
    t = Task(id="t1", type="sequential", payload={})
    assert t.state == TaskState.pending and t.attempts == 0 and t.max_retries == 1
