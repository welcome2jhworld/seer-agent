import asyncio
import io

import pytest

from seer_agent import ContractError, Event
from seer_agent.sources import document, frames, jsonl, merge, message


def test_documents_and_chat_preserve_multilingual_content(tmp_path):
    path = tmp_path/"sop.md"
    path.write_text("누유 확인 절차", encoding="utf-8")
    row = document(path, source="manual", area="pump", observed_at_ms=10)
    assert row.data["text"] == "누유 확인 절차"
    assert row.id == document(path, source="manual", area="pump", observed_at_ms=10).id
    assert message("확인 중", source="operator", message_id="message-1", observed_at_ms=20, area="pump").data["text"] == "확인 중"


def test_jsonl_bounds_and_invalid_event():
    with pytest.raises(ContractError):
        list(jsonl(io.StringIO("a"*262145)))
    with pytest.raises(ContractError):
        Event.from_dict({"id": "event"})


def test_camera_adapter_emits_unknown_for_missing_frame():
    async def source():
        yield {"observed_at_ms": 1, "frame_ref": "file:///frame.jpg"}
        yield {"observed_at_ms": 2, "error": "disconnected"}
    async def run():
        return [event async for event in frames(source(), source="camera", session="session-1")]
    events = asyncio.run(run())
    assert [e.quality for e in events] == ["known", "unknown"]
    assert events[0].id != events[1].id


def test_bounded_fan_in_cancels_without_deadlock():
    stopped = []
    async def source(name):
        try:
            for i in range(100):
                yield Event(f"{name}-{i}", name, "sensor", i)
        finally:
            stopped.append(name)
    async def run():
        combined = merge(source("a"), source("b"), capacity=1)
        assert await anext(combined)
        await combined.aclose()
    asyncio.run(asyncio.wait_for(run(), 1))
    assert sorted(stopped) == ["a", "b"]
