"""Input adapters feed data; only the host supplies mission configuration."""
from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from typing import AsyncIterable, TextIO

from .contracts import ContractError, Event


def jsonl(stream: TextIO):
    while True:
        line = stream.readline(262146)
        if not line:
            return
        if len(line.encode()) > 262144:
            raise ContractError("input_line_too_large")
        if line.strip():
            yield Event.from_dict(json.loads(line))


def document(path: str | Path, *, source: str, area: str, observed_at_ms: int) -> Event:
    with Path(path).open("rb") as stream:
        content = stream.read(65537)
    if len(content) > 65536:
        raise ContractError("document_too_large")
    checksum = hashlib.sha256(content).hexdigest()
    return Event(f"document:{source}:{checksum}", source, "document", observed_at_ms,
                 {"area": area, "text": content.decode("utf-8"), "sha256": checksum}, provenance="unverified")


def message(text: str, *, source: str, message_id: str, observed_at_ms: int, area: str) -> Event:
    return Event.from_dict(Event(message_id, source, "message", observed_at_ms,
                                {"area": area, "text": text}, provenance="human").to_dict())


async def merge(*sources: AsyncIterable[Event], capacity: int = 64):
    """Bounded fan-in. Backpressure reaches producers; no silent observation drops."""
    if not 1 <= capacity <= 10000:
        raise ContractError("invalid_queue_capacity")
    queue = asyncio.Queue(capacity)
    async def consume(source):
        try:
            async for event in source:
                await queue.put(event)
        except asyncio.CancelledError:
            raise
        except Exception:
            await queue.put(ContractError("source_failed"))
        await queue.put(None)
    tasks = [asyncio.create_task(consume(source)) for source in sources]
    ended = 0
    try:
        while ended < len(tasks):
            event = await queue.get()
            if event is None:
                ended += 1
            elif isinstance(event, Exception):
                raise event
            else:
                yield event
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def frames(stream: AsyncIterable[dict], *, source: str, session: str):
    """Adapt a camera SDK's frame references, without owning codecs or frame buffers.

    Each row contains observed_at_ms and either a frame_ref or an explicit error.
    The host owns reference availability, RTSP authentication and media retention.
    """
    sequence = 0
    async for row in stream:
        sequence += 1
        known = isinstance(row.get("frame_ref"), str) and bool(row["frame_ref"])
        yield Event.from_dict(Event(f"{source}:{session}:{sequence}", source, "camera.frame",
            row["observed_at_ms"], {"frame_ref": row["frame_ref"]} if known else {"reason": "camera_unavailable"},
            "known" if known else "unknown", "sensor").to_dict())
