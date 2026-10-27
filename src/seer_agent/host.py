"""Host protocol v1: one app process runs many missions in one harness over stdio JSON Lines."""
from __future__ import annotations

import asyncio
import importlib.metadata
import json
import threading
from typing import Callable, Iterable

from .contracts import ContractError, Event, Mission
from .runtime import Runtime, wall_time_ms
from .store import Store
from .tools import Registry, Tool, ToolContext, ToolResult, ToolSpec

PROTOCOL = 1
MAX_MESSAGE_BYTES = 262144
HEARTBEAT_MS = 2000


def harness_version() -> str:
    try:
        return importlib.metadata.version("seer-agent")
    except importlib.metadata.PackageNotFoundError:
        return "0"


class Host:
    """Holds the open missions, one registry and one store. Tools the app declares at hello are
    proxies: a call goes to the app as tool.call and finishes with the matching tool.result. The
    runtime's deadline still applies, so an unanswered call becomes an unknown outcome."""

    def __init__(self, registry: Registry, store: Store, write: Callable[[dict], None], *,
                 clock: Callable[[], int] = wall_time_ms, allow_external: bool = False):
        self.registry, self.store, self.write, self.clock = registry, store, write, clock
        self.allow_external = allow_external
        self.greeted = self.stopping = False
        self.done = asyncio.Event()
        self.runtimes: dict[str, Runtime] = {}
        self.revisions: dict[str, int] = {}
        self.calls: dict[str, asyncio.Future] = {}
        self.pumps: dict[str, asyncio.Task] = {}
        self.dirty: set[str] = set()
        self.sequence = 0
        self.handlers = {"hello": self.hello, "mission.open": self.open, "mission.close": self.close_mission,
                         "event": self.event, "coverage": self.coverage, "tool.result": self.tool_result,
                         "incident.clear": self.clear, "shutdown": self.shutdown}

    def send(self, kind: str, body: dict, reply_to: str | None = None) -> None:
        self.sequence += 1
        message = {"v": PROTOCOL, "type": kind, "id": f"h{self.sequence}", "body": body}
        if reply_to is not None:
            message["reply_to"] = reply_to
        self.write(message)

    def error(self, code: str, reply_to: str | None = None) -> None:
        self.send("error", {"code": code}, reply_to)

    async def receive(self, line: str) -> None:
        if len(line.encode()) > MAX_MESSAGE_BYTES:
            return self.error("message_too_large")
        try:
            message = json.loads(line)
        except ValueError:
            return self.error("invalid_json")
        if not isinstance(message, dict) or not isinstance(message.get("body", {}), dict):
            return self.error("invalid_message")
        reply = message.get("id") if isinstance(message.get("id"), str) else None
        if message.get("v") != PROTOCOL:
            return self.error("protocol_mismatch", reply)
        handler = self.handlers.get(message.get("type"))
        if handler is None:
            return self.error("unknown_type", reply)
        if handler != self.hello and not self.greeted:
            return self.error("hello_required", reply)
        try:
            await handler(message.get("body", {}), reply)
        except ContractError as error:
            self.error(str(error), reply)
        except (KeyError, TypeError, ValueError):
            # Messages can carry site data; never echo details.
            self.error("invalid_message", reply)

    def proxy(self, spec: ToolSpec) -> Tool:
        async def execute(context: ToolContext, arguments: dict) -> ToolResult:
            future = asyncio.get_running_loop().create_future()
            self.calls[context.operation_id] = future
            try:
                self.send("tool.call", {"call": context.operation_id, "tool": spec.name, "mission": context.mission_id,
                                        "arguments": arguments, "event": context.event.to_dict(),
                                        "deadline_ms": context.deadline_ms})
                return await future
            finally:
                self.calls.pop(context.operation_id, None)
        return Tool(spec, execute)

    async def hello(self, body: dict, reply: str | None) -> None:
        if self.greeted:
            raise ContractError("already_greeted")
        if body.get("protocol") != PROTOCOL:
            raise ContractError("protocol_mismatch")
        tools = body.get("tools", [])
        if not isinstance(tools, list) or len(tools) > 64:
            raise ContractError("invalid_host_tools")
        specs = [ToolSpec(t["name"], str(t["version"]), t["effect"], t["input_schema"], t["output_schema"],
                          t.get("configuration", {})) for t in tools]
        for spec in specs:
            if spec.name in self.registry.tools:
                raise ContractError("duplicate_tool")
        for spec in specs:
            self.registry.tool(self.proxy(spec))
        self.greeted = True
        stored = [row[0] for row in self.store.db.execute("SELECT id FROM missions ORDER BY id")]
        self.send("ready", {"protocol": PROTOCOL, "harness_version": harness_version(), "missions": stored}, reply)

    async def open(self, body: dict, reply: str | None) -> None:
        mission = Mission.from_dict(body["mission"])
        runtime = self.runtimes.get(mission.key)
        if runtime is None or runtime.doc != mission.document:
            if runtime is not None:
                raise ContractError("mission_or_plugin_changed_without_new_version")
            runtime = Runtime(mission, self.registry, self.store, clock=self.clock, allow_external=self.allow_external)
            self.runtimes[mission.key] = runtime
            self.revisions[mission.key] = self.store.db.execute(
                "SELECT COALESCE(MAX(revision),0) FROM decisions WHERE mission=?", (mission.key,)).fetchone()[0]
        self.send("ack", {"mission": mission.key, "state": runtime.state}, reply)
        self.schedule(mission.key)

    async def close_mission(self, body: dict, reply: str | None) -> None:
        key = body["mission"]
        if self.runtimes.pop(key, None) is None:
            raise ContractError("mission_not_open")
        task = self.pumps.pop(key, None)
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.revisions.pop(key, None)
        self.dirty.discard(key)
        self.send("ack", {"mission": key}, reply)

    def route(self, event: Event, keys: Iterable[str]) -> list[dict]:
        results = []
        for key in keys:
            try:
                result = self.runtimes[key].ingest(event)
                results.append({"mission": key, "status": result["status"], "transition": result.get("transition")})
            except ContractError as error:
                results.append({"mission": key, "status": "error", "code": str(error)})
            self.decisions(key)
            self.schedule(key)
        return results

    async def event(self, body: dict, reply: str | None) -> None:
        event = Event.from_dict(body["event"])
        keys = [k for k, r in self.runtimes.items() if event.source in r.doc["sources"]]
        self.send("ack", {"event_id": event.id, "results": self.route(event, keys)}, reply)

    async def coverage(self, body: dict, reply: str | None) -> None:
        start, end, observed = int(body["from_ms"]), int(body["to_ms"]), body["observed"]
        if end < start or not isinstance(observed, bool):
            raise ContractError("invalid_coverage")
        source = body["source"]
        event = Event(f"coverage:{source}:{start}:{end}", source, "coverage", end,
                      {"from_ms": start, "to_ms": end, "observed": observed, "reason": str(body.get("reason", ""))[:200]},
                      "known" if observed else "unknown", "sensor")
        keys = [k for k, r in self.runtimes.items() if "coverage" in r.doc["sources"].get(source, [])]
        self.send("ack", {"event_id": event.id, "results": self.route(event, keys)}, reply)

    async def tool_result(self, body: dict, reply: str | None) -> None:
        future = self.calls.get(body["call"])
        if future is None or future.done():
            raise ContractError("unknown_call")
        receipt = body.get("receipt")
        future.set_result(ToolResult(body["status"], body.get("data", {}), receipt))

    async def clear(self, body: dict, reply: str | None) -> None:
        key = body["mission"]
        runtime = self.runtimes.get(key)
        if runtime is None:
            raise ContractError("mission_not_open")
        query = "SELECT id FROM operations WHERE mission=? AND status='pending'"
        arguments = [key]
        if "event_id" in body:
            query += " AND event_id=?"
            arguments.append(body["event_id"])
        cancelled = [row[0] for row in self.store.db.execute(query, arguments).fetchall()]
        for operation in cancelled:
            runtime.cancel(operation)
        self.send("ack", {"mission": key, "cancelled": cancelled}, reply)

    async def shutdown(self, body: dict, reply: str | None) -> None:
        if self.stopping:
            raise ContractError("already_stopping")
        timeout = max(0, min(int(body.get("timeout_ms", 5000)), 60000)) / 1000
        self.stopping = True

        async def drain():
            # Tool results keep arriving while pending work drains.
            pending = [task for task in self.pumps.values() if not task.done()]
            if pending:
                await asyncio.wait(pending, timeout=timeout)
            self.send("ack", {}, reply)
            self.done.set()
        asyncio.get_running_loop().create_task(drain())

    def schedule(self, key: str) -> None:
        task = self.pumps.get(key)
        if task is not None and not task.done():
            self.dirty.add(key)
        elif not self.stopping:
            self.pumps[key] = asyncio.get_running_loop().create_task(self.pump(key))

    async def pump(self, key: str) -> None:
        while (runtime := self.runtimes.get(key)) is not None:
            self.dirty.discard(key)
            try:
                worked = await runtime.drain()
            except ContractError as error:
                self.send("error", {"code": str(error), "mission": key})
                return
            self.decisions(key)
            if not worked and key not in self.dirty:
                return

    def decisions(self, key: str) -> None:
        rows = self.store.db.execute("SELECT * FROM decisions WHERE mission=? AND revision>? ORDER BY revision",
                                     (key, self.revisions.get(key, 0))).fetchall()
        for row in rows:
            self.revisions[key] = row["revision"]
            if row["transition"]:
                self.send("decision", {"mission": key, "event_id": row["event_id"], "transition": row["transition"],
                                       "summary": row["summary"], "evidence": json.loads(row["evidence"])})

    def heartbeat(self) -> None:
        missions = []
        for key in self.runtimes:
            pending = self.store.db.execute(
                "SELECT COUNT(*) FROM operations WHERE mission=? AND status IN ('pending','running')", (key,)).fetchone()[0]
            missions.append({"mission": key, "pending": pending})
        self.send("heartbeat", {"at_ms": self.clock(), "missions": missions})

    async def heartbeats(self, interval_ms: int = HEARTBEAT_MS) -> None:
        while True:
            self.heartbeat()
            await asyncio.sleep(interval_ms / 1000)

    async def close(self) -> None:
        tasks = list(self.pumps.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def serve(host: Host, lines: Iterable[str]) -> int:
    """Reads messages on a thread so blocking stdin never stalls tool dispatch."""
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue()

    def read():
        try:
            for line in lines:
                loop.call_soon_threadsafe(queue.put_nowait, line)
        finally:
            loop.call_soon_threadsafe(queue.put_nowait, None)

    threading.Thread(target=read, daemon=True).start()
    beat = asyncio.create_task(host.heartbeats())
    stopped = asyncio.create_task(host.done.wait())
    getter = None
    try:
        while not host.done.is_set():
            getter = getter or asyncio.create_task(queue.get())
            finished, _ = await asyncio.wait({getter, stopped}, return_when=asyncio.FIRST_COMPLETED)
            if getter not in finished:
                break
            line, getter = getter.result(), None
            if line is None:
                break
            if line.strip():
                await host.receive(line)
    finally:
        for task in (beat, stopped, getter):
            if task is not None:
                task.cancel()
        await host.close()
    return 0
