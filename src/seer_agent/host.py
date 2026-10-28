"""Host protocol v1: one app process runs many missions in one harness over stdio JSON Lines."""
from __future__ import annotations

import asyncio
import importlib.metadata
import json
import threading
from typing import Callable, Iterable

from . import conversation
from .contracts import ContractError, Event, MemoryView, Mission
from .integrations import webhook
from .integrations.http import HttpTransport
from .runtime import Runtime, wall_time_ms
from .store import Store
from .tools import Registry, Tool, ToolContext, ToolResult, ToolSpec

PROTOCOL = 1
MAX_MESSAGE_BYTES = 262144
HEARTBEAT_MS = 2000
# Host tools the app declares at hello to reach the cloud model for each conversation contract.
MODEL_TOOLS = {"plan": "seer.model.plan", "ask": "seer.model.ask", "analysis": "seer.model.analysis"}
MODEL_TIMEOUT_MS = 120000


def harness_version() -> str:
    try:
        return importlib.metadata.version("seer-agent")
    except importlib.metadata.PackageNotFoundError:
        return "0"


def line(message: dict) -> str:
    """ASCII-only JSON so any pipe encoding carries it unchanged."""
    return json.dumps(message, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def integer(value, *, minimum: int = 0, maximum: int = 9007199254740991) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ContractError("invalid_number")
    return value


def text(value, *, limit: int = 160) -> str:
    if not isinstance(value, str) or not 0 < len(value) <= limit:
        raise ContractError("invalid_text")
    return value


class Host:
    """Holds the open missions, one registry and one store; the host must own the store
    exclusively. Tools the app declares at hello are proxies: a call goes to the app as tool.call
    and finishes with the matching tool.result. The runtime's deadline still applies, so an
    unanswered call becomes an unknown outcome."""

    def __init__(self, registry: Registry, store: Store, write: Callable[[dict], None], *,
                 clock: Callable[[], int] = wall_time_ms, allow_external: bool = False):
        self.registry, self.store, self.write, self.clock = registry, store, write, clock
        self.allow_external = allow_external
        self.greeted = self.stopping = False
        self.done = asyncio.Event()
        self.runtimes: dict[str, Runtime] = {}
        self.revisions: dict[str, int] = {}
        self.failures: dict[str, str] = {}
        self.calls: dict[str, tuple[asyncio.Future, ToolSpec, int]] = {}
        self.pumps: dict[str, set[asyncio.Task]] = {}
        self.dirty: set[str] = set()
        self.sequence = 0
        self.conversations: set[asyncio.Task] = set()
        self.turns = 0
        self.watching: dict[str, tuple[dict, list, object]] = {}
        self.handlers = {"hello": self.hello, "mission.open": self.open, "mission.close": self.close_mission,
                         "event": self.event, "coverage": self.coverage, "tool.result": self.tool_result,
                         "incident.clear": self.clear, "shutdown": self.shutdown, "plan": self.plan,
                         "ask": self.ask, "analysis": self.analysis, "tool.progress": self.tool_progress}

    def send(self, kind: str, body: dict, reply_to=None) -> None:
        self.sequence += 1
        message = {"v": PROTOCOL, "type": kind, "id": f"h{self.sequence}", "body": body}
        if reply_to is not None:
            message["reply_to"] = reply_to
        self.write(message)

    def error(self, code: str, reply_to=None, **context) -> None:
        self.send("error", {"code": code, **context}, reply_to)

    async def receive(self, raw: str) -> None:
        reply = None
        try:
            if len(raw.encode("utf-8")) > MAX_MESSAGE_BYTES:
                return self.error("message_too_large")
            try:
                message = json.loads(raw)
            except ValueError:
                return self.error("invalid_json")
            if not isinstance(message, dict) or not isinstance(message.get("body", {}), dict):
                return self.error("invalid_message")
            ident = message.get("id")
            reply = ident if isinstance(ident, (str, int)) and not isinstance(ident, bool) else None
            if message.get("v") != PROTOCOL:
                return self.error("protocol_mismatch", reply)
            handler = self.handlers.get(message.get("type"))
            if handler is None:
                return self.error("unknown_type", reply)
            if handler != self.hello and not self.greeted:
                return self.error("hello_required", reply)
            await handler(message.get("body", {}), reply)
        except ContractError as error:
            self.error(str(error), reply)
        except Exception:
            # Messages carry site data; a bad one is refused without details and never stops the host.
            self.error("invalid_message", reply)

    def proxy(self, spec: ToolSpec) -> Tool:
        async def execute(context: ToolContext, arguments: dict) -> ToolResult:
            body = {"call": context.operation_id, "tool": spec.name, "mission": context.mission_id,
                    "arguments": arguments, "event": context.event.to_dict(), "deadline_ms": context.deadline_ms}
            if len(line({"v": PROTOCOL, "type": "tool.call", "id": "h", "body": body})) > MAX_MESSAGE_BYTES - 64:
                return ToolResult("failed", {"reason": "tool_call_too_large"})
            future = asyncio.get_running_loop().create_future()
            self.calls[context.operation_id] = (future, spec, context.deadline_ms)
            try:
                self.send("tool.call", body)
                return await future
            finally:
                self.calls.pop(context.operation_id, None)
        return Tool(spec, execute)

    async def hello(self, body: dict, reply) -> None:
        if self.greeted:
            raise ContractError("already_greeted")
        if body.get("protocol") != PROTOCOL:
            raise ContractError("protocol_mismatch")
        tools = body.get("tools", [])
        if not isinstance(tools, list) or len(tools) > 64 or not all(isinstance(t, dict) for t in tools):
            raise ContractError("invalid_host_tools")
        try:
            proxies = [self.proxy(ToolSpec(text(t["name"]), text(str(t["version"]), limit=40), t["effect"],
                                           t["input_schema"], t["output_schema"], t.get("configuration", {})))
                       for t in tools]
        except ContractError:
            raise
        except Exception:
            raise ContractError("invalid_host_tools") from None
        integrations = body.get("integrations", [])
        if not isinstance(integrations, list) or len(integrations) > 16:
            raise ContractError("invalid_integrations")
        for item in integrations:
            proxies.append(self.integration(item))
        names = [tool.spec.name for tool in proxies]
        if len(set(names)) != len(names) or any(name in self.registry.tools for name in names):
            raise ContractError("duplicate_tool")
        for tool in proxies:
            self.registry.tool(tool)
        self.greeted = True
        stored = [row[0] for row in self.store.db.execute("SELECT id FROM missions ORDER BY id")]
        self.send("ready", {"protocol": PROTOCOL, "harness_version": harness_version(), "missions": stored,
                            "contracts": conversation.CONTRACTS}, reply)

    @staticmethod
    def integration(item) -> Tool:
        """Host-configured senders that run in the harness, such as a LAN webhook."""
        if not isinstance(item, dict) or item.get("kind") != "webhook":
            raise ContractError("invalid_integrations")
        allow_private, require_receipt = item.get("allow_private", False), item.get("require_receipt", True)
        if not isinstance(allow_private, bool) or not isinstance(require_receipt, bool):
            raise ContractError("invalid_integrations")
        endpoint = text(item.get("endpoint"), limit=2048)
        HttpTransport(allow_private=allow_private).check(endpoint)
        return webhook(name=text(item.get("name")), endpoint=endpoint, allow_private=allow_private,
                       require_receipt=require_receipt)

    async def open(self, body: dict, reply) -> None:
        if not isinstance(body.get("mission"), dict):
            raise ContractError("invalid_mission")
        mission = Mission.from_dict(body["mission"])
        key = mission.key
        latest = self.store.db.execute("SELECT COALESCE(MAX(revision),0) FROM decisions WHERE mission=?", (key,)).fetchone()[0]
        after = integer(body["after_revision"]) if "after_revision" in body else latest
        runtime = self.runtimes.get(key)
        if runtime is not None and runtime.doc != mission.document:
            raise ContractError("mission_or_plugin_changed_without_new_version")
        if runtime is None:
            runtime = Runtime(mission, self.registry, self.store, clock=self.clock, allow_external=self.allow_external)
            # This process owns the store; work a previous process left running has an unknown outcome.
            self.store.interrupt(key, self.clock())
            self.runtimes[key] = runtime
        self.revisions[key] = min(after, latest)
        self.send("ack", {"mission": key, "state": runtime.state, "revision": latest}, reply)
        self.decisions(key)
        self.schedule(key)

    async def close_mission(self, body: dict, reply) -> None:
        key = text(body.get("mission"))
        if self.runtimes.pop(key, None) is None:
            raise ContractError("mission_not_open")
        tasks = self.pumps.pop(key, set())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.revisions.pop(key, None)
        self.failures.pop(key, None)
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

    async def event(self, body: dict, reply) -> None:
        if not isinstance(body.get("event"), dict):
            raise ContractError("invalid_event")
        event = Event.from_dict(body["event"])
        keys = [k for k, r in self.runtimes.items() if event.source in r.doc["sources"]]
        self.send("ack", {"event_id": event.id, "results": self.route(event, keys)}, reply)

    async def coverage(self, body: dict, reply) -> None:
        start, end = integer(body.get("from_ms")), integer(body.get("to_ms"))
        observed, source = body.get("observed"), text(body.get("source"))
        if end < start or not isinstance(observed, bool):
            raise ContractError("invalid_coverage")
        reason = body.get("reason", "")
        event = Event(f"coverage:{source}:{start}:{end}", source, "coverage", end,
                      {"from_ms": start, "to_ms": end, "observed": observed,
                       "reason": reason[:200] if isinstance(reason, str) else ""},
                      "known" if observed else "unknown", "sensor")
        keys = [k for k, r in self.runtimes.items() if "coverage" in r.doc["sources"].get(source, [])]
        self.send("ack", {"event_id": event.id, "results": self.route(event, keys)}, reply)

    async def tool_result(self, body: dict, reply) -> None:
        call = self.calls.get(body.get("call")) if isinstance(body.get("call"), str) else None
        if call is None or call[0].done():
            raise ContractError("unknown_call")
        future, spec, deadline = call
        if self.clock() >= deadline:
            raise ContractError("call_expired")
        data = body.get("data", {})
        result = ToolResult(body.get("status"), data, body.get("receipt"))
        try:
            result.validate(spec)
        except ContractError as error:
            # The runtime records an invalid result as unknown; the app learns why.
            future.set_result(result)
            raise error
        future.set_result(result)

    async def clear(self, body: dict, reply) -> None:
        key = text(body.get("mission"))
        runtime = self.runtimes.get(key)
        if runtime is None:
            raise ContractError("mission_not_open")
        event_id = text(body["event_id"]) if "event_id" in body else None
        cancelled = runtime.clear(event_id)
        self.send("ack", {"mission": key, "cancelled": cancelled}, reply)
        self.decisions(key)
        self.schedule(key)

    def model(self, kind: str, request: dict, timeout_ms: int, reply=None):
        """The cloud model for a conversation is a host tool; None when the app declared none."""
        tool = self.registry.tools.get(MODEL_TOOLS[kind])
        if tool is None:
            return None

        async def call(payload: dict) -> dict:
            self.turns += 1
            operation = f"conversation:{kind}:{request['conversation']}:{self.turns}"
            event = Event(operation, "app", f"{kind}.request", self.clock(),
                          {"conversation": request["conversation"]}, provenance="human")
            context = ToolContext(operation, f"conversation.{kind}", event, MemoryView([]), self.clock()+timeout_ms)
            if kind == "ask":
                self.watching[operation] = (request, payload["scope"], reply)
            try:
                result = await asyncio.wait_for(tool.execute(context, payload), timeout_ms/1000)
            finally:
                self.watching.pop(operation, None)
            if result.status != "succeeded":
                raise ContractError("model_failed")
            return result.data
        return call

    def converse(self, kind: str, body: dict, reply) -> None:
        """Rules answer at once; a model call runs as its own task so tool.result can arrive meanwhile.
        A request that passes its checks always gets a result: an error or a stop becomes the contract's
        failure, never silence."""
        if self.stopping:
            raise ContractError("stopping")
        timeout = min(integer(body.get("timeout_ms", MODEL_TIMEOUT_MS), minimum=1), 600000)
        request = {key: value for key, value in body.items() if key != "timeout_ms"}
        check = {"plan": conversation.check_plan_request, "ask": conversation.check_ask_request,
                 "analysis": conversation.check_analysis_request}[kind]
        check(request)
        model = self.model(kind, request, timeout, reply)

        async def run():
            try:
                if kind == "plan":
                    result = await conversation.make_plan(request, model)
                elif kind == "ask":
                    result = await conversation.answer_ask(request, model, lambda p: self.send("ask.progress", p, reply))
                else:
                    result = await conversation.run_analysis(request, model)
            except asyncio.CancelledError:
                self.send(f"{kind}.result", conversation.failed(kind, request, conversation.STOPPED), reply)
                raise
            except Exception:
                reason = {"plan": conversation.PLAN_FAILED, "ask": conversation.ASK_FAILED,
                          "analysis": conversation.ANALYSIS_FAILED}[kind]
                result = conversation.failed(kind, request, reason)
            self.send(f"{kind}.result", result, reply)
        task = asyncio.get_running_loop().create_task(run())
        self.conversations.add(task)
        task.add_done_callback(self.conversations.discard)

    async def plan(self, body: dict, reply) -> None:
        self.converse("plan", body, reply)

    async def ask(self, body: dict, reply) -> None:
        self.converse("ask", body, reply)

    async def analysis(self, body: dict, reply) -> None:
        self.converse("analysis", body, reply)

    async def tool_progress(self, body: dict, reply) -> None:
        """The app reports how far the model has watched; Ask relays it as seer.ask progress."""
        watch = self.watching.get(body.get("call")) if isinstance(body.get("call"), str) else None
        if watch is None:
            raise ContractError("unknown_call")
        request, scope, origin = watch
        self.send("ask.progress", conversation.ask_progress(request, scope, integer(body.get("done_ms"))), origin)

    async def shutdown(self, body: dict, reply) -> None:
        if self.stopping:
            raise ContractError("already_stopping")
        timeout = min(integer(body.get("timeout_ms", 5000)), 60000) / 1000
        self.stopping = True

        async def drain():
            # Tool results keep arriving while pending work and open conversations drain.
            pending = [task for tasks in self.pumps.values() for task in tasks if not task.done()]
            pending += [task for task in self.conversations if not task.done()]
            if pending:
                await asyncio.wait(pending, timeout=timeout)
            stopped = [task for task in self.conversations if not task.done()]
            for task in stopped:
                task.cancel()
            await asyncio.gather(*stopped, return_exceptions=True)
            self.send("ack", {}, reply)
            self.done.set()
        asyncio.get_running_loop().create_task(drain())

    def schedule(self, key: str) -> None:
        """One pump per free dispatch slot, so new work does not wait behind a slow tool call."""
        runtime = self.runtimes.get(key)
        if runtime is None or self.stopping:
            return
        tasks = self.pumps.setdefault(key, set())
        tasks.difference_update({task for task in tasks if task.done()})
        self.dirty.add(key)
        if len(tasks) < runtime.doc["policy"]["max_concurrency"]:
            task = asyncio.get_running_loop().create_task(self.pump(key))
            tasks.add(task)

    async def pump(self, key: str) -> None:
        while (runtime := self.runtimes.get(key)) is not None:
            self.dirty.discard(key)
            try:
                worked = await runtime.drain()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                code = str(error) if isinstance(error, ContractError) else "pump_failed"
                self.failures[key] = code
                self.error(code, mission=key)
                return
            self.failures.pop(key, None)
            self.decisions(key)
            if not worked and key not in self.dirty:
                return

    def decisions(self, key: str) -> None:
        rows = self.store.db.execute("SELECT * FROM decisions WHERE mission=? AND revision>? ORDER BY revision",
                                     (key, self.revisions.get(key, 0))).fetchall()
        for row in rows:
            if row["transition"]:
                self.send("decision", {"mission": key, "revision": row["revision"], "event_id": row["event_id"],
                                       "transition": row["transition"], "summary": row["summary"],
                                       "evidence": json.loads(row["evidence"])})
            self.revisions[key] = row["revision"]

    def heartbeat(self) -> None:
        missions = []
        for key, runtime in self.runtimes.items():
            pending = self.store.db.execute(
                "SELECT COUNT(*) FROM operations WHERE mission=? AND status IN ('pending','running')", (key,)).fetchone()[0]
            health = "failing" if key in self.failures else runtime.state.get("health", "unknown")
            missions.append({"mission": key, "pending": pending, "health": health,
                             **({"error": self.failures[key]} if key in self.failures else {})})
        self.send("heartbeat", {"at_ms": self.clock(), "missions": missions})

    async def heartbeats(self, interval_ms: int = HEARTBEAT_MS) -> None:
        while True:
            if self.greeted:
                self.heartbeat()
                # Deadlines expire even when no new event arrives.
                for key in list(self.runtimes):
                    self.schedule(key)
            await asyncio.sleep(interval_ms / 1000)

    async def close(self) -> None:
        tasks = [task for tasks in self.pumps.values() for task in tasks] + list(self.conversations)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def serve(host: Host, lines: Iterable) -> int:
    """Reads messages on a thread so blocking stdin never stalls tool dispatch. Byte lines are
    decoded one at a time, so one invalid line is refused without ending the session."""
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue()

    def read():
        try:
            for raw in lines:
                loop.call_soon_threadsafe(queue.put_nowait, raw)
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
            raw, getter = getter.result(), None
            if raw is None:
                break
            if isinstance(raw, bytes):
                try:
                    raw = raw.decode("utf-8")
                except UnicodeDecodeError:
                    host.error("invalid_utf8")
                    continue
            if raw.strip():
                await host.receive(raw)
    finally:
        for task in (beat, stopped, getter):
            if task is not None:
                task.cancel()
        await host.close()
    return 0


def writer(stream) -> Callable[[dict], None]:
    def write(message: dict) -> None:
        stream.write((line(message)+"\n").encode("ascii"))
        stream.flush()
    return write


__all__ = ["Host", "serve", "writer", "line", "PROTOCOL", "MAX_MESSAGE_BYTES", "HEARTBEAT_MS"]
