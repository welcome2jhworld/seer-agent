import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from seer_agent import Store
from seer_agent.builtins import DETECTIONS, demo_registry
from seer_agent.contracts import TEXT, object_schema
from seer_agent.host import Host, MAX_MESSAGE_BYTES, serve

EXAMPLES = Path(__file__).parents[1] / "examples"
NOTIFY = {"name": "site.notify", "version": "1", "effect": "local",
          "input_schema": object_schema({"text": TEXT}), "output_schema": object_schema({"sent": {"type": "boolean"}})}
DETECT = {"name": "app.detect", "version": "1", "effect": "read", "input_schema": object_schema({}),
          "output_schema": DETECTIONS}
ENV = {**os.environ, "PYTHONPATH": str(Path(__file__).parents[1]/"src")}


def mission(timeout_ms=30000, detect="fixture.detect"):
    doc = json.loads((EXAMPLES/"person_detection"/"mission.json").read_text())
    doc["observe"][0]["tool"] = detect
    doc["act"] = [{"on": "opened", "tool": "site.notify", "arguments": {"text": "$summary"}}]
    doc["policy"]["allowed_tools"] = [detect, "site.notify"]
    doc["policy"]["timeout_ms"] = timeout_ms
    doc["sources"]["camera"].append("coverage")
    return doc


def events():
    return [json.loads(line) for line in (EXAMPLES/"person_detection"/"events.jsonl").read_text().splitlines()]


def message(kind, body, ident):
    return json.dumps({"v": 1, "type": kind, "id": ident, "body": body})


class Harness:
    def __init__(self, tmp_path):
        self.out, self.clock = [], [0]
        self.store = Store(tmp_path/"host.sqlite")
        self.host = Host(demo_registry(), self.store, self.out.append, clock=lambda: self.clock[0])

    async def settle(self):
        for _ in range(30):
            await asyncio.sleep(0)

    async def send(self, kind, body, ident="m"):
        await self.host.receive(message(kind, body, ident))
        await self.settle()

    def of(self, kind):
        return [m for m in self.out if m["type"] == kind]

    def reply(self, ident):
        return [m for m in self.out if m.get("reply_to") == ident]

    def operations(self, tool):
        return [r for r in self.store.operations("person_detection:1") if r["tool"] == tool]

    async def start(self, doc, **open_body):
        await self.send("hello", {"protocol": 1, "app_version": "test", "clock_source": "wall", "tools": [NOTIFY]}, "hello")
        await self.send("mission.open", {"mission": doc, **open_body}, "open")

    async def feed(self, items=None):
        for index, event in enumerate(items or events()):
            self.clock[0] = max(self.clock[0], event["observed_at_ms"])
            await self.send("event", {"event": event}, f"e{index}")


def test_host_tool_round_trip_reaches_the_decision(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        await h.start(mission())
        ready = h.of("ready")[0]
        assert ready["reply_to"] == "hello" and ready["body"]["protocol"] == 1 and ready["body"]["missions"] == []
        await h.feed()
        calls = h.of("tool.call")
        assert [c["body"]["tool"] for c in calls] == ["site.notify"]
        opened = [d["body"] for d in h.of("decision") if d["body"]["transition"] == "opened"]
        assert len(opened) == 1 and opened[0]["revision"] > 0
        await h.send("tool.result", {"call": calls[0]["body"]["call"], "status": "succeeded",
                                     "data": {"sent": True}, "receipt": "light-1"})
        assert [(r["status"], r["receipt"]) for r in h.operations("site.notify")] == [("succeeded", "light-1")]
        await h.host.close()
    asyncio.run(scenario())


def test_unanswered_host_tool_becomes_unknown_at_its_deadline(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        await h.start(mission(timeout_ms=100))
        await h.feed()
        await asyncio.sleep(0.3)
        await h.settle()
        notify = h.operations("site.notify")
        # Whichever of the timeout or the next drain's expiry comes first, the outcome is unknown.
        assert [r["status"] for r in notify] == ["unknown"]
        assert json.loads(notify[0]["result"])["reason"] in {"tool_failed_or_timed_out", "deadline_expired"}
        stale = h.of("tool.call")[0]["body"]["call"]
        await h.send("tool.result", {"call": stale, "status": "succeeded", "data": {"sent": True}}, "late")
        assert h.reply("late")[0]["body"]["code"] == "unknown_call"
        assert len(h.of("tool.call")) == 1
        await h.host.close()
    asyncio.run(scenario())


def test_slow_host_tool_does_not_hold_back_detection(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        await h.start(mission())
        later = events()
        for index, event in enumerate(later[2:5]):
            later.append({**event, "id": f"frame-late-{index}", "observed_at_ms": 9000 + 1000 * index})
        await h.feed(later)
        assert h.operations("site.notify")[0]["status"] == "running"
        assert all(r["status"] == "succeeded" for r in h.operations("fixture.detect"))
        await h.host.close()
    asyncio.run(scenario())


def test_invalid_results_are_refused_and_recorded_unknown(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        await h.start(mission())
        await h.feed()
        call = h.of("tool.call")[0]["body"]["call"]
        await h.send("tool.result", {"call": call, "status": "succeeded", "data": {"sent": "yes"}}, "bad")
        assert h.reply("bad")[0]["body"]["code"] == "invalid_contract"
        assert h.operations("site.notify")[0]["status"] == "unknown"
        await h.host.close()
    asyncio.run(scenario())


def test_malformed_messages_are_codes_and_never_stop_the_host(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        await h.send("event", {"event": events()[0]}, "early")
        await h.host.receive(json.dumps({"v": 2, "type": "hello", "id": "old", "body": {}}))
        await h.host.receive("{not json")
        await h.host.receive("x" * (MAX_MESSAGE_BYTES + 1))
        await h.host.receive("[" * 100000 + "]" * 100000)
        await h.send("hello", {"protocol": 1, "tools": [NOTIFY, NOTIFY]}, "twice")
        broken = {**NOTIFY, "name": "site.broken", "input_schema": {"type": 7}}
        await h.send("hello", {"protocol": 1, "tools": [NOTIFY, broken]}, "schema")
        await h.send("hello", {"protocol": 1, "tools": [NOTIFY]}, 7)
        await h.send("hello", {"protocol": 1, "tools": []}, "again")
        await h.send("teleport", {}, "odd")
        await h.send("mission.open", {"mission": "x"}, "string")
        await h.send("mission.open", {"mission": {"id": "broken"}}, "bad")
        await h.send("coverage", {"source": "camera", "from_ms": float("inf"), "to_ms": 1, "observed": False}, "inf")
        await h.send("mission.open", {"mission": mission()}, "open")
        await h.send("incident.clear", {"mission": "person_detection:1", "event_id": {"x": 1}}, "object")
        codes = [(m.get("reply_to"), m["body"]["code"]) for m in h.of("error")]
        assert codes == [("early", "hello_required"), ("old", "protocol_mismatch"), (None, "invalid_json"),
                         (None, "message_too_large"), (None, "invalid_message"), ("twice", "duplicate_tool"),
                         ("schema", "invalid_host_tools"), ("again", "already_greeted"), ("odd", "unknown_type"),
                         ("string", "invalid_mission"), ("bad", "invalid_contract"), ("inf", "invalid_number"),
                         ("object", "invalid_text")]
        assert h.reply(7)[0]["type"] == "ready"
        assert h.reply("open")[0]["type"] == "ack"
        await h.host.close()
    asyncio.run(scenario())


def test_clear_cancels_pending_actions_and_records_the_clear(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        doc = mission()
        doc["act"].append({"on": "opened", "tool": "site.notify", "arguments": {"text": "Second channel"}})
        doc["policy"]["max_concurrency"] = 1
        await h.start(doc)
        h.clock[0] = 5000
        await h.send("coverage", {"source": "camera", "from_ms": 4000, "to_ms": 5000, "observed": False,
                                  "reason": "harness_restart"}, "gap")
        assert h.reply("gap")[0]["body"]["results"] == [{"mission": "person_detection:1", "status": "admitted", "transition": None}]
        await h.send("coverage", {"source": "door", "from_ms": 4000, "to_ms": 5000, "observed": True}, "other")
        assert h.reply("other")[0]["body"]["results"] == []
        for index, event in enumerate(events()):
            h.clock[0] = max(h.clock[0], event["observed_at_ms"] + 5000)
            await h.send("event", {"event": {**event, "observed_at_ms": event["observed_at_ms"] + 5000}}, f"x{index}")
        assert any(d["body"]["transition"] == "opened" for d in h.of("decision"))
        pending_detection = [r["id"] for r in h.operations("fixture.detect") if r["status"] == "pending"]
        await h.send("incident.clear", {"mission": "person_detection:1"}, "clear")
        assert len(h.reply("clear")[0]["body"]["cancelled"]) == 1
        assert sorted(r["status"] for r in h.operations("site.notify")) == ["cancelled", "running"]
        assert pending_detection and all(r["status"] != "cancelled" for r in h.operations("fixture.detect"))
        assert h.of("decision")[-1]["body"]["transition"] == "cleared"
        assert h.host.runtimes["person_detection:1"].state["active"] is False
        await h.host.close()
    asyncio.run(scenario())


def test_reopen_replays_decisions_after_the_revision_the_app_has(tmp_path):
    async def scenario():
        first = Harness(tmp_path)
        await first.start(mission())
        await first.feed()
        opened = [d["body"]["revision"] for d in first.of("decision")]
        await first.host.close()
        first.store.close()
        second = Harness(tmp_path)
        second.clock[0] = first.clock[0]
        await second.start(mission(), after_revision=0)
        assert [d["body"]["revision"] for d in second.of("decision")] == opened
        assert second.reply("open")[0]["body"]["revision"] >= opened[-1]
        await second.host.close()
    asyncio.run(scenario())


def test_heartbeat_waits_for_hello_and_reports_health(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        beats = asyncio.create_task(h.host.heartbeats(10))
        await asyncio.sleep(0.05)
        assert h.of("heartbeat") == []
        await h.start(mission())
        await asyncio.sleep(0.05)
        body = h.of("heartbeat")[-1]["body"]
        assert body["missions"] == [{"mission": "person_detection:1", "pending": 0, "health": "unknown"}]
        beats.cancel()
        await h.host.close()
    asyncio.run(scenario())


def test_serve_survives_invalid_utf8_and_keeps_output_ascii(tmp_path):
    async def scenario():
        out = []
        store = Store(tmp_path/"bytes.sqlite")
        host = Host(demo_registry(), store, out.append)
        lines = [b"\xff\xfe broken\n", message("hello", {"protocol": 1, "app_version": "카메라"}, "hello").encode() + b"\n",
                 message("shutdown", {"timeout_ms": 100}, "bye").encode() + b"\n"]
        assert await serve(host, iter(lines)) == 0
        assert [m["type"] for m in out[:2]] == ["error", "ready"] and out[0]["body"]["code"] == "invalid_utf8"
        store.close()
    asyncio.run(scenario())


def read_until(process, kind, timeout=10):
    deadline = time.monotonic() + timeout
    seen = []
    while time.monotonic() < deadline:
        raw = process.stdout.readline()
        if not raw:
            break
        assert raw.isascii()
        seen.append(json.loads(raw))
        if seen[-1]["type"] == kind:
            return seen
    raise AssertionError(seen)


def test_killed_harness_never_resends_its_interrupted_call(tmp_path):
    database = tmp_path/"serve.sqlite"
    command = [sys.executable, "-m", "seer_agent", "serve", "--db", str(database)]
    first = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=ENV)
    now = time.time_ns() // 1_000_000
    frame = {**events()[2], "observed_at_ms": now}
    for line in [message("hello", {"protocol": 1, "tools": [NOTIFY, DETECT]}, "hello"),
                 message("mission.open", {"mission": mission(detect="app.detect")}, "open"),
                 message("event", {"event": frame}, "frame")]:
        first.stdin.write(line.encode() + b"\n")
    first.stdin.flush()
    call = read_until(first, "tool.call")[-1]["body"]["call"]
    busy = subprocess.run(command, input=b"", capture_output=True, timeout=30, env=ENV)
    assert busy.returncode == 2 and json.loads(busy.stdout)["body"]["code"] == "store_in_use"
    first.kill()
    first.wait(10)
    lines = [message("hello", {"protocol": 1, "tools": [NOTIFY, DETECT]}, "hello"),
             message("mission.open", {"mission": mission(detect="app.detect")}, "open"),
             message("shutdown", {"timeout_ms": 500}, "bye")]
    second = subprocess.run(command, input=("\n".join(lines) + "\n").encode(), capture_output=True, timeout=30, env=ENV)
    assert second.returncode == 0, second.stderr
    out = [json.loads(line) for line in second.stdout.splitlines()]
    assert [m["reply_to"] for m in out if m["type"] == "ack"] == ["open", "bye"]
    assert not [m for m in out if m["type"] == "tool.call"]
    store = Store(database)
    row = [r for r in store.operations("person_detection:1") if r["id"] == call][0]
    assert (row["status"], json.loads(row["result"])["reason"]) == ("unknown", "harness_restarted")
    store.close()


def test_hello_registers_a_lan_webhook_the_mission_can_use(tmp_path):
    import http.server
    import threading
    received = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    async def scenario():
        h = Harness(tmp_path)
        h.host.allow_external = True
        hook = {"kind": "webhook", "name": "site.webhook", "endpoint": f"http://127.0.0.1:{server.server_port}/alert",
                "allow_private": True, "require_receipt": False}
        await h.send("hello", {"protocol": 1, "integrations": [{**hook, "endpoint": "http://plc.lan/alert"}]}, "bad")
        assert h.reply("bad")[0]["body"]["code"] == "https_required"
        await h.send("hello", {"protocol": 1, "tools": [NOTIFY], "integrations": [hook]}, "hello")
        doc = mission()
        doc["act"] = [{"on": "opened", "tool": "site.webhook", "arguments": {"text": "$summary"}}]
        doc["policy"]["allowed_tools"] = ["fixture.detect", "site.webhook"]
        doc["policy"]["allow_external"] = True
        await h.send("mission.open", {"mission": doc}, "open")
        await h.feed()
        for _ in range(100):
            if received:
                break
            await asyncio.sleep(0.02)
        await h.settle()
        assert [r["status"] for r in h.operations("site.webhook")] == ["accepted"]
        assert received[0]["text"] == "Person presence detected in the monitored region."
        await h.host.close()
    try:
        asyncio.run(scenario())
    finally:
        server.shutdown()
