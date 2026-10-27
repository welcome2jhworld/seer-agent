import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys

from seer_agent import Store
from seer_agent.builtins import DETECTIONS, demo_registry
from seer_agent.contracts import TEXT, object_schema
from seer_agent.host import Host, MAX_MESSAGE_BYTES

EXAMPLES = Path(__file__).parents[1] / "examples"
NOTIFY = {"name": "site.notify", "version": "1", "effect": "local",
          "input_schema": object_schema({"text": TEXT}), "output_schema": object_schema({"sent": {"type": "boolean"}})}
DETECT = {"name": "app.detect", "version": "1", "effect": "read", "input_schema": object_schema({}),
          "output_schema": DETECTIONS}


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

    async def send(self, kind, body, ident="m"):
        await self.host.receive(message(kind, body, ident))
        for _ in range(20):
            await asyncio.sleep(0)

    def of(self, kind):
        return [m for m in self.out if m["type"] == kind]

    async def start(self, doc):
        await self.send("hello", {"protocol": 1, "app_version": "test", "clock_source": "wall", "tools": [NOTIFY]}, "hello")
        await self.send("mission.open", {"mission": doc}, "open")

    async def feed(self):
        for index, event in enumerate(events()):
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
        assert any(d["body"]["transition"] == "opened" for d in h.of("decision"))
        await h.send("tool.result", {"call": calls[0]["body"]["call"], "status": "succeeded",
                                     "data": {"sent": True}, "receipt": "light-1"})
        await h.host.close()
        rows = h.store.operations(h.host.runtimes["person_detection:1"].key)
        notify = [r for r in rows if r["tool"] == "site.notify"]
        assert [(r["status"], r["receipt"]) for r in notify] == [("succeeded", "light-1")]
    asyncio.run(scenario())


def test_unanswered_host_tool_becomes_unknown_without_resend(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        await h.start(mission(timeout_ms=100))
        await h.feed()
        await asyncio.sleep(0.3)
        for _ in range(20):
            await asyncio.sleep(0)
        await h.host.close()
        notify = [r for r in h.store.operations("person_detection:1") if r["tool"] == "site.notify"]
        assert [r["status"] for r in notify] == ["unknown"]
        assert len(h.of("tool.call")) == 1
        stale = h.of("tool.call")[0]["body"]["call"]
        await h.send("tool.result", {"call": stale, "status": "succeeded", "data": {"sent": True}}, "late")
        assert h.of("error")[-1]["body"]["code"] == "unknown_call"
    asyncio.run(scenario())


def test_restart_keeps_state_and_never_resends_an_interrupted_call(tmp_path):
    async def scenario():
        first = Harness(tmp_path)
        await first.start(mission())
        await first.feed()
        assert len(first.of("tool.call")) == 1
        state = first.host.runtimes["person_detection:1"].state
        await first.host.close()
        first.store.close()
        second = Harness(tmp_path)
        second.clock[0] = first.clock[0]
        await second.start(mission())
        assert second.of("ready")[0]["body"]["missions"] == ["person_detection:1"]
        assert [m for m in second.of("ack") if m.get("reply_to") == "open"][0]["body"]["state"] == state
        await asyncio.sleep(0.05)
        assert second.of("tool.call") == []
        notify = [r for r in second.store.operations("person_detection:1") if r["tool"] == "site.notify"]
        assert [r["status"] for r in notify] == ["unknown"]
        await second.host.close()
    asyncio.run(scenario())


def test_protocol_errors_are_codes_without_details(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        await h.send("event", {"event": events()[0]}, "early")
        await h.host.receive(json.dumps({"v": 2, "type": "hello", "id": "old", "body": {}}))
        await h.host.receive("{not json")
        await h.host.receive("x" * (MAX_MESSAGE_BYTES + 1))
        await h.send("hello", {"protocol": 1, "tools": []}, "hello")
        await h.send("hello", {"protocol": 1, "tools": []}, "again")
        await h.send("teleport", {}, "odd")
        await h.send("mission.open", {"mission": {"id": "broken"}}, "bad")
        codes = [(m.get("reply_to"), m["body"]["code"]) for m in h.of("error")]
        assert codes == [("early", "hello_required"), ("old", "protocol_mismatch"), (None, "invalid_json"),
                         (None, "message_too_large"), ("again", "already_greeted"), ("odd", "unknown_type"),
                         ("bad", "invalid_contract")]
        await h.host.close()
    asyncio.run(scenario())


def test_coverage_reaches_only_missions_that_listen_and_clear_cancels_pending(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        await h.start(mission())
        h.clock[0] = 5000
        await h.send("coverage", {"source": "camera", "from_ms": 4000, "to_ms": 5000, "observed": False,
                                  "reason": "harness_restart"}, "gap")
        ack = [m for m in h.of("ack") if m.get("reply_to") == "gap"][0]
        assert ack["body"]["results"] == [{"mission": "person_detection:1", "status": "admitted", "transition": None}]
        await h.send("coverage", {"source": "door", "from_ms": 4000, "to_ms": 5000, "observed": True}, "other")
        assert [m for m in h.of("ack") if m.get("reply_to") == "other"][0]["body"]["results"] == []
        event = events()[2]
        event["observed_at_ms"] = h.clock[0] = 6000
        await h.host.receive(message("event", {"event": event}, "frame"))
        await h.host.receive(message("incident.clear", {"mission": "person_detection:1"}, "clear"))
        cleared = [m for m in h.of("ack") if m.get("reply_to") == "clear"][0]
        assert len(cleared["body"]["cancelled"]) == 1
        await h.host.close()
    asyncio.run(scenario())


def test_serve_speaks_json_lines_over_stdio(tmp_path):
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).parents[1]/"src")}
    lines = [message("hello", {"protocol": 1, "tools": [NOTIFY, DETECT]}, "hello"),
             message("mission.open", {"mission": mission(detect="app.detect")}, "open"),
             message("shutdown", {"timeout_ms": 1000}, "bye")]
    result = subprocess.run([sys.executable, "-m", "seer_agent", "serve", "--db", str(tmp_path/"serve.sqlite")],
                            input="\n".join(lines) + "\n", capture_output=True, text=True, timeout=30, env=env)
    assert result.returncode == 0, result.stderr
    out = [json.loads(line) for line in result.stdout.splitlines()]
    assert {m["type"] for m in out} >= {"ready", "ack", "heartbeat"}
    assert [m["reply_to"] for m in out if m["type"] == "ack"] == ["open", "bye"], out
    assert all(m["v"] == 1 for m in out)
