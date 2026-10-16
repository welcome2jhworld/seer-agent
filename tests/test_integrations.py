import asyncio
from dataclasses import replace
import json
from urllib.parse import parse_qs

import pytest

from seer_agent import Event, MemoryView, ToolContext
from seer_agent.builtins import DETECTIONS
from seer_agent.integrations import model_endpoint, telegram, twilio, webhook
from seer_agent.integrations.http import Response


class Transport:
    def __init__(self, response):
        self.response, self.requests = response, []
    async def post(self, url, headers, body, *, timeout):
        self.requests.append((url, headers, body))
        return self.response


def context():
    return ToolContext("operation-1", "mission:1", Event("frame-1", "camera", "camera.frame", 1000,
                       {"frame_ref": "local://frame-1"}), MemoryView([]), 10000)


def execute(tool):
    result = asyncio.run(tool.execute(context(), {"text": "Suspected leak <check>"}))
    result.validate(tool.spec)
    return result


def test_telegram_acceptance_is_not_delivery(monkeypatch):
    monkeypatch.setenv("TEST_TOKEN", "123:token")
    transport = Transport(Response(200, {"ok": True, "result": {"message_id": 42}}))
    tool = telegram(name="notify.telegram", chat_id="room", token_env="TEST_TOKEN", transport=transport)
    result = execute(tool)
    assert result.status == "accepted" and result.receipt == "room:42"
    assert json.loads(transport.requests[0][2]) == {"chat_id": "room", "text": "Suspected leak <check>"}
    assert "token" not in json.dumps(tool.spec.__dict__)


@pytest.mark.parametrize("data", [{"ok": True, "result": {}}, {"ok": True, "result": "bad"}, {"ok": False}, {"ok": True, "result": {"message_id": True}}])
def test_telegram_malformed_response_remains_unknown(monkeypatch, data):
    monkeypatch.setenv("TEST_TOKEN", "123:token")
    assert execute(telegram(name="telegram", chat_id="room", token_env="TEST_TOKEN", transport=Transport(Response(200, data)))).status == "unknown"


@pytest.mark.parametrize("mode,resource,prefix", [("voice", "Calls", "CA"), ("whatsapp", "Messages", "SM")])
def test_twilio_request_and_receipt(monkeypatch, mode, resource, prefix):
    monkeypatch.setenv("ACCOUNT", "AC"+"0"*32)
    monkeypatch.setenv("TOKEN", "secret")
    transport = Transport(Response(201, {"sid": prefix+"1"*32, "status": "queued"}))
    tool = twilio(name="contact", mode=mode, sender="+10000000000", recipient="+10000000001",
                  account_env="ACCOUNT", token_env="TOKEN", content_sid="HX"+"2"*32 if mode == "whatsapp" else None,
                  transport=transport)
    result = execute(tool)
    assert result.status == "accepted"
    url, headers, body = transport.requests[0]
    assert url.endswith(resource+".json")
    form = parse_qs(body.decode())
    if mode == "voice":
        assert "&lt;check&gt;" in form["Twiml"][0]
    else:
        assert form["To"] == ["whatsapp:+10000000001"]
        assert form["ContentSid"] == ["HX"+"2"*32]
    assert "secret" not in json.dumps(tool.spec.__dict__)


def test_webhook_uses_stable_operation_identity():
    transport = Transport(Response(202, {"receipt": "request-1"}))
    result = execute(webhook(name="notify", endpoint="https://example.invalid/hook", transport=transport))
    assert result.status == "accepted"
    assert transport.requests[0][1]["Idempotency-Key"] == "operation-1"


def test_model_tool_retains_source_event_and_validates_output():
    transport = Transport(Response(200, {"detections": []}))
    tool = model_endpoint(name="detect", endpoint="https://example.invalid/model", output_schema=DETECTIONS, transport=transport)
    result = asyncio.run(tool.execute(context(), {}))
    result.validate(tool.spec)
    assert result.status == "succeeded" and result.data == {"detections": []}
    assert json.loads(transport.requests[0][2])["event"]["id"] == "frame-1"


def test_http_errors_do_not_look_like_success(monkeypatch):
    monkeypatch.setenv("TEST_TOKEN", "123:token")
    tool = telegram(name="telegram", chat_id="room", token_env="TEST_TOKEN", transport=Transport(Response(500, {})))
    assert execute(tool).status == "unknown"
