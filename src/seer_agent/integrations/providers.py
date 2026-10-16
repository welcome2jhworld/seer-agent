"""Optional host-configured adapters. Tokens are kept outside missions and journals."""
from __future__ import annotations

import base64
import os
import re
from urllib.parse import urlencode
from xml.sax.saxutils import escape

from ..contracts import ContractError, TEXT, object_schema
from ..tools import Tool, ToolResult, ToolSpec
from .http import HttpTransport, json_bytes


TEXT_INPUT = object_schema({"text": {"type": "string", "minLength": 1, "maxLength": 4000}})
ACCEPTED = object_schema({"provider": TEXT, "state": TEXT})


def secret(environment: str) -> str:
    value = os.environ.get(environment, "")
    if not value:
        raise ContractError("provider_credentials_missing")
    return value


def telegram(*, name: str, chat_id: str, token_env: str = "TELEGRAM_BOT_TOKEN", transport=None) -> Tool:
    transport = transport or HttpTransport()
    async def execute(context, arguments):
        token = secret(token_env)
        if not re.fullmatch(r"[0-9]+:[A-Za-z0-9_-]+", token):
            raise ContractError("invalid_telegram_credential")
        result = await transport.post(f"https://api.telegram.org/bot{token}/sendMessage",
            {"Content-Type": "application/json"}, json_bytes({"chat_id": chat_id, "text": arguments["text"]}), timeout=10)
        message = result.data.get("result", {})
        message_id = message.get("message_id") if isinstance(message, dict) else None
        if result.status == 200 and result.data.get("ok") is True and type(message_id) is int and message_id > 0:
            return ToolResult("accepted", {"provider": "telegram", "state": "accepted"}, f"{chat_id}:{message_id}")
        return ToolResult("unknown", {"reason": "provider_did_not_confirm_acceptance"})
    return Tool(ToolSpec(name, "1", "external", TEXT_INPUT, ACCEPTED,
                         {"provider": "telegram", "chat_id": chat_id, "credential_env": token_env}), execute)


def twilio(*, name: str, mode: str, sender: str, recipient: str,
           account_env: str = "TWILIO_ACCOUNT_SID", token_env: str = "TWILIO_AUTH_TOKEN",
           content_sid: str | None = None, transport=None) -> Tool:
    if mode not in {"whatsapp", "voice"}:
        raise ContractError("invalid_twilio_mode")
    if mode == "whatsapp" and (not content_sid or not re.fullmatch(r"HX[0-9a-fA-F]{32}", content_sid)):
        raise ContractError("whatsapp_requires_approved_template")
    transport = transport or HttpTransport()
    async def execute(context, arguments):
        account, token = secret(account_env), secret(token_env)
        if not re.fullmatch(r"AC[0-9a-fA-F]{32}", account):
            raise ContractError("invalid_account_sid")
        credential = base64.b64encode(f"{account}:{token}".encode()).decode()
        if mode == "whatsapp":
            form = {"From": f"whatsapp:{sender}", "To": f"whatsapp:{recipient}", "ContentSid": content_sid,
                    "ContentVariables": json_bytes({"1": arguments["text"]}).decode()}
            resource, prefix = "Messages", "SM"
        else:
            form = {"From": sender, "To": recipient,
                    "Twiml": f"<Response><Say>{escape(arguments['text'])}</Say></Response>"}
            resource, prefix = "Calls", "CA"
        result = await transport.post(f"https://api.twilio.com/2010-04-01/Accounts/{account}/{resource}.json",
            {"Authorization": f"Basic {credential}", "Content-Type": "application/x-www-form-urlencoded"},
            urlencode(form).encode(), timeout=10)
        sid, status = result.data.get("sid"), result.data.get("status")
        if result.status in {200, 201} and isinstance(sid, str) and re.fullmatch(prefix+r"[0-9a-fA-F]{32}", sid) and isinstance(status, str):
            return ToolResult("accepted", {"provider": "twilio", "state": status}, sid)
        return ToolResult("unknown", {"reason": "provider_did_not_confirm_acceptance"})
    return Tool(ToolSpec(name, "1", "external", TEXT_INPUT, ACCEPTED,
        {"provider": "twilio", "mode": mode, "sender": sender, "recipient": recipient,
         "content_sid": content_sid, "account_env": account_env, "credential_env": token_env}), execute)


def webhook(*, name: str, endpoint: str, transport=None, token_env: str | None = None) -> Tool:
    transport = transport or HttpTransport()
    async def execute(context, arguments):
        headers = {"Content-Type": "application/json", "Idempotency-Key": context.operation_id}
        if token_env:
            headers["Authorization"] = "Bearer "+secret(token_env)
        result = await transport.post(endpoint, headers,
            json_bytes({"operation_id": context.operation_id, "mission_id": context.mission_id, **arguments}), timeout=10)
        receipt = result.data.get("receipt")
        if 200 <= result.status < 300 and isinstance(receipt, str) and receipt:
            return ToolResult("accepted", {"provider": "webhook", "state": "accepted"}, receipt)
        return ToolResult("unknown", {"reason": "provider_did_not_confirm_acceptance"})
    return Tool(ToolSpec(name, "1", "external", TEXT_INPUT, ACCEPTED,
                         {"endpoint": endpoint, "credential_env": token_env}), execute)


def model_endpoint(*, name: str, endpoint: str, output_schema: dict,
                   token_env: str | None = None, transport=None) -> Tool:
    """Adapt a user-owned detector/VLM service returning a typed JSON object."""
    transport = transport or HttpTransport()
    async def execute(context, arguments):
        headers = {"Content-Type": "application/json"}
        if token_env:
            headers["Authorization"] = "Bearer "+secret(token_env)
        result = await transport.post(endpoint, headers, json_bytes({"operation_id": context.operation_id,
            "event": context.event.to_dict(), "parameters": arguments}), timeout=10)
        if result.status != 200:
            return ToolResult("unknown", {"reason": "model_unavailable"})
        return ToolResult("succeeded", result.data)
    # Read tools may upload data. Endpoint registration is an explicit host decision.
    return Tool(ToolSpec(name, "1", "read", {"type": "object"}, output_schema,
                         {"endpoint": endpoint, "credential_env": token_env, "sends_data": True}), execute)
