# Integrations

Tools use one async contract regardless of whether they observe or act. The runtime owns authorization, durable intent, timeouts and outcomes. Adapters own provider-specific serialization and authentication.

| Adapter | Factory | Result |
| --- | --- | --- |
| In-process detector/model | `function_tool` | Schema-validated observations |
| User-hosted model service | `model_endpoint` | JSON observation data |
| Telegram bot | `telegram` | Accepted message receipt |
| WhatsApp through Twilio | `twilio(mode="whatsapp")` | Accepted message SID |
| Telephone through Twilio | `twilio(mode="voice")` | Accepted call SID |
| Other service | `webhook` or custom `Tool` | Provider receipt or typed result |

These are implemented adapters, tested with injected fake transports. No real service, recipient, camera or model was used for this implementation. Additional providers can register tools without editing the runtime.

```python
from seer_agent.builtins import builtin_registry
from seer_agent.integrations import telegram, twilio, webhook

registry = builtin_registry()
registry.tool(telegram(name="notify.telegram", chat_id="HOST_CONFIGURED_CHAT"))
registry.tool(twilio(name="notify.whatsapp", mode="whatsapp",
                    sender="HOST_CONFIGURED_NUMBER", recipient="HOST_CONFIGURED_RECIPIENT",
                    content_sid="HX" + "0" * 32))  # Replace with your approved template ID.
registry.tool(twilio(name="call.operator", mode="voice",
                    sender="HOST_CONFIGURED_NUMBER", recipient="HOST_CONFIGURED_RECIPIENT"))
registry.tool(webhook(name="notify.service", endpoint="https://example.invalid/seer"))
```

Registration makes no requests. Tokens are read at execution from `TELEGRAM_BOT_TOKEN`, `TWILIO_ACCOUNT_SID` and `TWILIO_AUTH_TOKEN`, or explicitly configured environment names. Authorize the desired tool ID in the Mission, select it under `act`, and enable external effects in both Mission and host before any live execution. Run replay with the local `record` tool.

Notification arguments contain `text`. Mission arguments may use exact values `$summary`, `$mission_id`, `$event_id` and `$evidence`; no expression evaluation is performed. Recipients and endpoint addresses are fixed in host setup. Webhooks include a stable `Idempotency-Key`; the receiving service must implement deduplication for it to guarantee anything.

Telegram uses [`sendMessage`](https://core.telegram.org/bots/api#sendmessage) and records the returned message ID as acceptance. WhatsApp notifications use an [approved template](https://www.twilio.com/docs/whatsapp/tutorial/send-whatsapp-notification-messages-templates) with `ContentSid` and one text variable. Phone calls use the [Twilio Call resource](https://www.twilio.com/docs/voice/api/call-resource) and escaped inline TwiML. These APIs were consulted on October 16, 2026. A successful submission does not prove that a recipient read a message or answered a call.

The generic model endpoint receives `operation_id`, the selected `event`, and fixed `parameters`, and returns the declared output object directly. It is an adapter contract for a user-owned service, not compatibility with every vendor API. Raw video is not stored in the harness database; the camera/media host controls frame references, retention and access. Runtime credentials, URLs containing secrets and raw provider errors must not be copied into event data.
