# Python perception harness

Assigned October 16, 2026. The owner selected a Python-first runtime. No service deployment, binary release, package publication or notarization is assigned.

## Product boundary

`seer_agent` is the only intended open-source product. `seer_app` and `seer_backend` are separate private-product repositories. The existing app implementation and history remain intact; the directory migration does not replace its C++ runtime. The landing site and research remain outside these three folders.

## Runtime

A Mission specifies what to observe, what context to remember, which condition to maintain and which tools may act. Source adapters emit versioned events for camera frames, documents, messages and integration data. Optional observation tools convert selected events into typed evidence. Conditions receive a bounded, time-scoped memory view and return state transitions. The runtime commits observations, state and tool intents atomically before dispatch.

One SQLite store owns observation history, mission state and the outbox. Tool operations have stable IDs, pinned tool descriptors, immutable arguments, deadlines and explicit succeeded/accepted/failed/unknown outcomes. Unknown external effects are not automatically retried. An accepted provider request is not confirmed delivery. Tools are asynchronous Python integrations with JSON Schema inputs/outputs; conditions and sources are also extensible. Registration is explicit trusted host code or a named installed entry point, never content supplied by an observation.

Memory keeps provenance and host availability time. Decisions can only see admitted earlier revisions. Documents/chat supply contextual facts, not policy changes. A tool result enters the observation path as new evidence with its own availability time. No model, UI, account or private runtime is needed for the included examples.

## Acceptance

1. Install the public package independently and run both CLI examples.
2. Person detection combines a synthetic camera stream, a site document and operator messages. Oil-leak monitoring combines synthetic visible-change evidence, maintenance context and a sensor input; it reports suspected leakage requiring review, not verified oil identity.
3. Custom detection, Telegram, WhatsApp, telephone and webhook tools share one contract. Live transports are implemented but tested only with injected fake responses.
4. Restart, duplicate/conflicting input, failed observation, policy denial, timeout, unknown external outcome and concurrent action claim tests pass.
5. A third-party condition/tool can be added without editing the runtime. Public build artifacts contain only the harness.

## Sequence

Repository boundaries; contracts and storage; observation/condition/action runtime; integrations and sources; CLI examples; failure tests and installed-package verification; independent review and documentation.

## Limits

This is a new Python alpha API, not an automatic migration of historical C++ journals. Single-machine SQLite, bounded asynchronous dispatch and explicit integrations are the initial scale. Distributed queues, automatic natural-language workflow compilation, generic autonomous planning, physical-camera/model accuracy and authenticated inbound service webhooks need separate implementation or validation. Credentials and live recipients remain host configuration.
