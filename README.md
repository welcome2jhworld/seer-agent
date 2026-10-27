# Seer Agent

Seer Agent is a Python perception harness for deciding what to observe, what to remember and when to act. A versioned mission connects multiple sources, observation tools, a stateful condition, scoped memory and permitted actions.

The public package contains the runtime, CLI, extension interfaces and two synthetic examples. It has no desktop, Qt, camera codec, model weights, account or private-service dependency. The Python API is experimental (`0.2.0a1`); historical C++ journals are not migrated automatically.

## Start

Requires Python 3.11 or later.

```sh
git clone https://github.com/welcome2jhworld/seer-agent.git
cd seer-agent
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
seer-agent run --mission examples/person_detection/mission.json \
  --input examples/person_detection/events.jsonl \
  --db runs/person.sqlite --demo --replay
seer-agent inspect --db runs/person.sqlite --mission person_detection:1
seer-agent tools --demo
```

On Windows, activate `.venv\Scripts\Activate.ps1` in PowerShell. The examples make no network requests and need no models or credentials.

A host app runs many missions in one process with `seer-agent serve --db PATH`. It speaks host protocol v1 as JSON Lines on stdin and stdout; see [docs/runtime.md](docs/runtime.md#host-protocol-v1).

## Observe, remember, act

| Mission component | Responsibility |
| --- | --- |
| `sources` | Explicit source identities and accepted event kinds |
| `observe` | Select inputs for an observation tool and declare its result kind |
| `remember` | Bound recalled context by source, age and number of records |
| `condition` | Maintain a typed interpretation and identify transitions |
| `act` | Map transitions to registered tools with fixed, validated arguments |
| `policy` | Limit tool authority, external effects, backlog, concurrency and time |

Events preserve source, event time, provenance and evidence references. The host records availability time. Camera frames are references, documents and messages are data, and failed observations remain unknown. Input text cannot rewrite a mission or install tools.

State changes and tool intents commit together before execution. SQLite stores history, bounded-query memory and the durable outbox. A restarted operation whose effect is uncertain remains unknown; the runtime does not blindly resend a message or repeat a phone call. Provider acceptance is recorded separately from verified completion.

## Examples

- [Person detection](examples/person_detection/README.md): camera observations, a site document and operator chat inform region-presence monitoring.
- [Suspected oil leak](examples/oil_leak/README.md): persistent visible change, maintenance context and an oil-level sensor inform a review request.

Both examples run through the same runtime and tool contract. They demonstrate workflow execution with synthetic evidence, not detection accuracy. Their README files describe how changing relevant context changes the result and how to inspect original receipts after restart.

## Extend

Register async Python tools with JSON Schema arguments and results. A tool can run a detector, query a business system, ask for another observation or send a notification. Optional [integrations](docs/integrations.md) include a model endpoint, Telegram, WhatsApp through Twilio, Twilio Voice and a generic webhook. In-process models can use `function_tool` without modifying the runtime.

Source adapters can feed camera frame references, documents, chat and other service events. `sources.merge` combines async streams with a bounded queue. Custom conditions implement `evaluate(event, state, memory) -> Evaluation`. Explicitly named installed plugins register tools and conditions through the `seer_agent.plugins` entry-point group. See [extension contracts](docs/extensions.md) and the [installed plugin example](examples/person_detection/plugin/README.md).

The runtime is a single-machine alpha. It provides bounded concurrent tool dispatch, not a distributed task service. Mission configuration is explicit; natural-language workflow compilation and general autonomous planning are not implemented. Raw audit history is retained; `remember` limits recall, not total disk usage. See [runtime semantics](docs/runtime.md).

## Development

```sh
python -m pip install -e . pytest build
python -m pytest -q
python -m build
```

Tests cover synthetic workflows, scoped context, interrupted execution, idempotency, ordering, unknown outcomes, custom extensions and provider request formats with fake transports. No real model, camera, notification or telephone service is exercised. Source publication, hosted deployment and binary distribution are separate operations.
