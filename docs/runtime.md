# Runtime semantics

## Authority and definitions

A Mission has a stable identity and version. SQLite pins its complete JSON definition, condition version and tool fingerprints. Reopening the same version with different arguments, recipients, endpoint configuration or tool versions fails. In-place mission mutation also fails. Trusted plugin authors must increment implementation versions when semantics change.

Every external action requires both mission policy and the host's `allow_external=True`. The CLI adds this authority only through `--allow-external`; replay cannot enable it. Registering an integration is not a request to send. Recipients, endpoints and credential environment names belong to trusted host configuration, not events. A read-only model service may transmit its selected input: hosts must explicitly configure and authorize that tool and its data destination.

## Time and memory

Event time describes the source observation. Availability time is assigned by the host. Future and stale events are retained as rejected audit records and cannot affect conditions or tool dispatch. Duplicate identity with identical content is a no-op; conflicting content fails. IDs must include the source/session or another stable upstream identity.

Memory is isolated by mission version and selected source IDs. Queries are bounded by host time, age and record count. The total record budget is divided into per-source slots so camera traffic cannot evict every document or sensor record. A tool receives a pinned snapshot selected when its intent committed. Current conditions see context available at evaluation time. Document/chat/sensor context is matched by area; a newer unknown context supersedes an older known value. Late condition observations remain history-only. Late oil baselines cannot invalidate a newer baseline.

Raw events and tool-produced evidence retain separate identities. Observation tools run for explicitly matched raw inputs; their output is validated and enters the condition path once, never recursively invoking an observer. Source event revisions determine application order, even when a batch has one host timestamp or tools finish out of order.

An action binding can set `emits` to send its outcome into the condition path. That event contains operation ID, tool, status, result and receipt. A custom condition can use it to select a fallback or request human review. An unknown outcome is a known fact about execution uncertainty, not evidence that the underlying effect failed. `max_tool_depth` bounds outcome-driven chains; budget denial stops further result emission. No provider-specific fallback is automatic.

`remember` bounds recalled context, not retained audit bytes. There is no automatic pruning of event, decision or action history in this alpha. Plan disk capacity and rotate to a new explicitly managed store only after preserving active state and unresolved operations. Disk-full and power-loss behavior have not been validated on physical hardware.

## Effects and recovery

State transitions, decisions and pending operations are one transaction. Operations use an identity derived from mission version, triggering event, role, binding and tool name. Tool arguments and descriptor fingerprints are immutable. SQLite atomically claims pending operations, including across two connections. Operation history records claims, outcomes, expiry and explicit resolution.

Pending work can be cancelled before claim. Claimed work has a fixed deadline. Timeout, cancellation, invalid provider output or a crash after claim produces an unknown outcome when resolved or expired; this does not establish that the external service stopped. There is no automatic resend. `Store.resolve` accepts a trusted host's independently verified outcome for an accepted or unknown action and records the verification receipt without dispatching again.

Accepted means the provider accepted a request. It is not delivery, a human acknowledgment or incident resolution. Verified resolution creates a new immutable outcome event when the action declares `emits`; its earlier accepted/unknown event remains unchanged. Provider callbacks/polling and their signature verification belong to the integrating host. This alpha supplies an explicit resolution seam, not an inbound public webhook server.

Outcome history retains the provider acceptance receipt and later verification receipt separately, even without an emitted event. A definitive late result can update an expired unknown operation without redispatch. Concurrent resolution cannot mark a newer outcome as consumed while applying an earlier one.

Observation results are committed before conversion into condition evidence. If the process stops between those steps, `flush()` applies the saved result after reopening. Errors and invalid results become unknown evidence. Unknown observations break continuous timing but never clear an active incident. Camera adapters must emit unknown on outages and on process/session discontinuities; timestamps alone do not establish uninterrupted camera coverage.

## Scheduling and scale

`ingest` commits bounded work synchronously. `drain` executes at most `max_concurrency` pending operations asynchronously, with running capacity enforced atomically across connections; `flush` drains currently eligible work. Durable FIFO ordering prevents new camera work from starving an older action. Applications can ingest while tools await I/O. Async integrations must yield promptly; use a worker or `asyncio.to_thread` for blocking SDKs. Cancelling a thread-backed request cannot undo its remote effect.

Input envelopes are limited to 256 KiB and tool result data to 64 KiB, leaving room for provenance and execution metadata. Oversized or invalid model/provider output becomes a bounded unknown outcome and cannot block the queue permanently.

Pending capacity is checked inside the ingestion transaction. Backpressure rolls back the event, state and intents together, allowing the host to drain and retry the unchanged event. Derived results wait for capacity while existing operations continue draining. Impossible action fan-out is rejected in configuration. `sources.merge` bounds upstream fan-in. SQLite is a single-machine persistence backend; distributed workers, leases across machines and a durable cross-host source bus are not supplied.

## Host protocol v1

`seer-agent serve --db PATH` holds many missions with one registry and one store, and takes an exclusive lock on the store; a second `serve` on the same store exits with `store_in_use`. The host app writes UTF-8 JSON Lines to stdin and reads ASCII JSON Lines from stdout; logs never go to stdout and there is no network port. Every message has `v` (1), `type`, `id` and `body`. Replies carry `reply_to` with the request `id`. A message is at most 256 KiB. A line that is not UTF-8, not JSON or not a valid request gets an `error` with a code and the session continues.

| From the host | Body | Reply |
|---|---|---|
| `hello` | `protocol`, `app_version`, `clock_source`, `tools` (host tool specs), `integrations` (harness-run senders) | `ready` with `harness_version` and the missions in the store |
| `mission.open` | `mission` (versioned document), optional `after_revision` | `ack` with the mission state and latest decision `revision`, then every decision after `after_revision` |
| `mission.close` | `mission` (identity and version) | `ack` |
| `event` | `event` (Event envelope) | `ack` with a result per mission that lists the source |
| `coverage` | `source`, `from_ms`, `to_ms`, `observed`, `reason` | `ack`; missions that list `coverage` for the source receive a `coverage` event, unknown when not observed |
| `tool.result` | `call`, `status`, `data`, `receipt` | `error` only: `unknown_call`, `call_expired`, or a contract code for a result that does not match the tool (recorded as unknown) |
| `incident.clear` | `mission`, optional `event_id` | `ack` with the cancelled pending actions; an active incident records a `cleared` decision |
| `shutdown` | optional `timeout_ms` | `ack` after pending work drains or the timeout passes |

After `ready`, the harness sends `heartbeat` every 2 s with pending work and health per mission, `tool.call` (`call`, `tool`, `mission`, `arguments`, `event`, `deadline_ms`) for host tools, `decision` (with its `revision`) for each condition transition and `error` with a `code`. Error messages never echo input.

Host tools declared at `hello` are proxies in the registry and are pinned in mission fingerprints like any other tool. A `hello` with any invalid or duplicate tool registers none of them. The runtime's deadline applies to host tools: a call without a valid `tool.result` before the deadline becomes an unknown outcome, and a late result is refused. A tool call that would exceed the message limit fails without being sent. Tools run outside the dispatch lock, so a slow host tool does not hold back other work of the mission up to `max_concurrency`. Heartbeats also drain each mission, so deadlines expire without new events.

An integration is `{"kind": "webhook", "name", "endpoint", "allow_private", "require_receipt"}`. The harness sends it itself, so a webhook to a LAN system works without internet. Plain HTTP is accepted only for literal private, loopback or link-local addresses with `allow_private`; host names need HTTPS. With `require_receipt` false, any 2xx answer is accepted and the receipt records the status and operation identity. Webhooks are external effects and need `--allow-external` and mission policy.

The host decides which missions run: after a restart it sends `mission.open` again, with `after_revision` set to the last decision it applied so none is lost. Work the previous process left running is marked unknown with reason `harness_restarted` and is never sent again.

`incident.clear` cancels pending actions only; observation work continues. It sets the mission state's `active` to false, which the bundled conditions use for an open incident.

The native Seer App still runs its previous C++ runtime. Connecting it to `serve` is the bridge phase of the runtime convergence plan; it must preserve approval gates, source identities, state and outbox semantics before execution moves.
