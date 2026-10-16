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

The native Seer App currently retains its previous C++ runtime. Moving its directory does not connect it to this Python runtime. A future App bridge must preserve approval gates, source identities, state and outbox semantics before switching execution.
