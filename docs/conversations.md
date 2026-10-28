# Conversation contracts

Three versioned contracts back the app's conversation screens: the task plan card, Ask on Live and Analysis. `seer-agent contracts` prints every schema with its version, and the host's `ready` message lists the versions (`contracts`), so the app can refuse a harness it does not understand.

The harness applies the rules that need no model: it checks requests, builds the free tier's plan cards, decides what Ask may look at, computes search coverage and the indexing offer, and checks every model candidate. A model only fills content. A candidate that breaks a rule becomes a typed failure, never a partial card. Fixed sentences carry a stable `code` with English `text`, so the app translates by code. Times are epoch milliseconds; the app formats them in the site's time zone.

| Contract | Request | Result | Model host tool |
| --- | --- | --- | --- |
| `seer.task_plan` 1 | `seer.plan_request` | `ready`, `unsupported`, `needs_cloud`, `failed` | `seer.model.plan` |
| `seer.ask` 1 | `seer.ask_request` | `progress` while watching, then `answered`, `handoff`, `needs_cloud`, `failed` | `seer.model.ask` |
| `seer.analysis` 1 | `seer.analysis_request` | `answered`, `no_matches`, `not_indexed`, `offer_indexing`, `indexing`, `needs_cloud`, `failed` | `seer.model.analysis` |

## Task plan

A request carries exactly one of `example` (a free example task), `text` (the user's words) or `reply` (an answer or edit to `previous`). Conversation ids are at most 120 characters and camera ids are unique. A `previous` card is checked like any card, and a quick reply must name the card's question and one of its replies; otherwise the request gets an `error` and no card. The card has a `title`, a one-sentence `goal`, an `input` (camera, zone polygon, hours), the `tools` the agent chose from the catalog, at most two sentences of `reasoning`, at most one `question` with up to four quick replies, and a `response` (`alert` or `record`).

```json
{"contract": "seer.task_plan", "version": 1, "conversation": "t1", "revision": 1, "status": "ready",
 "title": "Person stays in Gate area", "goal": "Alert when a person stays in Gate area for 10 seconds or more.",
 "input": {"camera": "cam-main", "camera_name": "Main gate", "zone": {"name": "Gate area", "polygon": [[0.37,0.46],[0.77,0.46],[0.77,0.9],[0.37,0.9]]}, "hours": {"all_day": true}},
 "tools": [{"id": "person_detection", "label": "Person detection"}, {"id": "time_in_area", "label": "Time in area"}],
 "reasoning": "I start counting when the bottom of the box is inside Gate area, and keep counting through up to 10 seconds of being hidden. Other objects are ignored.",
 "question": {"id": "response", "text": "Should I alert you, or only record it?", "replies": [{"id": "alert", "label": "Alert me"}, {"id": "record", "label": "Only record"}]},
 "response": "alert", "local_only": true, "mission": {"schema_version": 1, "id": "task.t1", "version": 1, "condition": {"type": "object_dwell"}}}
```

- The free tier builds this card without a model from an example: people, vehicles and bikes entering an area (`zone_entry`) or staying in it (`time_in_area`). The example's `dwell_s` is in seconds; the mission's `dwell_ms` is in milliseconds, and the goal states the exact time (150 s reads "150 seconds", 120 s reads "2 minutes"). The card keeps its `example`, and its `mission` is a ready `object_dwell` mission; Start watching opens it with `mission.open`. Watching hours stay with the app, which routes frames only inside them.
- The free card asks one question, `response` ("Should I alert you, or only record it?"). Its quick reply is applied without a model: the card is rebuilt from its example, so the goal says "Alert when…" or "Record when…", the mission's purpose follows it, and `revision` and the mission version go up. The app alerts on a decision only when the card's `response` is `alert`. Every other reply, including a reply to a planner's own question, goes back to the planner with `previous` and `reply`. On the free tier, free words return `needs_cloud` with three `examples` built from the user's cameras.
- A planner candidate is `{"status": "ready", ...card}` or `{"status": "unsupported", "suggestion", "example"?}`. Unsupported becomes `alternative` with the fixed reason `cannot_watch_reliably` ("I can't watch for this reliably.") and an observable suggestion, optionally as an example the app can build. A goal longer than one sentence, reasoning longer than two, a camera or tool outside the request and catalog, a `response` question with answers other than `alert` and `record`, or any extra field becomes `failed` with `plan_failed`. A sentence ends at `.`, `!` or `?` followed by a space and anything but a lowercase Latin letter, so "after 9 p.m. on weekdays" stays one sentence and two Korean sentences count as two. Tool labels always come from the catalog. `local_only` is false when any tool runs on Seer Cloud (`clip_review`).

## Ask on Live

Ask answers about now and the last 10 minutes. Attachments are a `photo`, a `file`, a `camera` or a recorded `clip`. With clips attached, Ask watches the clips; with cameras attached, the last 10 minutes of those cameras; otherwise the last 10 minutes of every camera. A clip longer than 10 minutes, or a question the model says is about older footage, becomes a `handoff` to Analysis (`clip_longer_than_ask_window` or `older_than_ask_window`).

```json
{"contract": "seer.ask", "version": 1, "conversation": "a1", "kind": "answer", "status": "answered",
 "text": "Yes. The person on the left arrives with a bike, waits at the gate, then walks in.",
 "moments": [{"at_ms": 1799999280000, "camera": "cam-main", "frame": {"camera": "cam-main", "at_ms": 1799999280000}, "text": "Arrives with a bike"}],
 "coverage": {"watched": [{"camera": "cam-main", "from_ms": 1799999040000, "to_ms": 1799999640000}], "not_available": []},
 "follow_ups": ["Where did they go next?"]}
```

While the model watches, the harness sends `{"kind": "progress", "status": "watching", "watching", "done_ms", "total_ms"}`; `done_ms` counts through the watched ranges in order and `watching` is the range it has reached. A moment outside what Ask watched, or a gap (`not_available`) that does not sit inside one watched range of its camera, is refused (`ask_failed`). The coverage line ("Watched Main gate 12:20–12:30") is rendered by the app from `coverage`.

## Analysis

A request names the `scope` (cameras and range) and, once per camera, how far back the search index reaches (`indexed_from_ms`, null when not indexed). Analysis needs Seer Cloud: without the `seer.model.analysis` tool every search returns `needs_cloud` with its coverage, and nothing is offered. When the app marks the first search (`first_search: true`) and cameras in scope have no index, it offers `last_24_hours` or `from_now`; later searches answer from what is indexed and list the rest as not indexed; the credit estimate is computed only from the `credits_per_camera_hour` the app supplies, and is null without it. Choosing `last_24_hours` returns `indexing`; `from_now` searches what is indexed. Coverage lists `searched` and `not_indexed` ranges, so "No matches in indexed footage" (`no_matches`) is never confused with "Not indexed" (`not_indexed`). A clip whose whole span (`at_ms` to `at_ms + length_ms`) does not sit inside one searched range of its camera, or whose frame falls outside the clip, is refused (`analysis_failed`).

```json
{"contract": "seer.analysis", "version": 1, "conversation": "n1", "status": "offer_indexing",
 "offer": {"cameras": ["cam-side"], "from_ms": 1799913600000, "to_ms": 1800000000000, "credits": 120, "choices": ["last_24_hours", "from_now"]}}
```

## Host protocol

These requests extend host protocol v1; earlier messages are unchanged.

| From the app | Body | Replies (with `reply_to`) |
| --- | --- | --- |
| `plan` | `seer.plan_request`, optional `timeout_ms` | `plan.result` |
| `ask` | `seer.ask_request`, optional `timeout_ms` | `ask.progress` (zero or more), then `ask.result` |
| `analysis` | `seer.analysis_request`, optional `timeout_ms` | `analysis.result` |
| `tool.progress` | `call`, `done_ms` for a running `seer.model.ask` call | `ask.progress` for that Ask; `unknown_call` otherwise |

An invalid request gets an `error` with a code and no result. A request that passes its checks always gets its result: an unexpected error becomes the contract's failure (`plan_failed`, `ask_failed`, `analysis_failed`) instead of silence. The model step is a host tool the app declares at `hello` (`seer.model.plan`, `seer.model.ask`, `seer.model.analysis`) and answers with `tool.result`; its arguments are `{request, revision, tools}`, `{request, scope}` and `{request, coverage}`, carried once (the call's `event.data` holds only the conversation id). Without the tool, Ask and Analysis return the status `needs_cloud`; a cloud-tier plan returns the status `failed` with the failure code `cloud_unavailable`. A model that fails, answers late (default 120 s, at most 600 s) or breaks a rule becomes the contract's failure. Model calls run as their own tasks, so missions and other messages keep flowing. After `shutdown`, new conversation requests get `error` `stopping`; open ones get the drain timeout to finish, and any still running then gets a failure with the code `stopped` before the shutdown `ack`.
