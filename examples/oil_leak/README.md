# Suspected oil leak

Run from the repository root after installation:

```sh
seer-agent run --mission examples/oil_leak/mission.json --input examples/oil_leak/events.jsonl --db runs/oil_leak.sqlite --demo --replay
seer-agent inspect --db runs/oil_leak.sqlite --mission oil_leak:1
seer-agent actions --db runs/oil_leak.sqlite --mission oil_leak:1
```

Expected: one `opened` transition at 3000 ms, one `cleared` transition at 6000 ms, two local `record` results and six observation-tool results. Repeat the first command against the same database: original operation IDs and receipts remain unchanged. No provider or model is called.

The camera, document, chat, and sensor entries are synthetic. The fixture tool consumes supplied labels; it performs no visual inference. Visible change plus a low oil-level reading is a suspected leak, not proof of oil identity or field accuracy.

Change the operator's `maintenance_active` value to `true` and use a new database: the incident opens as `review`. Document/chat/sensor context is selected by source, area and decision-time availability. Stale or unrelated context is excluded. Input text cannot grant tools or change recipients.

To use real evidence, replace the fixture tool with a registered async model tool or `model_endpoint`, keeping the same output schema; feed camera frame references through `sources.frames`. Configure a Telegram, WhatsApp, voice or webhook tool in trusted host code, explicitly allow its ID in policy, and opt into external effects. See [integrations](../../docs/integrations.md). Real cameras, weights and provider delivery need separate validation.
