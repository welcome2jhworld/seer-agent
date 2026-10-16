# Installed extension

This is a separate example Python distribution. It registers a custom detector tool and a custom condition without modifying Seer Agent.

From the harness repository root:

```sh
python -m pip install ./examples/person_detection/plugin
seer-agent tools --plugin site
```

Copy `examples/person_detection/mission.json` to a new file. Change `id` to `site_person`, change both occurrences of `fixture.detect` to `site.detect`, and change `condition.type` to `site.presence`. Run:

```sh
seer-agent run --mission /path/to/site-mission.json \
  --input examples/person_detection/events.jsonl --db runs/site.sqlite --plugin site --replay
```

Expected: one `review` transition, then `cleared`, with two local records. Repeat with the same database to preserve original receipts. The installed plugin uses synthetic detections; replacing its fixture with an actual model requires separate model/device validation.
