# Contributing

Keep changes focused on the perception harness. Use synthetic inputs and fake provider transports in tests. Add a source, condition or tool through the public interfaces before proposing core changes.

Run `python -m pytest -q` and build a wheel with `python -m build`. Verify both CLI examples against fresh stores and after restart. Changes to serialized contracts, time semantics, tool identity or recovery need regression tests and independent review. Preserve unknown outcomes and evidence provenance.

Do not commit credentials, personal data, recordings, weights, runtime databases or application/backend source. New integrations need provider documentation, strict response validation, clear effect/acceptance semantics and an offline test. A live-service claim requires a separately authorized execution and retained evidence.
