# Inventory Write Path POC

This POC explores the hot SQL path for inventory latest-state writes after moving
history bookkeeping out of the synchronous path.

The modeled shape is intentionally narrower than the server:

- `extension_resource_inventory` stores latest `observation`, `labels`, and
  `conditions` in one row, with labels and conditions as JSONB.
- labels and conditions have GIN indexes so write costs include the expected
  read-index maintenance overhead.
- replacement reports provide complete JSONB latest state.
- delta reports provide pre-aggregated per-resource JSONB patches instead of
  flattened label or condition rows.
- aliases use the claims/contributions model and an alias fingerprint fast path.
- `bench_acm_resources` models the ACM single-table JSONB write baseline with
  the same style of query indexes used by the server benchmark.

Run it with:

```sh
go test -v ./...
```

The test starts a Postgres 18 container, seeds 100,000 primary extension
resources, 100,000 latest inventory rows, 86,200 alias contributions, and an
ACM-style 100,000-row JSONB table, then logs `EXPLAIN (ANALYZE, BUFFERS)`
summaries for representative batches. It also performs a few correctness checks
after the mutating scenarios.

The most important cases are:

- base replacement with observation, labels, and conditions only
- ACM-style single-table JSONB batch replacement
- heartbeat-style deltas that only bump freshness timestamps
- patch-style deltas that update observation, labels, and conditions
- never-alias and same-alias fingerprint skips
- same-alias source-first classification without writes
- new alias contribution
- same-resource alias self-replacement by updating the existing claim value
- same-resource alias self-replacement conflict when a sibling still contributes the old claim
- alias retraction with same-statement orphan claim cleanup
- full replace mixed success in one query: latest state, fingerprint skip, source-first no-op, new claim, claim reuse, self-replace, absence retraction, orphan cleanup, and fingerprint updates
- full replace sibling conflict in one query: conflict classification gates latest-state, claim, contribution, retraction, cleanup, and fingerprint writes
