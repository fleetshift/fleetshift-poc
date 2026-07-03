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

To focus only on the production-shaped prepared statement stability check:

```sh
go test -count=1 -run '^TestInventoryWritePathPlans/production_replace_inventory_prepared_plan_stability' -v ./...
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
- full replace partial conflict in one query: conflicting alias candidates are reported and skipped, safe alias candidates and latest-state writes still apply, and fingerprints update only for reports with no alias conflicts

## Production-shaped prepared statement check

`production_replace_inventory_prepared_plan_stability` exercises one
production-shaped `ReplaceInventory` SQL statement with parameter arrays rather
than `generate_series` fixtures. Each 1,000-resource batch contains no-alias
reports, same-alias fingerprint skips, new alias claims, self-replacements,
absence retractions, orphan cleanup, and sibling conflicts. It verifies the
expected per-candidate partial-success result counts on every execution.

The important current finding is that PostgreSQL's plan choice dominates this
query:

- `plan_cache_mode = auto` runs five custom plans at roughly 0.9-1.0s per
  1,000 resources, then switches to a generic plan at roughly 50-60ms per
  1,000 resources.
- `force_custom_plan` stays at roughly 0.9-1.0s per 1,000 resources.
- `force_generic_plan` stays at roughly 50-60ms per 1,000 resources.

The test logs `pg_prepared_statements` counters to confirm the switch
(`generic=3 custom=5` in the auto case) and uses
`PREPARE ...; EXPLAIN ANALYZE EXECUTE ...` for the custom/generic plan
summaries. The practical implication is that the production implementation
should avoid paying the first five custom-plan executions per connection for
this statement, either by forcing a generic plan for this write or by otherwise
choosing an execution mode that does not hit the slow custom-plan path.

## Optimistic first-attempt experiment

`optimistic_replace_inventory_shapes` tests a smaller first-attempt statement
beside the diagnostic mega-CTE. The optimistic statement does not classify alias
conflicts deeply. It writes latest inventory state, gates alias work by the alias
fingerprint, skips aliases whose current contribution already points at the
exact reported claim, inserts missing exact claims and contributions with
`ON CONFLICT DO NOTHING`, deletes absent contributions for full replacement,
performs minimal orphan cleanup, and returns `failed_reports` when the exact
postcondition is not satisfied.

The intended production shape would be: run the optimistic statement inside a
savepoint, roll back to the savepoint if `failed_reports > 0`, then run the
diagnostic statement only when richer conflict handling is needed.

On the current Postgres 18 POC corpus, with both statements forced to generic
plans and run against the same production-array batches, the results are not a
clear win:

- never-alias and same-alias fingerprint-skip batches are essentially tied at
  roughly 20-25ms per 1,000 resources
- new secondary alias insertion is also roughly tied, around 110-125ms per
  1,000 resources
- alias retraction is much worse in the optimistic statement, roughly
  125-140ms per 1,000 resources versus roughly 37-40ms for the diagnostic CTE
- self-replace and sibling-conflict batches are cheaper to detect
  optimistically, but if the diagnostic fallback runs immediately, the combined
  path is slower than running the diagnostic CTE first
- the mixed batch with 300 failed reports costs roughly 40-55ms for the
  optimistic attempt plus roughly 50-55ms for diagnostic fallback, versus
  roughly 50-60ms for the diagnostic statement alone

The takeaway is that this optimistic statement is useful as a reference and
possibly as a cheap "can I apply this without diagnostics?" probe, but it does
not currently justify replacing the diagnostic CTE when synchronous diagnostic
fallback is required. Retraction cleanup is the biggest weakness in the simple
statement.
