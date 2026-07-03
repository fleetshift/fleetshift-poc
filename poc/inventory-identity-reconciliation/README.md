# Inventory Identity Reconciliation POC

> [!NOTE]
> The server implementation that shipped (see
> `fleetshift-server/internal/infrastructure/postgres/resource_identity_repo.go`'s
> `GetRepresentation` and
> `docs/design/architecture/resource_identity_and_api.md`) took a narrower slice
> of what's explored here. It kept "reported aliases are a pending assertion,
> not synchronously trusted identity" (below), but did **not** build the
> `accepted_platform_representations` / `identity_conflicts` /
> `identity_reconciliation_queue` read model or the async reconciler. Instead,
> representation is derived purely by name match, unconditionally: an
> extension resource with a non-empty pending alias set is *already* a
> representation of the platform resource sharing its declarative name, the
> same as one reporting no aliases at all -- there's no "hidden until
> reconciled" state for representations today. That's a deliberate
> scope decision to keep the write path simple for this iteration, not a
> rejection of this POC's model: distinguishing accepted from pending (or
> later, conflicting) representations explicitly is real future work this POC
> still describes reasonably well, it's just not wired into production yet.

This POC explores a different inventory identity model:

- inventory reports do not fail on alias conflicts
- the hot path stores latest inventory plus reported identity assertions
- accepted platform identity is a separate reconciled read model
- conflicting assertions are visible on the claimed platform resource, but do
  not become accepted representations or accepted aliases

The hot path intentionally does not decide whether aliases are globally valid.
When a report changes identity assertions, it marks the source `pending`,
which logically hides its accepted identity, and enqueues reconciliation. That
means a first report is not immediately accepted by the hot write path. It
becomes accepted after reconciliation. This keeps the write path simple and
avoids temporarily presenting uncertain identity as true platform state.

The important distinction is between reported state and accepted identity:

- `extension_resource_inventory` and `extension_resources.reported_aliases` are
  the latest state reported by an extension resource
- `accepted_platform_representations` and `accepted_alias_assertions` are the
  identity read model that platform resolution should trust
- `identity_conflicts` records sources whose assertions are stored but not
  accepted

This makes alias conflicts operationally visible without rejecting inventory
reports and without letting a conflicted source share the accepted platform
identity.

The synchronous write path deliberately keeps reported aliases unnormalized. It
stores the canonical alias set as JSONB plus a fingerprint on
`extension_resources`. If the fingerprint is unchanged, no identity work is
performed. If it changed, the source is marked `pending`, conflicts for that
source are cleared, and reconciliation is queued. Accepted alias rows are not
synchronously deleted; platform resolution joins through
`extension_resource_identity_status` and only trusts sources whose state is
`accepted`.

The reconciler demonstrated here is synchronous test SQL, not a durable worker.
It is meant to show the intended async boundary:

1. read pending sources from `identity_reconciliation_queue`
2. expand `extension_resources.reported_aliases` and compare against accepted
   platform identity
3. accept non-conflicting sources into `accepted_platform_representations` and
   `accepted_alias_*`
4. record `identity_conflicts` for conflicting sources
5. clear the queue

Run it with:

```sh
go test -v ./...
```

Run the longer benchmark with:

```sh
FLEETSHIFT_LONG_INVENTORY_IDENTITY_BENCH=1 go test -count=1 -run '^TestInventoryIdentityReconciliation$/long_benchmark$' -v ./...
```

The test exercises:

- first report pending, then accepted by reconciliation
- corroborating reports from another extension becoming accepted
- conflicting reports succeeding in the hot path but reconciling to `conflict`
- pending sources being removed from accepted alias resolution immediately
- conflict resolution after the source reports corrected aliases
- accepted alias removal when a source reports an empty alias set
- hot-path and reconciliation timing for 1,000-row batches across new,
  steady-state, and conflicting shapes

Current local timings on Postgres 18 were:

| Shape | Hot path | Reconciliation |
| --- | ---: | ---: |
| New no aliases | 28-37 ms / 1,000 | 17 ms / 1,000 |
| Steady no aliases | 15-16 ms / 1,000 | ~0.4 ms empty queue check |
| New aliases | 29-37 ms / 1,000 | 29-30 ms / 1,000 |
| Steady same aliases | 17-25 ms / 1,000 | ~0.4 ms empty queue check |
| Conflicting aliases | 31-37 ms / 1,000 | 16-17 ms / 1,000 |

The steady-state rows are the main hot-path target: once identity assertions are
accepted and the alias fingerprint is unchanged, the synchronous write avoids
alias payload rewrites, accepted-identity writes, conflict writes, and queue
writes.

Long benchmark results from the same environment used 5 warmup iterations and
20 measured iterations per shape. Batch sizes are included in the shape name.

| Shape | Sync hot path mean | Sync hot path p95 | Reconciliation mean | Reconciliation p95 |
| --- | ---: | ---: | ---: | ---: |
| `steady_no_aliases_5k` | 85.272 ms | 94.086 ms | 0.463 ms | 0.504 ms |
| `steady_same_aliases_5k` | 95.015 ms | 106.952 ms | 0.492 ms | 0.553 ms |
| `mixed_realistic_5k` | 89.455 ms | 101.804 ms | 6.452 ms | 6.800 ms |
| `new_no_aliases_5k` | 175.162 ms | 185.996 ms | 105.451 ms | 116.979 ms |
| `new_aliases_1k` | 42.489 ms | 56.139 ms | 98.162 ms | 113.639 ms |
| `conflicting_aliases_1k` | 46.822 ms | 54.304 ms | 92.987 ms | 98.681 ms |

The mixed shape is meant to approximate the common case: 96% no aliases, 3.8%
unchanged aliases, and 0.2% changed aliases. In that shape, only 10 of 5,000
resources entered identity reconciliation while the full batch still wrote
latest inventory.
