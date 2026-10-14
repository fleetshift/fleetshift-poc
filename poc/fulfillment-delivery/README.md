# Fulfillment delivery Quint model

This model explores the target-side reliability contract for one Fulfillment on
one Target. It is deliberately small enough to read and exhaustively check. It
does not model delivery authorization or attestation.

The result is a level-based reconciliation protocol with edge-triggered
responsiveness:

- The platform and the target retain full current desired state (the **level**).
- Desired-state changes and target drift produce prompt **edges** that schedule
  reconciliation.
- Durable retries preserve platform-to-target delivery edges until the target
  has durably accepted the level.
- A periodic rescan, or an equivalent complete relist-after-watch-gap protocol,
  recovers missed target-side edges.
- Reconciliation always rereads the latest durable level. It never treats an
  event payload as the source of truth.

## Files

- [`fulfillment_delivery.qnt`](fulfillment_delivery.qnt) is the state machine,
  its safety/liveness properties, and good/broken configurations.
- [`fulfillment_delivery_test.qnt`](fulfillment_delivery_test.qnt) contains
  executable happy-path and recovery scenarios.
- [`fulfillment_delivery_counterexamples.qnt`](fulfillment_delivery_counterexamples.qnt)
  contains executable demonstrations of the gaps.

## Conclusions

### Guarantees that hold

The model checks these guarantees for both supported processor designs:

1. **No target generation regression.** Once generation `g` has affected the
   target, a generation below `g` cannot affect it later.
2. **At-least-once delivery is safe.** Duplicate full-level delivery and apply
   are idempotent.
3. **Ack-before-completion can be safe.** The target may acknowledge promptly
   after durably persisting enough information to finish; a crash then resumes
   from the target-side level.
4. **Spec shrink and full removal converge.** This requires durable knowledge of
   every concrete resource previously owned by the Fulfillment.
5. **Lost drift edges do not prevent convergence.** This requires a periodic
   level scan or an equivalent watch protocol that detects gaps and relists.
6. **Eventual convergence.** With finitely many desired-state changes and drift
   events, and fair scheduling of enabled protocol work, the target repeatedly
   returns to current desired state.

These are not claims about arbitrary implementations. They hold only under the
assumptions encoded in the model and listed below.

### Required processor guarantee

The target needs a durable generation high-water mark at acceptance **plus one**
of these processor designs:

1. **Strict at-most-one processor.** No second process can begin target work
   while the first may still produce effects.
2. **A target-enforced fence on every effect.** Each write validates the current
   generation or fencing token atomically at the target. A stale process becomes
   unable to write after a newer process advances the fence.

An ordinary expiring lease is not automatically either guarantee. During a
pause, partition, or slow call, the lease can expire and a replacement can begin
while the old holder still reaches the target. Such a lease is safe only if the
old holder is guaranteed unable to continue, or if target writes also validate a
monotonically increasing fencing token.

Optimistic concurrency is a valid implementation of the second design when the
generation check and mutation form one atomic compare-and-swap. Checking a
generation and then issuing an unconditional write has the same race as no
fence. For multi-resource apply, every externally visible effect must be
protected; fencing only the journal is insufficient for an already-running old
worker.

### Required target-author contract

A target implementation that claims reliable continuous reconciliation must:

1. Treat a delivery as a full declarative level, not an imperative patch.
2. Persist accepted generation and full intent before acknowledging acceptance.
3. Make apply and delete idempotent under retry and partial completion.
4. Reject generations below its durable high-water mark.
5. Use strict at-most-one processing or validate a monotonic fence on every
   target-side effect.
6. Deterministically recover every concrete resource identity it owns. This can
   come from self-identifying resources, an authoritative ownership query, or a
   durable journal.
7. Retain enough old and in-progress inventory to prune spec shrink, full
   removal, and partially completed work after restart.
8. Reconcile from the latest durable level after startup, reconnect, watch-gap
   detection, and a periodic safety-net trigger.
9. Report terminal success only after observed reality matches the intended
   level. Acceptance and terminal delivery should be distinct protocol points.
10. Surface permanent failures while continuing retry according to an explicit
    policy; fairness cannot be obtained from storage alone.

For a journal, a single "last desired inventory" value is not a sufficient
concrete design for non-atomic targets. The write-ahead record must retain the
previous committed inventory, new desired inventory, generation/fence, and
in-progress phase (or equivalent operation records) until all effects and cleanup
are complete. Otherwise a crash between overwriting the journal and applying the
target can erase the information needed for cleanup. The model abstracts this
detail behind `DURABLE_ACCEPT` and `CAN_PRUNE`; a follow-up model should refine
partial multi-resource effects and the journal state machine.

## Gaps demonstrated by counterexample

| Configuration | Checked property | Result | Meaning |
| --- | --- | --- | --- |
| `reliable_contract` | safety and eventual convergence | holds | Per-effect fencing, durable intent, pruning knowledge, and periodic level scans suffice in this model. |
| `strict_single_processor_contract` | safety and eventual convergence | holds | A real at-most-one target processor can replace per-effect fencing. |
| `unfenced_contract` | generation monotonicity | fails | An already-running generation-1 worker writes after generation 2. |
| `edge_only_contract` | eventual convergence | fails | One missed drift edge leaves a permanently divergent stuttering execution. |
| `no_prune_contract` | eventual convergence | fails | A resource omitted by spec shrink remains forever. |
| `volatile_ack_contract` | eventual convergence | fails | Ack, crash, and loss of volatile intent strand unapplied work after platform retries stop. |

This also clarifies an ambiguity in the prose protocol: `accepted` should mean
"durably replayable by the target," while `delivered` should mean "the target
has observed convergence." If one RPC response is used for both, it must wait
for convergence; if quick acknowledgements are desired, two lifecycle points
are needed.

## Run it

The model was checked with Quint 0.32.0. Temporal properties should use TLC;
Quint currently warns that Apalache's temporal-property support is experimental.

```sh
cd poc/fulfillment-delivery

quint typecheck fulfillment_delivery.qnt
quint typecheck fulfillment_delivery_test.qnt
quint typecheck fulfillment_delivery_counterexamples.qnt

quint test fulfillment_delivery_test.qnt \
  --match='accepted_work_survives_restart|spec_shrink_prunes_previous_inventory|full_removal_prunes_all_inventory|stale_worker_is_fenced_at_effect|drift_edge_schedules_level_reconcile'

quint test fulfillment_delivery_counterexamples.qnt \
  --main=unfenced_counterexamples --match='overlapping_workers_regress_target'
quint test fulfillment_delivery_counterexamples.qnt \
  --main=edge_only_counterexamples --match='missed_edge_strands_drift'
quint test fulfillment_delivery_counterexamples.qnt \
  --main=no_prune_counterexamples --match='spec_shrink_leaks_resource'
quint test fulfillment_delivery_counterexamples.qnt \
  --main=volatile_ack_counterexamples --match='volatile_ack_loses_work'

quint verify fulfillment_delivery.qnt --main=reliable_contract \
  --backend=tlc --invariant=safety
quint verify fulfillment_delivery.qnt --main=reliable_contract \
  --backend=tlc --temporal=eventualConvergence

quint verify fulfillment_delivery.qnt --main=strict_single_processor_contract \
  --backend=tlc --invariant=safety
quint verify fulfillment_delivery.qnt --main=strict_single_processor_contract \
  --backend=tlc --temporal=eventualConvergence
```

The following commands intentionally exit non-zero and print counterexamples:

```sh
quint verify fulfillment_delivery.qnt --main=unfenced_contract \
  --backend=tlc --invariant=noGenerationRegression
quint verify fulfillment_delivery.qnt --main=edge_only_contract \
  --backend=tlc --temporal=eventualConvergence
quint verify fulfillment_delivery.qnt --main=no_prune_contract \
  --backend=tlc --temporal=eventualConvergence
quint verify fulfillment_delivery.qnt --main=volatile_ack_contract \
  --backend=tlc --temporal=eventualConvergence
```

Use `--verbosity=3` on a failing verification to see the full state-by-state TLC
trace.

## Reading the model from TLA+

The translation is direct:

| TLA+ idea | Quint spelling here |
| --- | --- |
| Variables tuple | the single `var state: State` record |
| `Init` | `action init` |
| `Next` | `action step = any { ... }` |
| Primed assignment | `state' = ...` |
| State invariant | `val safety` |
| `WF` / `SF` | `.weakFair(state)` / `.strongFair(state)` |
| `[]<>P` | `always(eventually(P))` |

The booleans passed into each module instance are not runtime feature flags.
They define nearby design alternatives so the checker can show exactly which
assumptions make a property hold.

## Model boundary and next refinements

The model intentionally omits:

- attestation, credentials, and authorization failures
- rollout steps and multiple targets
- multiple Fulfillments contending for the same resource
- partial ordering within a multi-resource apply
- target API failures, backoff, and permanent-failure policy
- terminal status/report deduplication and platform failover races
- non-idempotent external side effects and compensation

The highest-value next refinement is a concrete write-ahead journal with
per-resource partial apply/delete steps and crash points. After that, model
multiple Fulfillments sharing a target to validate ownership isolation and
server-side-apply conflict semantics.
