module fulfillment_delivery_test {
  import fulfillment_delivery(
    MAX_GENERATION = 2,
    MAX_DRIFTS = 1,
    DURABLE_ACCEPT = true,
    PERIODIC_RESYNC = true,
    CAN_PRUNE = true,
    AT_MOST_ONE_PROCESSOR = false,
    FENCE_EVERY_WRITE = true
  ).* from "./fulfillment_delivery"

  /// A durable acceptance survives a worker crash and can be replayed from the
  /// target-side level, even after the platform has accepted the acknowledgement.
  run accepted_work_survives_restart =
    init
      .then(changeDesired(Set(0, 1)))
      .then(dispatchLatest)
      .then(acceptLatest(0))
      .then(ackLatest)
      .then(crash(0))
      .then(scheduleLevelReconcile(1))
      .then(apply(1))
      .expect(converged)

  /// Full desired-state replacement plus remembered ownership prunes resources
  /// that disappeared from the new manifest set.
  run spec_shrink_prunes_previous_inventory =
    init
      .then(changeDesired(Set(0, 1)))
      .then(dispatchLatest)
      .then(acceptLatest(0))
      .then(ackLatest)
      .then(apply(0))
      .then(changeDesired(Set(0)))
      .then(dispatchLatest)
      .then(acceptLatest(0))
      .then(apply(0))
      .expect(actual == Set(0))

  /// Full Fulfillment removal is the empty desired level and uses the same
  /// idempotent pruning path as an ordinary spec shrink.
  run full_removal_prunes_all_inventory =
    init
      .then(changeDesired(Set(0, 1)))
      .then(dispatchLatest)
      .then(acceptLatest(0))
      .then(apply(0))
      .then(changeDesired(Set()))
      .then(dispatchLatest)
      .then(acceptLatest(0))
      .then(apply(0))
      .expect(actual == Set())

  /// Once generation 2 has been accepted, the per-write generation fence keeps
  /// a generation-1 worker from changing the target.
  run stale_worker_is_fenced_at_effect =
    init
      .then(changeDesired(Set(0)))
      .then(dispatchLatest)
      .then(acceptLatest(0))
      .then(changeDesired(Set(1)))
      .then(dispatchLatest)
      .then(acceptLatest(1))
      .then(apply(1))
      .expect(not(applyAllowed(0)))
      .expect(noGenerationRegression)

  /// A delivered drift edge improves latency by scheduling the same level-based
  /// reconciliation operation used by periodic recovery.
  run drift_edge_schedules_level_reconcile =
    init
      .then(changeDesired(Set(0)))
      .then(dispatchLatest)
      .then(acceptLatest(0))
      .then(ackLatest)
      .then(apply(0))
      .then(driftWithEdge(0))
      .then(scheduleLevelReconcile(1))
      .then(apply(1))
      .expect(converged)
}
