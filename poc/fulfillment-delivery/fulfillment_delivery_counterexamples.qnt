module unfenced_counterexamples {
  import fulfillment_delivery(
    MAX_GENERATION = 2,
    MAX_DRIFTS = 0,
    DURABLE_ACCEPT = true,
    PERIODIC_RESYNC = true,
    CAN_PRUNE = true,
    AT_MOST_ONE_PROCESSOR = false,
    FENCE_EVERY_WRITE = false
  ).* from "./fulfillment_delivery"

  /// Acceptance-time generation ordering alone does not stop a worker that was
  /// already running. Generation 1 writes after generation 2 and regresses the
  /// customer-visible target.
  run overlapping_workers_regress_target =
    init
      .then(changeDesired(Set(0)))
      .then(dispatchLatest)
      .then(acceptLatest(0))
      .then(changeDesired(Set(1)))
      .then(dispatchLatest)
      .then(acceptLatest(1))
      .then(apply(1))
      .then(apply(0))
      .expect(not(noGenerationRegression))
}

module edge_only_counterexamples {
  import fulfillment_delivery(
    MAX_GENERATION = 1,
    MAX_DRIFTS = 1,
    DURABLE_ACCEPT = true,
    PERIODIC_RESYNC = false,
    CAN_PRUNE = true,
    AT_MOST_ONE_PROCESSOR = false,
    FENCE_EVERY_WRITE = true
  ).* from "./fulfillment_delivery"

  /// A missed drift edge leaves no enabled repair action. Repeating stutter from
  /// this state is the infinite liveness counterexample.
  run missed_edge_strands_drift =
    init
      .then(changeDesired(Set(0)))
      .then(dispatchLatest)
      .then(acceptLatest(0))
      .then(ackLatest)
      .then(apply(0))
      .then(driftWithoutEdge(0))
      .expect(not(converged) and not(reconcileWakeup))
      .then(stutter)
      .expect(not(converged))
}

module no_prune_counterexamples {
  import fulfillment_delivery(
    MAX_GENERATION = 2,
    MAX_DRIFTS = 0,
    DURABLE_ACCEPT = true,
    PERIODIC_RESYNC = true,
    CAN_PRUNE = false,
    AT_MOST_ONE_PROCESSOR = false,
    FENCE_EVERY_WRITE = true
  ).* from "./fulfillment_delivery"

  /// Without prior concrete ownership, replay can create what is present in the
  /// new spec but cannot infer that an omitted resource must be deleted.
  run spec_shrink_leaks_resource =
    init
      .then(changeDesired(Set(0, 1)))
      .then(dispatchLatest)
      .then(acceptLatest(0))
      .then(apply(0))
      .then(changeDesired(Set(0)))
      .then(dispatchLatest)
      .then(acceptLatest(0))
      .then(apply(0))
      .expect(actual == Set(0, 1) and actual != platformDesired)
}

module volatile_ack_counterexamples {
  import fulfillment_delivery(
    MAX_GENERATION = 1,
    MAX_DRIFTS = 0,
    DURABLE_ACCEPT = false,
    PERIODIC_RESYNC = true,
    CAN_PRUNE = true,
    AT_MOST_ONE_PROCESSOR = false,
    FENCE_EVERY_WRITE = true
  ).* from "./fulfillment_delivery"

  /// The platform stops retrying after ack, but a crash loses the target's only
  /// copy of intent before apply. Neither delivery nor level replay remains due.
  run volatile_ack_loses_work =
    init
      .then(changeDesired(Set(0)))
      .then(dispatchLatest)
      .then(acceptLatest(0))
      .then(ackLatest)
      .then(crash(0))
      .expect(and {
        not(deliveryDue),
        journalGeneration == 0,
        actual != platformDesired,
      })
}
