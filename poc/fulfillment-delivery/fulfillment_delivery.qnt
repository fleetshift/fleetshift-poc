/// One Fulfillment delivered to one Target by two possible addon processes.
/// Resources are abstract integer identities. The target's actual set contains
/// only resources owned by this Fulfillment; CAN_PRUNE states whether the target
/// can recover that ownership set from self-identifying resources or a journal.
module fulfillment_delivery {
  const MAX_GENERATION: int
  const MAX_DRIFTS: int
  const DURABLE_ACCEPT: bool
  const PERIODIC_RESYNC: bool
  const CAN_PRUNE: bool
  const AT_MOST_ONE_PROCESSOR: bool
  const FENCE_EVERY_WRITE: bool

  pure val PROCESSORS = Set(0, 1)
  pure val RESOURCES = Set(0, 1)

  type Delivery = { generation: int, resources: Set[int] }
  type State = {
    platformGeneration: int,
    platformDesired: Set[int],
    platformAckedGeneration: int,
    deliveryDue: bool,
    network: Set[Delivery],
    acceptedHighWater: int,
    journalGeneration: int,
    journalDesired: Set[int],
    busy: int -> bool,
    workGeneration: int -> int,
    workDesired: int -> Set[int],
    actual: Set[int],
    lastAppliedGeneration: int,
    highestAppliedGeneration: int,
    reconcileWakeup: bool,
    driftsRemaining: int,
  }

  var state: State

  // Short aliases keep actions readable and make traces approachable.
  val platformGeneration = state.platformGeneration
  val platformDesired = state.platformDesired
  val platformAckedGeneration = state.platformAckedGeneration
  val deliveryDue = state.deliveryDue
  val network = state.network
  val acceptedHighWater = state.acceptedHighWater
  val journalGeneration = state.journalGeneration
  val journalDesired = state.journalDesired
  val busy = state.busy
  val workGeneration = state.workGeneration
  val workDesired = state.workDesired
  val actual = state.actual
  val lastAppliedGeneration = state.lastAppliedGeneration
  val highestAppliedGeneration = state.highestAppliedGeneration
  val reconcileWakeup = state.reconcileWakeup
  val driftsRemaining = state.driftsRemaining

  pure def maxInt(a: int, b: int): int = if (a > b) a else b

  val latestDelivery: Delivery = {
    generation: platformGeneration,
    resources: platformDesired,
  }

  val noGenerationRegression: bool =
    lastAppliedGeneration == highestAppliedGeneration

  val acceptedStateIsCurrentOrAhead: bool =
    journalGeneration <= acceptedHighWater and
    platformAckedGeneration <= platformGeneration

  val typeAndBoundsOk: bool = and {
    platformGeneration >= 0,
    platformGeneration <= MAX_GENERATION,
    platformDesired.subseteq(RESOURCES),
    actual.subseteq(RESOURCES),
    journalDesired.subseteq(RESOURCES),
    driftsRemaining >= 0,
    PROCESSORS.forall(p => and {
      workGeneration.get(p) >= 0,
      workDesired.get(p).subseteq(RESOURCES),
    }),
  }

  val safety: bool = and {
    noGenerationRegression,
    acceptedStateIsCurrentOrAhead,
    typeAndBoundsOk,
  }

  val converged: bool = and {
    journalGeneration == platformGeneration,
    platformAckedGeneration == platformGeneration,
    actual == platformDesired,
  }

  /// Acceptance-time generation checks cannot stop a worker that was accepted
  /// earlier but remains live after another worker advances the high-water mark.
  def applyAllowed(p: int): bool = and {
    p.in(PROCESSORS),
    busy.get(p),
    not(FENCE_EVERY_WRITE) or workGeneration.get(p) == acceptedHighWater,
  }

  action init = state' = {
    platformGeneration: 0,
    platformDesired: Set(),
    platformAckedGeneration: 0,
    deliveryDue: false,
    network: Set(),
    acceptedHighWater: 0,
    journalGeneration: 0,
    journalDesired: Set(),
    busy: PROCESSORS.mapBy(p => false),
    workGeneration: PROCESSORS.mapBy(p => 0),
    workDesired: PROCESSORS.mapBy(p => Set()),
    actual: Set(),
    lastAppliedGeneration: 0,
    highestAppliedGeneration: 0,
    reconcileWakeup: false,
    driftsRemaining: MAX_DRIFTS,
  }

  /// User mutations and strategy invalidations change the desired level and
  /// durably record a delivery edge. Deliveries contain full state, not patches.
  action changeDesired(resources: Set[int]): bool = all {
    platformGeneration < MAX_GENERATION,
    resources.subseteq(RESOURCES),
    resources != platformDesired,
    state' = state
      .with("platformGeneration", platformGeneration + 1)
      .with("platformDesired", resources)
      .with("deliveryDue", true),
  }

  /// Durable orchestration retries until the current generation is accepted.
  /// Old messages remain in the network as delayed duplicates.
  action dispatchLatest = all {
    deliveryDue,
    not(latestDelivery.in(network)),
    state' = state.with("network", network.union(Set(latestDelivery))),
  }

  /// Persist full intent and advance a generation high-water mark before ack.
  /// DURABLE_ACCEPT=false is the intentionally broken "ack RAM" alternative.
  action accept(p: int, delivery: Delivery): bool = all {
    p.in(PROCESSORS),
    delivery.in(network),
    not(busy.get(p)),
    delivery.generation >= acceptedHighWater,
    not(AT_MOST_ONE_PROCESSOR) or PROCESSORS.forall(q => not(busy.get(q))),
    state' = state
      .with("acceptedHighWater", maxInt(acceptedHighWater, delivery.generation))
      .with("journalGeneration", if (delivery.generation >= journalGeneration)
        delivery.generation else journalGeneration)
      .with("journalDesired", if (delivery.generation >= journalGeneration)
        delivery.resources else journalDesired)
      .with("busy", busy.put(p, true))
      .with("workGeneration", workGeneration.put(p, delivery.generation))
      .with("workDesired", workDesired.put(p, delivery.resources)),
  }

  action acceptLatest(p: int): bool = all {
    deliveryDue,
    accept(p, latestDelivery),
  }

  action acceptLatestSome = {
    nondet p = PROCESSORS.oneOf()
    acceptLatest(p)
  }

  action acceptAny = {
    nondet p = PROCESSORS.oneOf()
    nondet delivery = network.oneOf()
    accept(p, delivery)
  }

  /// Ack means the target durably accepted enough information to finish. It
  /// does not claim that all external side effects are already complete.
  action ackLatest = all {
    deliveryDue,
    journalGeneration == platformGeneration,
    state' = state
      .with("platformAckedGeneration", platformGeneration)
      .with("deliveryDue", false),
  }

  /// Full-level apply is idempotent. Safe pruning requires recoverable ownership
  /// of resources omitted by a newer manifest set.
  action apply(p: int): bool = all {
    applyAllowed(p),
    state' = state
      .with("actual", if (CAN_PRUNE)
        workDesired.get(p) else actual.union(workDesired.get(p)))
      .with("lastAppliedGeneration", workGeneration.get(p))
      .with("highestAppliedGeneration", maxInt(
        highestAppliedGeneration, workGeneration.get(p)))
      .with("busy", busy.put(p, false)),
  }

  action applySome = {
    nondet p = PROCESSORS.oneOf()
    apply(p)
  }

  /// A crash drops volatile work. Correct ack leaves the journal intact.
  action crash(p: int): bool = all {
    p.in(PROCESSORS),
    busy.get(p),
    state' = state
      .with("busy", busy.put(p, false))
      .with("journalGeneration", if (DURABLE_ACCEPT) journalGeneration else 0)
      .with("journalDesired", if (DURABLE_ACCEPT) journalDesired else Set()),
  }

  action crashSome = {
    nondet p = PROCESSORS.oneOf()
    crash(p)
  }

  /// Edges and periodic scans schedule the same level-based operation, which
  /// rereads durable current intent instead of replaying an event payload.
  action scheduleLevelReconcile(p: int): bool = all {
    p.in(PROCESSORS),
    not(busy.get(p)),
    journalGeneration > 0,
    actual != journalDesired,
    reconcileWakeup or PERIODIC_RESYNC,
    not(AT_MOST_ONE_PROCESSOR) or PROCESSORS.forall(q => not(busy.get(q))),
    state' = state
      .with("busy", busy.put(p, true))
      .with("workGeneration", workGeneration.put(p, journalGeneration))
      .with("workDesired", workDesired.put(p, journalDesired))
      .with("reconcileWakeup", false),
  }

  action scheduleSomeLevelReconcile = {
    nondet p = PROCESSORS.oneOf()
    scheduleLevelReconcile(p)
  }

  action driftWithEdge(resource: int): bool = all {
    driftsRemaining > 0,
    journalGeneration > 0,
    resource.in(RESOURCES),
    state' = state
      .with("actual", if (resource.in(actual))
        actual.exclude(Set(resource)) else actual.union(Set(resource)))
      .with("driftsRemaining", driftsRemaining - 1)
      .with("reconcileWakeup", true),
  }

  /// Watch gaps and restarts can lose an edge. Periodic level scans must make
  /// that history loss irrelevant to correctness.
  action driftWithoutEdge(resource: int): bool = all {
    driftsRemaining > 0,
    journalGeneration > 0,
    resource.in(RESOURCES),
    state' = state
      .with("actual", if (resource.in(actual))
        actual.exclude(Set(resource)) else actual.union(Set(resource)))
      .with("driftsRemaining", driftsRemaining - 1),
  }

  action changeToA = changeDesired(Set(0))
  action changeToB = changeDesired(Set(1))
  action changeToBoth = changeDesired(Set(0, 1))
  action changeToNone = changeDesired(Set())
  action driftAWithEdge = driftWithEdge(0)
  action driftAWithoutEdge = driftWithoutEdge(0)
  action stutter = state' = state

  action step = any {
    changeToA,
    changeToB,
    changeToBoth,
    changeToNone,
    dispatchLatest,
    acceptAny,
    ackLatest,
    applySome,
    crashSome,
    scheduleSomeLevelReconcile,
    driftAWithEdge,
    driftAWithoutEdge,
    stutter,
  }

  /// Availability/fairness is an explicit assumption. Durable storage alone
  /// cannot prove progress if enabled protocol work is never scheduled.
  temporal fairProtocol = and {
    dispatchLatest.weakFair(state),
    acceptLatestSome.strongFair(state),
    ackLatest.weakFair(state),
    applySome.strongFair(state),
    scheduleSomeLevelReconcile.strongFair(state),
  }

  /// External changes are finitely bounded in this instance. From every state,
  /// a fair execution eventually reaches the current desired level again.
  temporal eventualConvergence =
    fairProtocol implies always(eventually(converged))
}

/// Recommended: durable accepted intent, periodic level resync, recoverable
/// ownership, and a target-enforced generation check on every write.
module reliable_contract {
  import fulfillment_delivery(
    MAX_GENERATION = 2,
    MAX_DRIFTS = 1,
    DURABLE_ACCEPT = true,
    PERIODIC_RESYNC = true,
    CAN_PRUNE = true,
    AT_MOST_ONE_PROCESSOR = false,
    FENCE_EVERY_WRITE = true
  ).*
}

/// A real at-most-one processor is an alternative to per-effect fencing.
module strict_single_processor_contract {
  import fulfillment_delivery(
    MAX_GENERATION = 2,
    MAX_DRIFTS = 1,
    DURABLE_ACCEPT = true,
    PERIODIC_RESYNC = true,
    CAN_PRUNE = true,
    AT_MOST_ONE_PROCESSOR = true,
    FENCE_EVERY_WRITE = false
  ).*
}

/// Broken: overlapping workers and no target-enforced write fence.
module unfenced_contract {
  import fulfillment_delivery(
    MAX_GENERATION = 2,
    MAX_DRIFTS = 0,
    DURABLE_ACCEPT = true,
    PERIODIC_RESYNC = true,
    CAN_PRUNE = true,
    AT_MOST_ONE_PROCESSOR = false,
    FENCE_EVERY_WRITE = false
  ).*
}

/// Broken: prompt edges exist, but there is no recovery from a missed edge.
module edge_only_contract {
  import fulfillment_delivery(
    MAX_GENERATION = 1,
    MAX_DRIFTS = 1,
    DURABLE_ACCEPT = true,
    PERIODIC_RESYNC = false,
    CAN_PRUNE = true,
    AT_MOST_ONE_PROCESSOR = false,
    FENCE_EVERY_WRITE = true
  ).*
}

/// Broken: create works, but omitted previous resource identities cannot be
/// recovered, so spec shrink cannot be pruned.
module no_prune_contract {
  import fulfillment_delivery(
    MAX_GENERATION = 2,
    MAX_DRIFTS = 0,
    DURABLE_ACCEPT = true,
    PERIODIC_RESYNC = true,
    CAN_PRUNE = false,
    AT_MOST_ONE_PROCESSOR = false,
    FENCE_EVERY_WRITE = true
  ).*
}

/// Broken: the target acknowledges volatile state, then a restart loses the
/// replay copy after the platform stops retrying.
module volatile_ack_contract {
  import fulfillment_delivery(
    MAX_GENERATION = 1,
    MAX_DRIFTS = 0,
    DURABLE_ACCEPT = false,
    PERIODIC_RESYNC = true,
    CAN_PRUNE = true,
    AT_MOST_ONE_PROCESSOR = false,
    FENCE_EVERY_WRITE = true
  ).*
}
