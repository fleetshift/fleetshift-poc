package domain

import "context"

// FulfillmentObserver is called at key points during fulfillment
// orchestration. Implementations should embed [NoOpFulfillmentObserver]
// for forward compatibility with new methods added to this interface.
//
// The observer serves two layers of probes:
//   - Workflow-level probes ([FulfillmentRunProbe]) are created in the
//     deterministic workflow body and observe control flow.
//   - Activity-level probes are created inside activity closures and
//     observe I/O-bearing work. They cannot cross the workflow/activity
//     serialization boundary.
type FulfillmentObserver interface {
	// RunStarted is called when the orchestration workflow begins
	// processing a fulfillment. Returns a potentially modified context
	// and a probe to track the run.
	RunStarted(ctx context.Context, fulfillmentID FulfillmentID) (context.Context, FulfillmentRunProbe)

	// AcquireLockStarted is called at the start of the
	// acquire-lock-and-load activity.
	AcquireLockStarted(ctx context.Context, fulfillmentID FulfillmentID) (context.Context, AcquireLockProbe)

	// DeliverStarted is called at the start of the deliver-to-target
	// activity.
	DeliverStarted(ctx context.Context, input DeliverInput) (context.Context, DeliverProbe)

	// RemoveStarted is called at the start of the remove-from-target
	// activity.
	RemoveStarted(ctx context.Context, input RemoveInput) (context.Context, RemoveProbe)

	// PersistReconciliationStarted is called at the start of the
	// persist-and-complete-reconciliation activity.
	PersistReconciliationStarted(ctx context.Context, fulfillmentID FulfillmentID) (context.Context, PersistReconciliationProbe)

	// ProcessOutputsStarted is called at the start of the
	// process-delivery-outputs activity.
	ProcessOutputsStarted(ctx context.Context) (context.Context, ProcessOutputsProbe)
}

// ---------------------------------------------------------------------------
// Layer 1: Workflow-level probes
// ---------------------------------------------------------------------------

// FulfillmentRunProbe tracks a single orchestration run.
// Implementations should embed [NoOpFulfillmentRunProbe] for forward
// compatibility.
type FulfillmentRunProbe interface {
	// DispatchCycleStarted creates a sub-probe for a single
	// [dispatchAndAwait] invocation. The sub-probe tracks dispatches,
	// acks, timeouts, completions, and stale-event discards within the
	// cycle. Does not return a context because it runs in the
	// deterministic workflow body; the parent probe already holds the
	// workflow context.
	DispatchCycleStarted(deliveryCount int, expectedGen Generation) DispatchCycleProbe

	// StateChanged is called when the fulfillment transitions to a new state.
	StateChanged(state FulfillmentState)

	// RolloutStepStarted is called at the beginning of each rollout
	// step. isDeliver distinguishes deliver steps from remove steps.
	RolloutStepStarted(stepIndex, stepCount int, isDeliver bool)

	// GenerationAdvancedMidRollout is called when a mid-rollout
	// generation check detects that the fulfillment has been mutated.
	GenerationAdvancedMidRollout(startGen, currentGen Generation)

	// ReconciliationRestarting is called when the workflow decides to
	// restart reconciliation because the generation advanced during
	// the current pass.
	ReconciliationRestarting(generation Generation)

	// ContinueAsNewTriggered is called when the workflow returns a
	// [ContinueAsNew] error to restart with a fresh history.
	ContinueAsNewTriggered()

	// DeleteStarted is called when the delete pipeline begins.
	DeleteStarted(targetCount int)

	// ManifestsFiltered is called after [FilterAcceptedManifests] runs for
	// a target. total is the pre-filter count; accepted is the post-filter
	// count. When accepted is zero the target receives no delivery.
	ManifestsFiltered(target TargetInfo, total, accepted int)

	// Error is called when an error occurs during the run.
	Error(err error)

	// End signals the run is complete (for timing). Called via defer.
	End()
}

// DispatchCycleProbe tracks a single [dispatchAndAwait] invocation.
// Created by [FulfillmentRunProbe.DispatchCycleStarted] at the top of
// dispatchAndAwait and ended via defer.
type DispatchCycleProbe interface {
	// Dispatched is called after each successful dispatch. isRedispatch
	// is true on the second and subsequent iterations of the outer
	// retry loop.
	Dispatched(deliveryID DeliveryID, isRedispatch bool)

	// Skipped is called when dispatch returns (false, nil), indicating
	// the delivery should not be tracked (e.g. no delivery record).
	Skipped(deliveryID DeliveryID)

	// AckReceived is called when a valid ack event arrives for a
	// tracked delivery.
	AckReceived(deliveryID DeliveryID)

	// AckTimeout is called when AwaitSignalWithTimeout returns
	// [ErrSignalTimeout], triggering a re-dispatch of unacked
	// deliveries.
	AckTimeout(unackedCount int)

	// Completed is called when a valid completion event arrives and
	// the delivery reaches a terminal state.
	Completed(deliveryID DeliveryID, state DeliveryState)

	// StaleEventDiscarded is called when an event's generation does
	// not match expectedGen and is silently dropped.
	StaleEventDiscarded(event FulfillmentEvent, expectedGen Generation)

	// Error is called when the dispatch cycle encounters a fatal error.
	Error(err error)

	// End signals the cycle is complete. Called via defer.
	End()
}

// ---------------------------------------------------------------------------
// Layer 2: Activity-level probes
// ---------------------------------------------------------------------------

// AcquireLockProbe tracks the acquire-lock-and-load activity.
// Observes lock acquisition, pool loading, and evidence resolution.
type AcquireLockProbe interface {
	// LockAcquired is called after the lock check. newlyAcquired is
	// true when the lock was claimed by this call, false when it was
	// already held by this workflow execution.
	LockAcquired(newlyAcquired bool)

	// PoolLoaded is called after the target pool is loaded.
	PoolLoaded(targetCount int)

	// EvidenceResolved is called after attestation evidence resolution.
	// hasEvidence is false when the fulfillment has no provenance.
	EvidenceResolved(hasEvidence bool)

	// Error is called when the activity encounters an error.
	Error(err error)

	// End signals the activity is complete. Called via defer.
	End()
}

// DeliverProbe tracks a single deliver-to-target activity invocation.
// Observes the three-way branch: new delivery, redispatch, or
// retry/skip.
type DeliverProbe interface {
	// NewDelivery is called when a brand-new delivery record is created.
	NewDelivery()

	// Redispatched is called when an existing delivery is redispatched
	// at an advanced generation.
	Redispatched(previousGen Generation)

	// Retried is called when an existing same-generation delivery is
	// retried (still Pending from a previous failed dispatch).
	Retried()

	// ResetForRetry is called when a terminal delivery at the same
	// generation is reset to Pending for re-dispatch.
	ResetForRetry(previousState DeliveryState)

	// SkippedAlreadyAcked is called when the delivery has already
	// progressed past Pending, so no re-dispatch is needed.
	SkippedAlreadyAcked()

	// Error is called when the activity encounters an error.
	Error(err error)

	// End signals the activity is complete. Called via defer.
	End()
}

// RemoveProbe tracks a single remove-from-target activity invocation.
// Observes the three-way branch: no record, withdraw, or already
// pending.
type RemoveProbe interface {
	// TargetNotFound is called when no delivery record exists for the
	// target, so the removal is skipped.
	TargetNotFound()

	// Withdrawn is called when the delivery is successfully
	// transitioned to a withdrawal state.
	Withdrawn()

	// AlreadyPending is called when the delivery has already
	// progressed past Pending (addon acked the removal) so no
	// re-dispatch is needed.
	AlreadyPending()

	// Error is called when the activity encounters an error.
	Error(err error)

	// End signals the activity is complete. Called via defer.
	End()
}

// PersistReconciliationProbe tracks the
// persist-and-complete-reconciliation activity. Observes the result
// state, restart decision, and delete cleanup signal.
type PersistReconciliationProbe interface {
	// Persisted is called after the reconciliation result is committed.
	// needsRestart is true when the generation advanced during the
	// pipeline.
	Persisted(state FulfillmentState, needsRestart bool)

	// DeleteCleanupSignaled is called after the delete cleanup signal
	// is sent for a DELETING fulfillment.
	DeleteCleanupSignaled()

	// Error is called when the activity encounters an error.
	Error(err error)

	// End signals the activity is complete. Called via defer.
	End()
}

// ProcessOutputsProbe tracks the process-delivery-outputs activity.
// Observes secret storage, target registration, and skip decisions.
type ProcessOutputsProbe interface {
	// SecretsStored is called after produced secrets are stored in
	// the vault.
	SecretsStored(count int)

	// TargetsRegistered is called after provisioned targets are
	// upserted into inventory and the target store.
	TargetsRegistered(count int)

	// Skipped is called when the delivery result has no outputs to
	// process.
	Skipped()

	// Error is called when the activity encounters an error.
	Error(err error)

	// End signals the activity is complete. Called via defer.
	End()
}

// ---------------------------------------------------------------------------
// NoOp implementations
// ---------------------------------------------------------------------------

// NoOpFulfillmentObserver is a [FulfillmentObserver] that returns no-op probes.
type NoOpFulfillmentObserver struct{}

func (NoOpFulfillmentObserver) RunStarted(ctx context.Context, _ FulfillmentID) (context.Context, FulfillmentRunProbe) {
	return ctx, NoOpFulfillmentRunProbe{}
}

func (NoOpFulfillmentObserver) AcquireLockStarted(ctx context.Context, _ FulfillmentID) (context.Context, AcquireLockProbe) {
	return ctx, NoOpAcquireLockProbe{}
}

func (NoOpFulfillmentObserver) DeliverStarted(ctx context.Context, _ DeliverInput) (context.Context, DeliverProbe) {
	return ctx, NoOpDeliverProbe{}
}

func (NoOpFulfillmentObserver) RemoveStarted(ctx context.Context, _ RemoveInput) (context.Context, RemoveProbe) {
	return ctx, NoOpRemoveProbe{}
}

func (NoOpFulfillmentObserver) PersistReconciliationStarted(ctx context.Context, _ FulfillmentID) (context.Context, PersistReconciliationProbe) {
	return ctx, NoOpPersistReconciliationProbe{}
}

func (NoOpFulfillmentObserver) ProcessOutputsStarted(ctx context.Context) (context.Context, ProcessOutputsProbe) {
	return ctx, NoOpProcessOutputsProbe{}
}

// NoOpFulfillmentRunProbe is a [FulfillmentRunProbe] that discards all events.
type NoOpFulfillmentRunProbe struct{}

func (NoOpFulfillmentRunProbe) DispatchCycleStarted(int, Generation) DispatchCycleProbe {
	return NoOpDispatchCycleProbe{}
}
func (NoOpFulfillmentRunProbe) StateChanged(FulfillmentState)                       {}
func (NoOpFulfillmentRunProbe) RolloutStepStarted(int, int, bool)                   {}
func (NoOpFulfillmentRunProbe) GenerationAdvancedMidRollout(Generation, Generation) {}
func (NoOpFulfillmentRunProbe) ReconciliationRestarting(Generation)                 {}
func (NoOpFulfillmentRunProbe) ContinueAsNewTriggered()                             {}
func (NoOpFulfillmentRunProbe) DeleteStarted(int)                                   {}
func (NoOpFulfillmentRunProbe) ManifestsFiltered(TargetInfo, int, int)              {}
func (NoOpFulfillmentRunProbe) Error(error)                                         {}
func (NoOpFulfillmentRunProbe) End()                                                {}

// NoOpDispatchCycleProbe is a [DispatchCycleProbe] that discards all events.
type NoOpDispatchCycleProbe struct{}

func (NoOpDispatchCycleProbe) Dispatched(DeliveryID, bool)                      {}
func (NoOpDispatchCycleProbe) Skipped(DeliveryID)                               {}
func (NoOpDispatchCycleProbe) AckReceived(DeliveryID)                           {}
func (NoOpDispatchCycleProbe) AckTimeout(int)                                   {}
func (NoOpDispatchCycleProbe) Completed(DeliveryID, DeliveryState)              {}
func (NoOpDispatchCycleProbe) StaleEventDiscarded(FulfillmentEvent, Generation) {}
func (NoOpDispatchCycleProbe) Error(error)                                      {}
func (NoOpDispatchCycleProbe) End()                                             {}

// NoOpAcquireLockProbe is an [AcquireLockProbe] that discards all events.
type NoOpAcquireLockProbe struct{}

func (NoOpAcquireLockProbe) LockAcquired(bool)     {}
func (NoOpAcquireLockProbe) PoolLoaded(int)        {}
func (NoOpAcquireLockProbe) EvidenceResolved(bool) {}
func (NoOpAcquireLockProbe) Error(error)           {}
func (NoOpAcquireLockProbe) End()                  {}

// NoOpDeliverProbe is a [DeliverProbe] that discards all events.
type NoOpDeliverProbe struct{}

func (NoOpDeliverProbe) NewDelivery()                {}
func (NoOpDeliverProbe) Redispatched(Generation)     {}
func (NoOpDeliverProbe) Retried()                    {}
func (NoOpDeliverProbe) ResetForRetry(DeliveryState) {}
func (NoOpDeliverProbe) SkippedAlreadyAcked()        {}
func (NoOpDeliverProbe) Error(error)                 {}
func (NoOpDeliverProbe) End()                        {}

// NoOpRemoveProbe is a [RemoveProbe] that discards all events.
type NoOpRemoveProbe struct{}

func (NoOpRemoveProbe) TargetNotFound() {}
func (NoOpRemoveProbe) Withdrawn()      {}
func (NoOpRemoveProbe) AlreadyPending() {}
func (NoOpRemoveProbe) Error(error)     {}
func (NoOpRemoveProbe) End()            {}

// NoOpPersistReconciliationProbe is a [PersistReconciliationProbe] that
// discards all events.
type NoOpPersistReconciliationProbe struct{}

func (NoOpPersistReconciliationProbe) Persisted(FulfillmentState, bool) {}
func (NoOpPersistReconciliationProbe) DeleteCleanupSignaled()           {}
func (NoOpPersistReconciliationProbe) Error(error)                      {}
func (NoOpPersistReconciliationProbe) End()                             {}

// NoOpProcessOutputsProbe is a [ProcessOutputsProbe] that discards
// all events.
type NoOpProcessOutputsProbe struct{}

func (NoOpProcessOutputsProbe) SecretsStored(int)     {}
func (NoOpProcessOutputsProbe) TargetsRegistered(int) {}
func (NoOpProcessOutputsProbe) Skipped()              {}
func (NoOpProcessOutputsProbe) Error(error)           {}
func (NoOpProcessOutputsProbe) End()                  {}
