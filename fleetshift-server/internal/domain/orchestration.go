package domain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

// errAuthPaused is a sentinel error returned by [dispatchAndAwait] when
// a delivery reports [DeliveryStateAuthFailed]. The orchestration
// catches this to set the pause reason (without changing lifecycle
// state) and complete the workflow.
var errAuthPaused = errors.New("delivery auth failed: pausing for fresh credentials")

// TargetDelta represents the difference between the previous and current
// resolved target sets for a fulfillment.
type TargetDelta struct {
	Added     []TargetInfo
	Removed   []TargetInfo
	Unchanged []TargetInfo
}

// RolloutStep is a single step in a rollout plan: either remove from targets
// or deliver to targets. Exactly one of Remove and Deliver is non-nil.
type RolloutStep struct {
	Remove  *RolloutStepRemove  // remove fulfillment from these targets
	Deliver *RolloutStepDeliver // generate and deliver to these targets
}

// RolloutStepRemove is a step that removes the fulfillment from the listed targets.
type RolloutStepRemove struct {
	Targets []TargetInfo
}

// RolloutStepDeliver is a step that generates manifests and delivers to the listed targets.
type RolloutStepDeliver struct {
	Targets []TargetInfo
}

// RolloutPlan is the output of a rollout strategy: an ordered sequence of steps.
// The orchestrator runs steps in order; each step is either remove or deliver.
type RolloutPlan struct {
	Steps []RolloutStep
}

// GenerateContext provides the target context for manifest generation.
type GenerateContext struct {
	Target TargetInfo
	Config map[string]any
}

// GenerateManifestsInput is the input to the generate-manifests activity.
type GenerateManifestsInput struct {
	Spec   ManifestStrategySpec
	Target TargetInfo
	Config map[string]any
}

// DeliverInput is the input to the deliver-to-target activity.
type DeliverInput struct {
	Target        TargetInfo
	DeliveryID    DeliveryID
	FulfillmentID FulfillmentID
	Manifests     []Manifest
	Auth          DeliveryAuth
	Attestation   *Attestation // nil for token-passthrough deliveries
	Generation    Generation   // fulfillment generation at dispatch; used for stale-delivery fencing
}

// RemoveInput is the input to the remove-from-target activity.
type RemoveInput struct {
	Target        TargetInfo
	DeliveryID    DeliveryID
	FulfillmentID FulfillmentID
	Manifests     []Manifest
	Auth          DeliveryAuth
	Attestation   *Attestation // nil for token-passthrough deliveries
	Generation    Generation   // fulfillment generation at dispatch; used for stale-delivery fencing
}

// RemoveOutput is the result of the remove-from-target activity.
// Dispatched indicates whether a removal was actually dispatched (true)
// or skipped (false). A skip occurs when no delivery record exists or
// the delivery already progressed past Pending (addon already acked
// the removal and a completion signal is queued).
type RemoveOutput struct {
	Dispatched bool
}

// DeliveryOutputsInput carries the delivery identity alongside the
// produced outputs so inventory registration can retain output
// ownership metadata.
type DeliveryOutputsInput struct {
	DeliveryID DeliveryID
	Result     DeliveryResult
}

// ResolvePlacementInput is the input to the resolve-placement activity.
// Pool is the placement view of targets only; see [PlacementTarget].
type ResolvePlacementInput struct {
	Spec PlacementStrategySpec
	Pool []PlacementTarget
}

// PlanRolloutInput is the input to the plan-rollout activity.
type PlanRolloutInput struct {
	Spec  *RolloutStrategySpec
	Delta TargetDelta
}

// ReconciliationSnapshot is the point-in-time state loaded by
// [OrchestrationWorkflowSpec.AcquireLockAndLoad] for a single
// reconciliation pass. It bundles the fulfillment, target pool, and
// any pre-resolved attestation evidence so the pipeline can execute
// without additional I/O for evidence resolution.
type ReconciliationSnapshot struct {
	Fulfillment Fulfillment
	Pool        []TargetInfo
	Evidence    *ResolvedEvidence
}

// PersistAndCompleteInput carries the reconciliation result and the
// generation that was reconciled. Used by the combined
// [PersistAndCompleteReconciliation] activity.
type PersistAndCompleteInput struct {
	Result        ReconciliationResult
	ReconciledGen Generation
}

// OrchestrationWorkflowSpec is the fulfillment pipeline expressed as a
// deterministic workflow. Each reconciliation loads the current state,
// runs the full pipeline (or delete), and atomically completes. If
// the fulfillment's [Generation] has advanced during execution the
// workflow loops and re-runs the pipeline.
//
// Pass this spec to [Registry.RegisterOrchestration] to obtain an
// [OrchestrationWorkflow] that can start instances.
type OrchestrationWorkflowSpec struct {
	Store           Store
	Delivery        DeliveryAgent
	Strategies      StrategyFactory
	Attestation     AttestationAssembler
	CleanupSignaler DeleteCleanupSignaler
	Observer        FulfillmentObserver
	Vault           Vault
	Now             func() time.Time

	// AckRetryInterval is how long the dispatch-and-await loop waits
	// for an acknowledgement signal before redispatching unacked
	// deliveries. Zero defaults to 30 s.
	//
	// Tests should use a value high enough that retries never fire
	// under normal conditions (e.g. 5 s). Too-short values (e.g.
	// 100 ms) cause spurious retries on loaded CI runners, spawning
	// duplicate deliveries and compounding goroutine scheduling
	// pressure.
	AckRetryInterval time.Duration
}

// OrchestrationWorkflowOption configures optional fields on
// [OrchestrationWorkflowSpec].
type OrchestrationWorkflowOption func(*OrchestrationWorkflowSpec)

// WithFulfillmentObserver sets the [FulfillmentObserver] for the
// orchestration workflow.
func WithFulfillmentObserver(o FulfillmentObserver) OrchestrationWorkflowOption {
	return func(s *OrchestrationWorkflowSpec) { s.Observer = o }
}

// WithAttestation sets the [AttestationAssembler] for provenance.
func WithAttestation(a AttestationAssembler) OrchestrationWorkflowOption {
	return func(s *OrchestrationWorkflowSpec) { s.Attestation = a }
}

// WithVault sets the [Vault] for secret storage.
func WithVault(v Vault) OrchestrationWorkflowOption {
	return func(s *OrchestrationWorkflowSpec) { s.Vault = v }
}

// WithNow sets the clock function used by the orchestration.
func WithNow(fn func() time.Time) OrchestrationWorkflowOption {
	return func(s *OrchestrationWorkflowSpec) { s.Now = fn }
}

// WithAckRetryInterval sets how long the dispatch-and-await loop waits
// before redispatching unacked deliveries.
func WithAckRetryInterval(d time.Duration) OrchestrationWorkflowOption {
	return func(s *OrchestrationWorkflowSpec) { s.AckRetryInterval = d }
}

// NewOrchestrationWorkflowSpec creates an [OrchestrationWorkflowSpec]
// with the given required dependencies and applies any options.
// Observer defaults to [NoOpFulfillmentObserver].
func NewOrchestrationWorkflowSpec(
	store Store,
	delivery DeliveryAgent,
	strategies StrategyFactory,
	signaler DeleteCleanupSignaler,
	opts ...OrchestrationWorkflowOption,
) *OrchestrationWorkflowSpec {
	s := &OrchestrationWorkflowSpec{
		Store:           store,
		Delivery:        delivery,
		Strategies:      strategies,
		CleanupSignaler: signaler,
		Observer:        NoOpFulfillmentObserver{},
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

type outputCleanupPlan struct {
	targetIDs    []TargetID
	inventoryIDs []InventoryItemID
	secretRefs   []SecretRef
}

func (s *OrchestrationWorkflowSpec) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *OrchestrationWorkflowSpec) ackRetryInterval() time.Duration {
	if s.AckRetryInterval > 0 {
		return s.AckRetryInterval
	}
	return 30 * time.Second
}

func (s *OrchestrationWorkflowSpec) Name() string { return "orchestrate-fulfillment" }

// Each method returns a typed [Activity] derived from the spec's own
// dependencies. Infrastructure adapters call these to register activities;
// the workflow body calls them via [RunActivity].

// AcquireLockAndLoad acquires the orchestration lock (if not already
// held) and loads the fulfillment and target pool in a single activity.
// On the first call the lock is claimed; on subsequent calls (within
// the same workflow execution) it is already held so only the load
// happens. This combines the former AcquireLock and LoadFulfillment
// activities to eliminate a redundant fulfillment read.
func (s *OrchestrationWorkflowSpec) AcquireLockAndLoad() Activity[FulfillmentID, ReconciliationSnapshot] {
	return NewActivity("acquire-lock-and-load", func(ctx context.Context, id FulfillmentID) (ReconciliationSnapshot, error) {
		ctx, probe := s.observer().AcquireLockStarted(ctx, id)
		defer probe.End()

		tx, err := s.Store.Begin(ctx)
		if err != nil {
			probe.Error(err)
			return ReconciliationSnapshot{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		f, err := tx.Fulfillments().Get(ctx, id)
		if err != nil {
			probe.Error(err)
			if errors.Is(err, ErrNotFound) {
				return ReconciliationSnapshot{}, TerminalError(err)
			}
			return ReconciliationSnapshot{}, err
		}
		acquired := f.AcquireOrchestrationLock()
		probe.LockAcquired(acquired)
		if acquired {
			if err := tx.Fulfillments().Update(ctx, f); err != nil {
				probe.Error(err)
				return ReconciliationSnapshot{}, err
			}
		}

		pool, err := tx.Targets().List(ctx)
		if err != nil {
			probe.Error(err)
			return ReconciliationSnapshot{}, err
		}
		probe.PoolLoaded(len(pool))

		evidence, err := s.Attestation.Resolve(ctx, tx, f)
		if err != nil {
			probe.Error(err)
			return ReconciliationSnapshot{}, err
		}
		probe.EvidenceResolved(evidence != nil)

		if err := tx.Commit(); err != nil {
			probe.Error(err)
			return ReconciliationSnapshot{}, fmt.Errorf("commit: %w", err)
		}
		return ReconciliationSnapshot{
			Fulfillment: *f,
			Pool:        pool,
			Evidence:    evidence,
		}, nil
	})
}

// ResolvePlacement runs the fulfillment's placement strategy against the
// target pool (placement view only). Invoked as an activity so placement
// may perform I/O or use state that changes over time.
func (s *OrchestrationWorkflowSpec) ResolvePlacement() Activity[ResolvePlacementInput, []PlacementTarget] {
	return NewActivity("resolve-placement", func(ctx context.Context, in ResolvePlacementInput) ([]PlacementTarget, error) {
		placement, err := s.Strategies.PlacementStrategy(in.Spec)
		if err != nil {
			return nil, err
		}
		return placement.Resolve(ctx, in.Pool)
	})
}

// PlanRollout runs the fulfillment's rollout strategy to produce an
// ordered execution plan from the target delta. Invoked as an activity
// so rollout may perform I/O or use state that changes over time.
func (s *OrchestrationWorkflowSpec) PlanRollout() Activity[PlanRolloutInput, RolloutPlan] {
	return NewActivity("plan-rollout", func(ctx context.Context, in PlanRolloutInput) (RolloutPlan, error) {
		rollout := s.Strategies.RolloutStrategy(in.Spec)
		return rollout.Plan(ctx, in.Delta)
	})
}

// GenerateManifests creates manifests for a single target using the
// configured manifest strategy.
func (s *OrchestrationWorkflowSpec) GenerateManifests() Activity[GenerateManifestsInput, []Manifest] {
	return NewActivity("generate-manifests", func(ctx context.Context, in GenerateManifestsInput) ([]Manifest, error) {
		strategy, err := s.Strategies.ManifestStrategy(in.Spec)
		if err != nil {
			return nil, err
		}
		return strategy.Generate(ctx, GenerateContext{
			Target: in.Target,
			Config: in.Config,
		})
	})
}

// DeliverToTarget delivers manifests to a target. It persists a
// [Delivery] record in [DeliveryStatePending] (creating a new record
// or using [Delivery.Redispatch] if one exists), then dispatches to
// the [DeliveryAgent]. The agent reports progress and results back
// via its injected [DeliveryReporter].
//
// The delivery receives [context.Background] rather than the activity
// context. Delivery agents run asynchronously (returning immediately
// and completing in a background goroutine), and the activity context
// is canceled once go-workflows completes the activity task. This
// matches the production architecture where delivery runs on a remote
// fleetlet with its own context.
//
// Deliver returns only an error for dispatch failures; all delivery
// outcomes (accepted, rejected, failed, delivered) flow through
// [DeliveryReporter.ReportResult]. This eliminates the dual-path
// problem where synchronous returns could bypass the reporter and
// leave delivery records in a stale state.
func (s *OrchestrationWorkflowSpec) DeliverToTarget() Activity[DeliverInput, struct{}] {
	return NewActivity("deliver-to-target", func(ctx context.Context, in DeliverInput) (struct{}, error) {
		ctx, probe := s.observer().DeliverStarted(ctx, in)
		defer probe.End()

		tx, err := s.Store.Begin(ctx)
		if err != nil {
			probe.Error(err)
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		now := s.now()
		existing, err := tx.Deliveries().GetByFulfillmentTarget(ctx, in.FulfillmentID, in.Target.ID())
		if err != nil && !errors.Is(err, ErrNotFound) {
			probe.Error(err)
			return struct{}{}, fmt.Errorf("load delivery record: %w", err)
		}

		var d Delivery
		if errors.Is(err, ErrNotFound) {
			probe.NewDelivery()
			d = NewDelivery(in.DeliveryID, in.FulfillmentID, in.Target.ID(), in.Manifests, in.Generation, now)
		} else {
			d = existing
			if in.Generation > d.Generation() {
				probe.Redispatched(d.Generation())
				if err := d.Redispatch(in.Manifests, in.Generation, now); err != nil {
					probe.Error(err)
					return struct{}{}, fmt.Errorf("redispatch delivery %s: %w", d.ID(), err)
				}
			} else if !d.Retry(in.Generation, now) {
				if d.Generation() == in.Generation && d.State().IsTerminal() {
					// Delivery reached a terminal state during a previous
					// workflow run. ContinueAsNew restarted the workflow;
					// reset to Pending for retry — addons are idempotent
					// and the platform provides at-least-once delivery.
					// This mirrors Withdraw for the remove path.
					probe.ResetForRetry(d.State())
					if err := d.ResetForRetry(now); err != nil {
						probe.Error(err)
						return struct{}{}, fmt.Errorf("reset delivery %s for retry: %w", d.ID(), err)
					}
				} else {
					// Delivery already progressed past Pending (addon received
					// and acked). The ack signal is queued; no re-dispatch needed.
					probe.SkippedAlreadyAcked()
					return struct{}{}, nil
				}
			} else {
				probe.Retried()
			}
		}

		if err := tx.Deliveries().Put(ctx, d); err != nil {
			probe.Error(err)
			return struct{}{}, fmt.Errorf("persist delivery record: %w", err)
		}
		if err := tx.Commit(); err != nil {
			probe.Error(err)
			return struct{}{}, fmt.Errorf("commit: %w", err)
		}

		if err := s.Delivery.Deliver(context.Background(), in.Target, in.DeliveryID, in.Manifests, in.Auth, in.Attestation, in.Generation); err != nil {
			probe.Error(err)
			return struct{}{}, fmt.Errorf("dispatch delivery %s: %w", in.DeliveryID, err)
		}
		return struct{}{}, nil
	})
}

// RemoveFromTarget prepares the delivery record for removal via
// [Delivery.Withdraw], then dispatches to the [DeliveryAgent]. The
// agent reports removal outcomes via [DeliveryReporter.ReportResult],
// matching the async pattern of [DeliverToTarget].
//
// [Delivery.Withdraw] resets the record to [DeliveryStatePending] so
// the addon's progress events can transition through the delivery
// state machine and reach the observer.
//
// On retry, if the delivery is still Pending at the same generation
// (previous dispatch failed), [Delivery.Retry] bumps the timestamp
// and re-dispatches. If the addon already acked (delivery progressed
// past Pending), the activity returns early — the completion signal
// is already queued.
//
// If no delivery record exists (e.g., delivery failed before
// persisting), the target is skipped.
func (s *OrchestrationWorkflowSpec) RemoveFromTarget() Activity[RemoveInput, RemoveOutput] {
	return NewActivity("remove-from-target", func(ctx context.Context, in RemoveInput) (RemoveOutput, error) {
		ctx, probe := s.observer().RemoveStarted(ctx, in)
		defer probe.End()

		tx, err := s.Store.Begin(ctx)
		if err != nil {
			probe.Error(err)
			return RemoveOutput{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		delivery, err := tx.Deliveries().GetByFulfillmentTarget(ctx, in.FulfillmentID, in.Target.ID())
		if errors.Is(err, ErrNotFound) {
			probe.TargetNotFound()
			return RemoveOutput{Dispatched: false}, nil
		}
		if err != nil {
			probe.Error(err)
			return RemoveOutput{}, fmt.Errorf("load delivery record for target %s: %w", in.Target.ID(), err)
		}

		modified, err := delivery.Withdraw(in.Generation, s.now())
		if err != nil {
			probe.Error(err)
			return RemoveOutput{}, fmt.Errorf("withdraw delivery %s: %w", delivery.ID(), err)
		}
		if !modified {
			if !delivery.Retry(in.Generation, s.now()) {
				// In-progress at current generation — already
				// dispatched and acked. Return without re-dispatching;
				// dispatchAndAwait will wait for the completion signal.
				probe.AlreadyPending()
				return RemoveOutput{Dispatched: true}, nil
			}
		}
		probe.Withdrawn()

		if err := tx.Deliveries().Put(ctx, delivery); err != nil {
			probe.Error(err)
			return RemoveOutput{}, fmt.Errorf("persist delivery: %w", err)
		}
		if err := tx.Commit(); err != nil {
			probe.Error(err)
			return RemoveOutput{}, fmt.Errorf("commit: %w", err)
		}

		if err := s.Delivery.Remove(context.Background(), in.Target, in.DeliveryID, delivery.Manifests(), in.Auth, in.Attestation, in.Generation); err != nil {
			probe.Error(err)
			return RemoveOutput{}, fmt.Errorf("dispatch removal %s: %w", in.DeliveryID, err)
		}
		return RemoveOutput{Dispatched: true}, nil
	})
}

// CleanupDeliveryData cleans up delivery-owned outputs (provisioned
// targets, inventory items, and referenced vault secrets) and
// hard-deletes delivery records for a fulfillment. The fulfillment row
// itself is NOT deleted here; that responsibility belongs to an
// abstraction-specific cleanup workflow such as
// [DeleteDeploymentCleanupWorkflow] or
// [DeleteManagedResourceCleanupWorkflow], which runs after receiving a
// [DeleteCleanupCompleteSignal].
func (s *OrchestrationWorkflowSpec) CleanupDeliveryData() Activity[FulfillmentID, struct{}] {
	return NewActivity("cleanup-delivery-data", func(ctx context.Context, id FulfillmentID) (struct{}, error) {
		readTx, err := s.Store.BeginReadOnly(ctx)
		if err != nil {
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}
		defer readTx.Rollback()

		deliveries, err := readTx.Deliveries().ListByFulfillment(ctx, id)
		if err != nil {
			return struct{}{}, fmt.Errorf("list deliveries: %w", err)
		}

		items, err := readTx.Inventory().List(ctx)
		if err != nil {
			return struct{}{}, fmt.Errorf("list inventory items: %w", err)
		}

		plan, err := buildOutputCleanupPlan(deliveries, items)
		if err != nil {
			return struct{}{}, err
		}
		if err := readTx.Rollback(); err != nil {
			return struct{}{}, fmt.Errorf("close read tx: %w", err)
		}

		if s.Vault != nil {
			for _, ref := range plan.secretRefs {
				if err := s.Vault.Delete(ctx, ref); err != nil && !errors.Is(err, ErrNotFound) {
					return struct{}{}, fmt.Errorf("delete secret %q: %w", ref, err)
				}
			}
		}

		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		for _, targetID := range plan.targetIDs {
			if err := tx.Targets().Delete(ctx, targetID); err != nil && !errors.Is(err, ErrNotFound) {
				return struct{}{}, fmt.Errorf("delete provisioned target %s: %w", targetID, err)
			}
		}
		for _, inventoryID := range plan.inventoryIDs {
			if err := tx.Inventory().Delete(ctx, inventoryID); err != nil && !errors.Is(err, ErrNotFound) {
				return struct{}{}, fmt.Errorf("delete inventory item %s: %w", inventoryID, err)
			}
		}

		if err := tx.Deliveries().DeleteByFulfillment(ctx, id); err != nil {
			return struct{}{}, fmt.Errorf("delete delivery records: %w", err)
		}
		return struct{}{}, tx.Commit()
	})
}

// PersistAndCompleteReconciliation atomically applies a
// [ReconciliationResult] and completes reconciliation in a single
// read-modify-write cycle. It reads the latest fulfillment aggregate
// (preserving concurrent generation bumps), applies the result, and
// advances [Fulfillment.ObservedGeneration]. Returns needsRestart when
// the generation has advanced during the pipeline.
//
// For [FulfillmentStateDeleting] results, the activity also signals the
// fulfillment-scoped cleanup workflow after the commit succeeds.
// Combining the
// signal with the persist step eliminates a retry window where a
// separate signal activity could fail and force a redundant full
// re-execution of the delete pipeline.
//
// Combining persist and complete eliminates the window where the
// fulfillment's state is updated but the lock has not yet been released,
// and prevents the error-swallowing that existed when the two were
// separate activities.
func (s *OrchestrationWorkflowSpec) PersistAndCompleteReconciliation() Activity[PersistAndCompleteInput, bool] {
	return NewActivity("persist-and-complete-reconciliation", func(ctx context.Context, in PersistAndCompleteInput) (bool, error) {
		ctx, probe := s.observer().PersistReconciliationStarted(ctx, in.Result.FulfillmentID)
		defer probe.End()

		tx, err := s.Store.Begin(ctx)
		if err != nil {
			probe.Error(err)
			return false, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		fresh, err := tx.Fulfillments().Get(ctx, in.Result.FulfillmentID)
		if err != nil {
			probe.Error(err)
			return false, fmt.Errorf("get fulfillment: %w", err)
		}

		fresh.ApplyReconciliationResult(in.Result)
		needsRestart := fresh.CompleteReconciliation(in.ReconciledGen, s.now())

		if err := tx.Fulfillments().Update(ctx, fresh); err != nil {
			probe.Error(err)
			return false, err
		}
		if err := tx.Commit(); err != nil {
			probe.Error(err)
			return false, err
		}
		probe.Persisted(in.Result.State, needsRestart)

		if in.Result.State == FulfillmentStateDeleting {
			if err := s.CleanupSignaler.SignalDeleteCleanupComplete(ctx, in.Result.FulfillmentID, DeleteCleanupCompleteEvent{
				FulfillmentID: in.Result.FulfillmentID,
			}); err != nil {
				probe.Error(err)
				return false, fmt.Errorf("signal delete cleanup: %w", err)
			}
			probe.DeleteCleanupSignaled()
		}

		return needsRestart, nil
	})
}

// ProcessDeliveryOutputs stores produced secrets in the [Vault] and
// registers provisioned targets. Secrets are stored first so that
// target properties referencing vault refs are valid at registration
// time. Inventory items retain the originating delivery identity so
// delete-time cleanup can remove delivery-owned outputs generically.
// Results with no outputs are skipped.
//
// All writes use upsert semantics so the activity is replay-safe:
// delivery agents may be re-invoked after a transient failure and
// produce the same outputs on each attempt.
func (s *OrchestrationWorkflowSpec) ProcessDeliveryOutputs() Activity[DeliveryOutputsInput, struct{}] {
	return NewActivity("process-delivery-outputs", func(ctx context.Context, in DeliveryOutputsInput) (struct{}, error) {
		ctx, probe := s.observer().ProcessOutputsStarted(ctx)
		defer probe.End()

		if len(in.Result.ProducedSecrets) == 0 && len(in.Result.ProvisionedTargets) == 0 {
			probe.Skipped()
			return struct{}{}, nil
		}

		if s.Vault != nil {
			for _, secret := range in.Result.ProducedSecrets {
				if err := s.Vault.Put(ctx, secret.Ref, secret.Value); err != nil {
					probe.Error(err)
					return struct{}{}, fmt.Errorf("store secret %q: %w", secret.Ref, err)
				}
			}
			probe.SecretsStored(len(in.Result.ProducedSecrets))
		}

		tx, err := s.Store.Begin(ctx)
		if err != nil {
			probe.Error(err)
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		// TODO: revisit the "TargetRegistrar" thing – should we make that upsert instead? remove that?
		now := s.now()
		for _, pt := range in.Result.ProvisionedTargets {
			props, _ := json.Marshal(pt.Properties)
			invID := InventoryItemID("target:" + string(pt.ID))
			if err := tx.Inventory().CreateOrUpdate(ctx, NewInventoryItem(
				invID,
				InventoryType(pt.Type),
				pt.Name,
				props,
				pt.Labels,
				&in.DeliveryID,
				now,
			)); err != nil {
				probe.Error(err)
				return struct{}{}, fmt.Errorf("upsert inventory item for target %q: %w", pt.ID, err)
			}

			if err := tx.Targets().CreateOrUpdate(ctx, NewTargetInfo(
				pt.ID,
				pt.Type,
				pt.Name,
				"",
				pt.Labels,
				pt.Properties,
				pt.AcceptedManifestTypes,
			)); err != nil {
				probe.Error(err)
				return struct{}{}, fmt.Errorf("upsert target %q: %w", pt.ID, err)
			}
		}
		probe.TargetsRegistered(len(in.Result.ProvisionedTargets))
		if err := tx.Commit(); err != nil {
			probe.Error(err)
			return struct{}{}, err
		}
		return struct{}{}, nil
	})
}

// ReleaseLock clears [Fulfillment.ActiveWorkflowGen] without advancing
// [Fulfillment.ObservedGeneration]. Used before ContinueAsNew so the
// next execution can re-acquire the lock for a fresh retry attempt.
func (s *OrchestrationWorkflowSpec) ReleaseLock() Activity[FulfillmentID, struct{}] {
	return NewActivity("release-orchestration-lock", func(ctx context.Context, id FulfillmentID) (struct{}, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		f, err := tx.Fulfillments().Get(ctx, id)
		if err != nil {
			return struct{}{}, err
		}
		f.ReleaseOrchestrationLock()
		if err := tx.Fulfillments().Update(ctx, f); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, tx.Commit()
	})
}

// CheckGeneration reads the fulfillment's current generation from the
// store. Used mid-rollout to detect whether a new mutation has arrived.
func (s *OrchestrationWorkflowSpec) CheckGeneration() Activity[FulfillmentID, Generation] {
	return NewActivity("check-generation", func(ctx context.Context, id FulfillmentID) (Generation, error) {
		tx, err := s.Store.BeginReadOnly(ctx)
		if err != nil {
			return 0, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		f, err := tx.Fulfillments().Get(ctx, id)
		if err != nil {
			return 0, err
		}
		return f.Generation(), tx.Commit()
	})
}

func (s *OrchestrationWorkflowSpec) observer() FulfillmentObserver {
	return s.Observer
}

// Run is the deterministic workflow body. Each execution does a single
// pass through the pipeline. On retryable failure the workflow releases
// its lock and returns [ContinueAsNew] to restart with a fresh history.
// On terminal failure the workflow persists [FulfillmentStateFailed] and
// completes reconciliation atomically. Run never returns a non-nil
// error — it always terminates in a controlled state or restarts via
// ContinueAsNew.
func (s *OrchestrationWorkflowSpec) Run(record Record, fulfillmentID FulfillmentID) (struct{}, error) {
	ctx, probe := s.observer().RunStarted(record.Context(), fulfillmentID)
	defer probe.End()
	_ = ctx

	for {
		loaded, err := RunActivity(record, s.AcquireLockAndLoad(), fulfillmentID)
		if err != nil {
			probe.Error(err)
			if IsTerminal(err) {
				return struct{}{}, nil
			}
			return struct{}{}, s.releaseLockAndContinue(record, fulfillmentID, probe)
		}
		f, pool := loaded.Fulfillment, loaded.Pool
		startGen := f.Generation()

		var result ReconciliationResult

		switch f.State() {
		case FulfillmentStateDeleting:
			if err := s.executeDelete(record, f, pool, fulfillmentID, probe, loaded.Evidence); err != nil {
				probe.Error(err)
				if errors.Is(err, errAuthPaused) {
					result = ReconciliationResult{
						FulfillmentID:   fulfillmentID,
						PauseReason:     err.Error(),
						ResolvedTargets: f.ResolvedTargets(),
						Auth:            f.Auth(),
					}
				} else if !IsTerminal(err) {
					return struct{}{}, s.releaseLockAndContinue(record, fulfillmentID, probe)
				} else {
					result = ReconciliationResult{
						FulfillmentID:   fulfillmentID,
						State:           FulfillmentStateFailed,
						ResolvedTargets: f.ResolvedTargets(),
						StatusReason:    err.Error(),
						Auth:            f.Auth(),
					}
				}
			} else {
				if _, err := RunActivity(record, s.CleanupDeliveryData(), fulfillmentID); err != nil {
					probe.Error(err)
					return struct{}{}, s.releaseLockAndContinue(record, fulfillmentID, probe)
				}
				result = ReconciliationResult{
					FulfillmentID: fulfillmentID,
					State:         FulfillmentStateDeleting,
					Auth:          f.Auth(),
				}
			}

		default:
			resolvedIDs, err := s.executePlacementPipeline(record, f, pool, fulfillmentID, startGen, probe, loaded.Evidence)
			if errors.Is(err, errAuthPaused) {
				probe.Error(err)
				result = ReconciliationResult{
					FulfillmentID:   fulfillmentID,
					PauseReason:     err.Error(),
					ResolvedTargets: resolvedIDs,
					Auth:            f.Auth(),
				}
			} else if err != nil {
				probe.Error(err)
				if !IsTerminal(err) {
					return struct{}{}, s.releaseLockAndContinue(record, fulfillmentID, probe)
				}
				result = ReconciliationResult{
					FulfillmentID:   fulfillmentID,
					State:           FulfillmentStateFailed,
					ResolvedTargets: resolvedIDs,
					StatusReason:    err.Error(),
					Auth:            f.Auth(),
				}
			} else {
				result = ReconciliationResult{
					FulfillmentID:   fulfillmentID,
					State:           FulfillmentStateActive,
					ResolvedTargets: resolvedIDs,
					Auth:            f.Auth(),
				}
			}
		}

		if result.PauseReason != "" {
			probe.StateChanged(f.State())
		} else {
			probe.StateChanged(result.State)
		}
		needsRestart, err := RunActivity(record, s.PersistAndCompleteReconciliation(), PersistAndCompleteInput{
			Result:        result,
			ReconciledGen: startGen,
		})
		if err != nil {
			probe.Error(err)
			return struct{}{}, s.releaseLockAndContinue(record, fulfillmentID, probe)
		}

		if !needsRestart {
			return struct{}{}, nil
		}
		probe.ReconciliationRestarting(startGen)
	}
}

// releaseLockAndContinue releases the orchestration lock and returns a
// [ContinueAsNew] error to restart with a fresh history.
func (s *OrchestrationWorkflowSpec) releaseLockAndContinue(
	record Record,
	fulfillmentID FulfillmentID,
	probe FulfillmentRunProbe,
) error {
	if _, err := RunActivity(record, s.ReleaseLock(), fulfillmentID); err != nil {
		probe.Error(err)
	}
	probe.ContinueAsNewTriggered()
	return ContinueAsNew(fulfillmentID)
}

// executePlacementPipeline runs the full resolve → delta → plan → execute
// pipeline and returns the new resolved target IDs.
func (s *OrchestrationWorkflowSpec) executePlacementPipeline(
	record Record,
	f Fulfillment,
	pool []TargetInfo,
	fulfillmentID FulfillmentID,
	startGen Generation,
	probe FulfillmentRunProbe,
	evidence *ResolvedEvidence,
) ([]TargetID, error) {
	resolved, err := RunActivity(record, s.ResolvePlacement(), ResolvePlacementInput{
		Spec: f.PlacementStrategy(),
		Pool: PlacementTargets(pool),
	})
	if err != nil {
		return nil, fmt.Errorf("resolve placement: %w", err)
	}

	if len(resolved) == 0 {
		return nil, nil
	}

	ids := make([]TargetID, len(resolved))
	for i, t := range resolved {
		ids[i] = t.ID
	}

	resolvedTargets := ResolvedTargetInfos(resolved, pool)
	delta := ComputeTargetDelta(f.ResolvedTargets(), resolvedTargets, pool)

	plan, err := RunActivity(record, s.PlanRollout(), PlanRolloutInput{
		Spec:  f.RolloutStrategy(),
		Delta: delta,
	})
	if err != nil {
		return ids, fmt.Errorf("plan rollout: %w", err)
	}

	if err := s.executeRolloutPlan(record, f, plan, fulfillmentID, startGen, probe, evidence); err != nil {
		return ids, err
	}

	return ids, nil
}

// executeDelete removes the fulfillment from all currently resolved
// targets. Delivery-data cleanup is handled by [CleanupDeliveryData]
// in the DELETING branch of [Run]. The delete-ready signal and the
// actual row hard-deletes are handled by
// [PersistAndCompleteReconciliation] and an abstraction-specific
// cleanup workflow respectively.
func (s *OrchestrationWorkflowSpec) executeDelete(
	record Record,
	f Fulfillment,
	pool []TargetInfo,
	fulfillmentID FulfillmentID,
	probe FulfillmentRunProbe,
	evidence *ResolvedEvidence,
) error {
	targets := targetInfosByID(f.ResolvedTargets(), pool)
	probe.DeleteStarted(len(targets))

	inputs := make(map[DeliveryID]RemoveInput, len(targets))
	ids := make([]DeliveryID, 0, len(targets))
	for _, target := range targets {
		in := RemoveInput{
			Target:        target,
			DeliveryID:    deliveryIDFor(fulfillmentID, target.ID()),
			FulfillmentID: fulfillmentID,
			Auth:          f.Auth(),
			Generation:    f.Generation(),
		}
		if evidence != nil {
			in.Attestation = assembleRemoveAttestation(f, evidence)
		}
		inputs[in.DeliveryID] = in
		ids = append(ids, in.DeliveryID)
	}

	_, err := s.dispatchAndAwait(record, func(id DeliveryID) (bool, error) {
		out, err := RunActivity(record, s.RemoveFromTarget(), inputs[id])
		if err != nil {
			if errors.Is(err, ErrAuthExpired) {
				return false, fmt.Errorf("%w: target %s: %v", errAuthPaused, inputs[id].Target.ID(), err)
			}
			return false, fmt.Errorf("remove delivery for target %s: %w", inputs[id].Target.ID(), err)
		}
		return out.Dispatched, nil
	}, ids, f.Generation(), probe)
	return err
}

// executeRolloutPlan runs each step in a [RolloutPlan]. For deliver
// steps it dispatches all deliveries, then waits for every delivery in
// the step to reach a terminal state before proceeding to the next step.
// Between steps, it checks whether the fulfillment's generation has
// advanced; if so and the [VersionConflictPolicy] is restart, it aborts
// so the next reconciliation can start fresh.
func (s *OrchestrationWorkflowSpec) executeRolloutPlan(
	record Record,
	f Fulfillment,
	plan RolloutPlan,
	fulfillmentID FulfillmentID,
	startGen Generation,
	probe FulfillmentRunProbe,
	evidence *ResolvedEvidence,
) error {
	policy := f.RolloutStrategy().EffectiveVersionConflictPolicy()

	for i, step := range plan.Steps {
		probe.RolloutStepStarted(i, len(plan.Steps), step.Deliver != nil)

		if step.Remove != nil {
			removeInputs := make(map[DeliveryID]RemoveInput)
			removeIDs := make([]DeliveryID, 0, len(step.Remove.Targets))
			for _, target := range step.Remove.Targets {
				// TODO: need to call the manifest generator on remove hook
				in := RemoveInput{
					Target:        target,
					DeliveryID:    deliveryIDFor(fulfillmentID, target.ID()),
					FulfillmentID: fulfillmentID,
					Auth:          f.Auth(),
					Generation:    startGen,
				}
				if evidence != nil {
					in.Attestation = assembleRemoveAttestation(f, evidence)
				}
				removeInputs[in.DeliveryID] = in
				removeIDs = append(removeIDs, in.DeliveryID)
			}

			if _, err := s.dispatchAndAwait(record, func(id DeliveryID) (bool, error) {
				out, err := RunActivity(record, s.RemoveFromTarget(), removeInputs[id])
				if err != nil {
					return false, fmt.Errorf("remove delivery for target %s: %w", removeInputs[id].Target.ID(), err)
				}
				return out.Dispatched, nil
			}, removeIDs, startGen, probe); err != nil {
				return err
			}
		}
		if step.Deliver != nil {
			// Phase 1: build inputs — generate manifests and assemble DeliverInput per target.
			inputs := make(map[DeliveryID]DeliverInput)
			for _, target := range step.Deliver.Targets {
				manifests, err := RunActivity(record, s.GenerateManifests(), GenerateManifestsInput{
					Spec:   f.ManifestStrategy(),
					Target: target,
				})
				if err != nil {
					return fmt.Errorf("generate manifests for target %s: %w", target.ID(), err)
				}
				// TODO: partial delivery (where some manifests are filtered out)
				// may result in an incomplete or incoherent manifest set for a
				// target. Revisit whether to warn or make this configurable.
				total := len(manifests)
				manifests = FilterAcceptedManifests(target, manifests)
				probe.ManifestsFiltered(target, total, len(manifests))
				if len(manifests) == 0 {
					continue
				}
				did := deliveryIDFor(fulfillmentID, target.ID())
				in := DeliverInput{
					Target:        target,
					DeliveryID:    did,
					FulfillmentID: fulfillmentID,
					Manifests:     manifests,
					Auth:          f.Auth(),
					Generation:    startGen,
				}
				if evidence != nil {
					in.Attestation = AssembleDeliverAttestation(f, manifests, evidence)
				}
				inputs[did] = in
			}

			// Phase 2: dispatch + ack + complete loop.
			ids := make([]DeliveryID, 0, len(inputs))
			for id := range inputs {
				ids = append(ids, id)
			}
			results, err := s.dispatchAndAwait(record, func(id DeliveryID) (bool, error) {
				_, err := RunActivity(record, s.DeliverToTarget(), inputs[id])
				return err == nil, err
			}, ids, startGen, probe)
			if err != nil {
				return err
			}
			for _, result := range results {
				if len(result.Result.ProvisionedTargets) > 0 || len(result.Result.ProducedSecrets) > 0 {
					if _, err := RunActivity(record, s.ProcessDeliveryOutputs(), DeliveryOutputsInput{
						DeliveryID: result.DeliveryID,
						Result:     result.Result,
					}); err != nil {
						return fmt.Errorf("process delivery outputs: %w", err)
					}
				}
			}
		}

		if i < len(plan.Steps)-1 && policy == VersionConflictRestart {
			currentGen, err := RunActivity(record, s.CheckGeneration(), fulfillmentID)
			if err != nil {
				return fmt.Errorf("check generation: %w", err)
			}
			if currentGen > startGen {
				probe.GenerationAdvancedMidRollout(startGen, currentGen)
				return nil
			}
		}
	}
	return nil
}

// dispatchAndAwait dispatches all targets, waits for acks (with retry
// on timeout), then waits for all completions. The dispatch closure is
// called for each unacknowledged delivery ID on every retry. It returns
// (true, nil) when the target was dispatched, or (false, nil) to
// indicate the target should be skipped (e.g. no delivery record
// exists). Skipped targets are removed from tracking immediately.
// Returns accumulated results once every tracked delivery has reached
// a terminal state.
//
// The expectedGen parameter identifies the generation this dispatch
// cycle is operating on. Events whose generation does not match are
// silently discarded to prevent stale signals (from a prior generation
// whose delivery ID is the same) from affecting the current cycle.
func (s *OrchestrationWorkflowSpec) dispatchAndAwait(
	record Record,
	dispatch func(DeliveryID) (bool, error),
	ids []DeliveryID,
	expectedGen Generation,
	runProbe FulfillmentRunProbe,
) ([]DeliveryCompletionEvent, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	probe := runProbe.DispatchCycleStarted(len(ids), expectedGen)
	defer probe.End()

	unacked := make(map[DeliveryID]struct{}, len(ids))
	remaining := make(map[DeliveryID]struct{}, len(ids))
	for _, id := range ids {
		unacked[id] = struct{}{}
		remaining[id] = struct{}{}
	}
	var results []DeliveryCompletionEvent

	iteration := 0
	for len(remaining) > 0 {
		for id := range unacked {
			dispatched, err := dispatch(id)
			if err != nil {
				probe.Error(err)
				return nil, err
			}
			if !dispatched {
				probe.Skipped(id)
				delete(unacked, id)
				delete(remaining, id)
			} else {
				probe.Dispatched(id, iteration > 0)
			}
		}
		if len(remaining) == 0 {
			break
		}

		for len(remaining) > 0 {
			var event FulfillmentEvent
			var err error
			if len(unacked) > 0 {
				event, err = AwaitSignalWithTimeout(record, FulfillmentEventSignal, s.ackRetryInterval())
				if errors.Is(err, ErrSignalTimeout) {
					probe.AckTimeout(len(unacked))
					break // back to outer loop to re-dispatch unacked
				}
			} else {
				event, err = AwaitSignal(record, FulfillmentEventSignal)
			}
			if err != nil {
				probe.Error(err)
				return nil, fmt.Errorf("await delivery signal: %w", err)
			}
			if err := s.processDeliveryEvent(event, expectedGen, unacked, remaining, &results, probe); err != nil {
				return nil, err
			}
		}
		iteration++
	}
	return results, nil
}

// processDeliveryEvent handles a single FulfillmentEvent, updating the
// unacked/remaining tracking maps and accumulating results. Events for
// deliveries not in the current batch or whose generation does not
// match expectedGen are silently ignored to prevent stale or unrelated
// signals from affecting this dispatch cycle.
func (s *OrchestrationWorkflowSpec) processDeliveryEvent(
	event FulfillmentEvent,
	expectedGen Generation,
	unacked map[DeliveryID]struct{},
	remaining map[DeliveryID]struct{},
	results *[]DeliveryCompletionEvent,
	probe DispatchCycleProbe,
) error {
	if event.DeliveryAcked != nil {
		if event.DeliveryAcked.Generation != expectedGen {
			probe.StaleEventDiscarded(event, expectedGen)
			return nil
		}
		if _, ok := remaining[event.DeliveryAcked.DeliveryID]; ok {
			delete(unacked, event.DeliveryAcked.DeliveryID)
			probe.AckReceived(event.DeliveryAcked.DeliveryID)
		}
	}
	if event.DeliveryCompleted != nil {
		if event.DeliveryCompleted.Generation != expectedGen {
			probe.StaleEventDiscarded(event, expectedGen)
			return nil
		}
		did := event.DeliveryCompleted.DeliveryID
		if _, ok := remaining[did]; !ok {
			return nil
		}
		delete(unacked, did)
		delete(remaining, did)
		probe.Completed(did, event.DeliveryCompleted.Result.State)
		switch event.DeliveryCompleted.Result.State {
		case DeliveryStateFailed:
			return fmt.Errorf("delivery %s failed: %s",
				did, event.DeliveryCompleted.Result.Message)
		case DeliveryStateAuthFailed:
			return fmt.Errorf("%w: delivery %s: %s",
				errAuthPaused, did, event.DeliveryCompleted.Result.Message)
		}
		*results = append(*results, *event.DeliveryCompleted)
	}
	return nil
}

func buildOutputCleanupPlan(deliveries []Delivery, items []InventoryItem) (outputCleanupPlan, error) {
	deliveryIDs := make(map[DeliveryID]struct{}, len(deliveries))
	for _, delivery := range deliveries {
		deliveryIDs[delivery.ID()] = struct{}{}
	}

	targetIDs := make(map[TargetID]struct{})
	inventoryIDs := make(map[InventoryItemID]struct{})
	secretRefs := make(map[SecretRef]struct{})

	for _, item := range items {
		if item.SourceDeliveryID() == nil {
			continue
		}
		if _, ok := deliveryIDs[*item.SourceDeliveryID()]; !ok {
			continue
		}

		inventoryIDs[item.ID()] = struct{}{}
		refs, err := secretRefsFromProperties(item.ID(), item.Properties())
		if err != nil {
			return outputCleanupPlan{}, err
		}
		for _, ref := range refs {
			secretRefs[ref] = struct{}{}
		}
		if targetID, ok := targetIDFromInventoryItem(item.ID()); ok {
			targetIDs[targetID] = struct{}{}
		}
	}

	return outputCleanupPlan{
		targetIDs:    sortedKeys(targetIDs),
		inventoryIDs: sortedKeys(inventoryIDs),
		secretRefs:   sortedKeys(secretRefs),
	}, nil
}

func targetIDFromInventoryItem(id InventoryItemID) (TargetID, bool) {
	const prefix = "target:"
	if !strings.HasPrefix(string(id), prefix) {
		return "", false
	}
	return TargetID(strings.TrimPrefix(string(id), prefix)), true
}

func secretRefsFromProperties(id InventoryItemID, raw json.RawMessage) ([]SecretRef, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	var props map[string]string
	if err := json.Unmarshal(raw, &props); err != nil {
		return nil, fmt.Errorf("parse properties for inventory item %q: %w", id, err)
	}

	refs := make([]SecretRef, 0)
	for key, value := range props {
		if strings.HasSuffix(key, "_ref") && value != "" {
			refs = append(refs, SecretRef(value))
		}
	}
	return refs, nil
}

func sortedKeys[K ~string](set map[K]struct{}) []K {
	keys := make([]K, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	// Map iteration order is non-deterministic; stable ordering keeps the
	// cleanup activity deterministic under replay.
	if len(keys) > 1 {
		slices.Sort(keys)
	}
	return keys
}

// deliveryIDFor produces a deterministic [DeliveryID] for a
// fulfillment-target pair. This keeps IDs stable across re-deliveries
// to the same target, which is the current one-delivery-per-pair model.
// TODO: does this need to be deterministic? Do we actually want different IDs on redelivery?
func deliveryIDFor(fID FulfillmentID, tgtID TargetID) DeliveryID {
	return DeliveryID(string(fID) + ":" + string(tgtID))
}

// targetInfosByID looks up each id in pool and returns the matching
// [TargetInfo] values. Unknown IDs are silently skipped.
func targetInfosByID(ids []TargetID, pool []TargetInfo) []TargetInfo {
	index := make(map[TargetID]TargetInfo, len(pool))
	for _, t := range pool {
		index[t.ID()] = t
	}
	out := make([]TargetInfo, 0, len(ids))
	for _, id := range ids {
		if t, ok := index[id]; ok {
			out = append(out, t)
		}
	}
	return out
}

// ComputeTargetDelta calculates the difference between the previous
// resolved target set and the newly resolved set.
func ComputeTargetDelta(previousIDs []TargetID, resolved []TargetInfo, pool []TargetInfo) TargetDelta {
	prevSet := make(map[TargetID]struct{}, len(previousIDs))
	for _, id := range previousIDs {
		prevSet[id] = struct{}{}
	}

	resolvedSet := make(map[TargetID]struct{}, len(resolved))
	for _, t := range resolved {
		resolvedSet[t.ID()] = struct{}{}
	}

	var delta TargetDelta
	for _, t := range resolved {
		if _, wasPrevious := prevSet[t.ID()]; wasPrevious {
			delta.Unchanged = append(delta.Unchanged, t)
		} else {
			delta.Added = append(delta.Added, t)
		}
	}

	poolIndex := make(map[TargetID]TargetInfo, len(pool))
	for _, t := range pool {
		poolIndex[t.ID()] = t
	}
	for _, id := range previousIDs {
		if _, stillResolved := resolvedSet[id]; !stillResolved {
			if t, ok := poolIndex[id]; ok {
				delta.Removed = append(delta.Removed, t)
			} else {
				delta.Removed = append(delta.Removed, TargetInfoFromSnapshot(TargetInfoSnapshot{ID: id}))
			}
		}
	}

	return delta
}

// AssembleDeliverAttestation builds an [Attestation] for a delivery
// from the fulfillment's provenance, resolved signer evidence, and
// the manifests being delivered.
func AssembleDeliverAttestation(f Fulfillment, manifests []Manifest, ev *ResolvedEvidence) *Attestation {
	return &Attestation{
		Input: SignedInput{
			Provenance: *f.Provenance(),
			Signer:     *ev.SignerAssertion,
		},
		SignedRelation: ev.SignedRelation,
		Output: &PutManifests{
			Manifests: manifests,
		},
	}
}

func assembleRemoveAttestation(f Fulfillment, ev *ResolvedEvidence) *Attestation {
	return &Attestation{
		Input: SignedInput{
			Provenance: *f.Provenance(),
			Signer:     *ev.SignerAssertion,
		},
		SignedRelation: ev.SignedRelation,
		Output: &RemoveByDeploymentName{
			Name: ResourceName(f.Provenance().Content.ContentID()),
		},
	}
}

// unused import guard
var _ = uuid.New
