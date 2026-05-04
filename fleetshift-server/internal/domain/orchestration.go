package domain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// errAuthPaused is a sentinel error returned by [awaitDeliveries] when
// a delivery reports [DeliveryStateAuthFailed]. The orchestration
// catches this to transition to [FulfillmentStatePausedAuth] and
// complete the workflow.
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
}

// RemoveInput is the input to the remove-from-target activity.
type RemoveInput struct {
	Target        TargetInfo
	DeliveryID    DeliveryID
	FulfillmentID FulfillmentID
	Manifests     []Manifest
	Auth          DeliveryAuth
	Attestation   *Attestation // nil for token-passthrough deliveries
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

// FulfillmentAndPool is the result of loading a fulfillment and the target pool
// in a single step. Used to avoid separate durable steps for fulfillment and pool.
// When the fulfillment carries [Provenance], the signer enrollment is also
// resolved eagerly so attestation assembly is deterministic on replay.
type FulfillmentAndPool struct {
	Fulfillment     Fulfillment
	Pool            []TargetInfo
	SignerAssertion *SignerAssertion
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
	Store            Store
	Delivery         DeliveryService
	Strategies       StrategyFactory
	Registry         Registry
	Observer         FulfillmentObserver
	DeliveryObserver DeliveryObserver
	Vault            Vault
	Now              func() time.Time
}

func (s *OrchestrationWorkflowSpec) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *OrchestrationWorkflowSpec) Name() string { return "orchestrate-fulfillment" }

// Each method returns a typed [Activity] derived from the spec's own
// dependencies. Infrastructure adapters call these to register activities;
// the workflow body calls them via [RunActivity].

// AcquireLockAndLoad acquires the orchestration lock (if not already
// held) and loads the fulfillment and target pool in a single activity.
// On the first call the lock is claimed; on subsequent calls (within
// the same workflow execution) it is already held so only the load
// happens. This combines the former AcquireLock and LoadFulfillmentAndPool
// activities to eliminate a redundant fulfillment read.
func (s *OrchestrationWorkflowSpec) AcquireLockAndLoad() Activity[FulfillmentID, FulfillmentAndPool] {
	return NewActivity("acquire-lock-and-load", func(ctx context.Context, id FulfillmentID) (FulfillmentAndPool, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return FulfillmentAndPool{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		f, err := tx.Fulfillments().Get(ctx, id)
		if err != nil {
			return FulfillmentAndPool{}, err
		}
		if f.AcquireOrchestrationLock() {
			if err := tx.Fulfillments().Update(ctx, f); err != nil {
				return FulfillmentAndPool{}, err
			}
		}

		pool, err := tx.Targets().List(ctx)
		if err != nil {
			return FulfillmentAndPool{}, err
		}

		var sa *SignerAssertion
		if f.Provenance != nil {
			found, err := tx.SignerEnrollments().ListBySubject(ctx, f.Provenance.Sig.Signer)
			if err != nil {
				return FulfillmentAndPool{}, fmt.Errorf("list signer enrollments: %w", err)
			}
			if len(found) == 0 {
				return FulfillmentAndPool{}, fmt.Errorf("no signer enrollment found for %s / %s",
					f.Provenance.Sig.Signer.Subject, f.Provenance.Sig.Signer.Issuer)
			}
			enrollment := found[0]
			sa = &SignerAssertion{
				IdentityToken:   enrollment.IdentityToken,
				RegistryID:      enrollment.RegistryID,
				RegistrySubject: enrollment.RegistrySubject,
			}
		}

		if err := tx.Commit(); err != nil {
			return FulfillmentAndPool{}, fmt.Errorf("commit: %w", err)
		}
		return FulfillmentAndPool{Fulfillment: f, Pool: pool, SignerAssertion: sa}, nil
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
// [Delivery] record in [DeliveryStatePending], creates a
// [DeliverySignaler] for lifecycle state transitions and workflow
// signaling, then dispatches to the [DeliveryService].
//
// The delivery receives [context.Background] rather than the activity
// context. Delivery agents may run asynchronously (returning
// immediately and completing in a background goroutine), and the
// activity context is canceled once go-workflows completes the
// activity task. This matches the production architecture where
// delivery runs on a remote fleetlet with its own context; trace
// propagation across the boundary is done explicitly, not via Go
// context inheritance.
func (s *OrchestrationWorkflowSpec) DeliverToTarget() Activity[DeliverInput, DeliveryResult] {
	return NewActivity("deliver-to-target", func(ctx context.Context, in DeliverInput) (DeliveryResult, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return DeliveryResult{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		now := s.now()
		if err := tx.Deliveries().Put(ctx, Delivery{
			ID:            in.DeliveryID,
			FulfillmentID: in.FulfillmentID,
			TargetID:      in.Target.ID,
			Manifests:     in.Manifests,
			State:         DeliveryStatePending,
			CreatedAt:     now,
			UpdatedAt:     now,
		}); err != nil {
			return DeliveryResult{}, fmt.Errorf("create delivery record: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return DeliveryResult{}, fmt.Errorf("commit: %w", err)
		}

		signaler := NewDeliverySignaler(
			in.FulfillmentID, in.DeliveryID, in.Target,
			s.Store, s.Registry.SignalFulfillmentEvent,
			s.DeliveryObserver,
		)

		return s.Delivery.Deliver(context.Background(), in.Target, in.DeliveryID, in.Manifests, in.Auth, in.Attestation, signaler)
	})
}

// RemoveFromTarget loads the delivery record for a target (to get
// manifests) and calls the agent's Remove. If no delivery record
// exists (e.g., delivery failed before persisting), the target is
// skipped. The read transaction is closed before calling Remove so
// that the delivery agent can open write transactions without
// deadlocking on SQLite.
func (s *OrchestrationWorkflowSpec) RemoveFromTarget() Activity[RemoveInput, struct{}] {
	return NewActivity("remove-from-target", func(ctx context.Context, in RemoveInput) (struct{}, error) {
		tx, err := s.Store.BeginReadOnly(ctx)
		if err != nil {
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}

		delivery, err := tx.Deliveries().GetByFulfillmentTarget(ctx, in.FulfillmentID, in.Target.ID)
		tx.Rollback() // close before calling Remove
		if errors.Is(err, ErrNotFound) {
			return struct{}{}, nil
		}
		if err != nil {
			return struct{}{}, fmt.Errorf("load delivery record for target %s: %w", in.Target.ID, err)
		}

		return struct{}{}, s.Delivery.Remove(ctx, in.Target, in.DeliveryID, delivery.Manifests, in.Auth, in.Attestation, &DeliverySignaler{})
	})
}

// CleanupDeliveryData cleans up provisioned targets (e.g. kind
// clusters) and hard-deletes delivery records for a fulfillment. The
// fulfillment row itself is NOT deleted here; that responsibility
// belongs to [DeleteCleanupWorkflow], which deletes both the
// deployment and fulfillment rows in FK-safe order after receiving
// a [DeleteCleanupCompleteSignal].
func (s *OrchestrationWorkflowSpec) CleanupDeliveryData() Activity[FulfillmentID, struct{}] {
	return NewActivity("cleanup-delivery-data", func(ctx context.Context, id FulfillmentID) (struct{}, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		deliveries, err := tx.Deliveries().ListByFulfillment(ctx, id)
		if err != nil {
			return struct{}{}, fmt.Errorf("list deliveries: %w", err)
		}
		for _, d := range deliveries {
			target, err := tx.Targets().Get(ctx, d.TargetID)
			if err != nil {
				continue
			}
			if target.Type != "kind" {
				continue
			}
			for _, m := range d.Manifests {
				var spec struct{ Name string }
				if err := json.Unmarshal(m.Raw, &spec); err != nil || spec.Name == "" {
					continue
				}
				provID := TargetID("k8s-" + spec.Name)
				if err := tx.Targets().Delete(ctx, provID); err != nil && !errors.Is(err, ErrNotFound) {
					return struct{}{}, fmt.Errorf("delete provisioned target %s: %w", provID, err)
				}
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
// [DeleteCleanupWorkflow] after the commit succeeds. Combining the
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
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return false, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		fresh, err := tx.Fulfillments().Get(ctx, in.Result.FulfillmentID)
		if err != nil {
			return false, fmt.Errorf("get fulfillment: %w", err)
		}

		fresh.ApplyReconciliationResult(in.Result)
		needsRestart := fresh.CompleteReconciliation(in.ReconciledGen)
		fresh.UpdatedAt = s.now()

		if err := tx.Fulfillments().Update(ctx, fresh); err != nil {
			return false, err
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}

		if in.Result.State == FulfillmentStateDeleting {
			if err := s.Registry.SignalDeleteCleanupComplete(ctx, in.Result.FulfillmentID, DeleteCleanupCompleteEvent{
				FulfillmentID: in.Result.FulfillmentID,
			}); err != nil {
				return false, fmt.Errorf("signal delete cleanup: %w", err)
			}
		}

		return needsRestart, nil
	})
}

// ProcessDeliveryOutputs stores produced secrets in the [Vault] and
// registers provisioned targets. Secrets are stored first so that
// target properties referencing vault refs are valid at registration
// time. Results with no outputs are skipped.
//
// All writes use upsert semantics so the activity is replay-safe:
// delivery agents may be re-invoked after a transient failure and
// produce the same outputs on each attempt.
func (s *OrchestrationWorkflowSpec) ProcessDeliveryOutputs() Activity[DeliveryResult, struct{}] {
	return NewActivity("process-delivery-outputs", func(ctx context.Context, result DeliveryResult) (struct{}, error) {
		if len(result.ProducedSecrets) == 0 && len(result.ProvisionedTargets) == 0 {
			return struct{}{}, nil
		}

		if s.Vault != nil {
			for _, secret := range result.ProducedSecrets {
				if err := s.Vault.Put(ctx, secret.Ref, secret.Value); err != nil {
					return struct{}{}, fmt.Errorf("store secret %q: %w", secret.Ref, err)
				}
			}
		}

		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		// TODO: revisit the "TargetRegistrar" thing – should we make that upsert instead? remove that?
		now := s.now()
		for _, pt := range result.ProvisionedTargets {
			invID := InventoryItemID("target:" + string(pt.ID))

			props, _ := json.Marshal(pt.Properties)
			if err := tx.Inventory().CreateOrUpdate(ctx, InventoryItem{
				ID:         invID,
				Type:       InventoryType(pt.Type),
				Name:       pt.Name,
				Properties: props,
				Labels:     pt.Labels,
				CreatedAt:  now,
				UpdatedAt:  now,
			}); err != nil {
				return struct{}{}, fmt.Errorf("upsert inventory item for target %q: %w", pt.ID, err)
			}

			if err := tx.Targets().CreateOrUpdate(ctx, TargetInfo{
				ID:                    pt.ID,
				Type:                  pt.Type,
				Name:                  pt.Name,
				Labels:                pt.Labels,
				Properties:            pt.Properties,
				AcceptedResourceTypes: pt.AcceptedResourceTypes,
				InventoryItemID:       invID,
			}); err != nil {
				return struct{}{}, fmt.Errorf("upsert target %q: %w", pt.ID, err)
			}
		}
		return struct{}{}, tx.Commit()
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
		return f.Generation, tx.Commit()
	})
}

func (s *OrchestrationWorkflowSpec) observer() FulfillmentObserver {
	if s.Observer != nil {
		return s.Observer
	}
	return NoOpFulfillmentObserver{}
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
		startGen := f.Generation

		var result ReconciliationResult

		switch f.State {
		case FulfillmentStateDeleting:
			if err := s.executeDelete(record, f, pool, fulfillmentID, loaded.SignerAssertion); err != nil {
				probe.Error(err)
				if !IsTerminal(err) {
					return struct{}{}, s.releaseLockAndContinue(record, fulfillmentID, probe)
				}
				result = NewFailedResult(fulfillmentID, f.Auth, err.Error())
			} else {
				if _, err := RunActivity(record, s.CleanupDeliveryData(), fulfillmentID); err != nil {
					probe.Error(err)
					return struct{}{}, s.releaseLockAndContinue(record, fulfillmentID, probe)
				}
				result = ReconciliationResult{
					FulfillmentID: fulfillmentID,
					State:         FulfillmentStateDeleting,
					Auth:          f.Auth,
				}
			}

		default:
			resolvedIDs, err := s.executePlacementPipeline(record, f, pool, fulfillmentID, startGen, probe, loaded.SignerAssertion)
			if errors.Is(err, errAuthPaused) {
				probe.Error(err)
				result = NewPausedAuthResult(fulfillmentID, f.Auth)
			} else if err != nil {
				probe.Error(err)
				if !IsTerminal(err) {
					return struct{}{}, s.releaseLockAndContinue(record, fulfillmentID, probe)
				}
				result = NewFailedResult(fulfillmentID, f.Auth, err.Error())
			} else {
				result = NewActiveResult(fulfillmentID, resolvedIDs, f.Auth)
			}
		}

		probe.StateChanged(result.State)
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
	sa *SignerAssertion,
) ([]TargetID, error) {
	resolved, err := RunActivity(record, s.ResolvePlacement(), ResolvePlacementInput{
		Spec: f.PlacementStrategy,
		Pool: PlacementTargets(pool),
	})
	if err != nil {
		return nil, fmt.Errorf("resolve placement: %w", err)
	}

	if len(resolved) == 0 {
		return nil, nil
	}

	resolvedTargets := ResolvedTargetInfos(resolved, pool)
	delta := ComputeTargetDelta(f.ResolvedTargets, resolvedTargets, pool)

	plan, err := RunActivity(record, s.PlanRollout(), PlanRolloutInput{
		Spec:  f.RolloutStrategy,
		Delta: delta,
	})
	if err != nil {
		return nil, fmt.Errorf("plan rollout: %w", err)
	}

	if err := s.executeRolloutPlan(record, f, plan, fulfillmentID, startGen, probe, sa); err != nil {
		return nil, err
	}

	ids := make([]TargetID, len(resolved))
	for i, t := range resolved {
		ids[i] = t.ID
	}
	return ids, nil
}

// executeDelete removes the fulfillment from all currently resolved
// targets. Delivery-data cleanup is handled by [CleanupDeliveryData]
// in the DELETING branch of [Run]. The delete-ready signal and the
// actual row hard-deletes are handled by
// [PersistAndCompleteReconciliation] and [DeleteCleanupWorkflow]
// respectively.
func (s *OrchestrationWorkflowSpec) executeDelete(
	record Record,
	f Fulfillment,
	pool []TargetInfo,
	fulfillmentID FulfillmentID,
	sa *SignerAssertion,
) error {
	targets := targetInfosByID(f.ResolvedTargets, pool)
	for _, target := range targets {
		in := RemoveInput{
			Target:        target,
			DeliveryID:    deliveryIDFor(fulfillmentID, target.ID),
			FulfillmentID: fulfillmentID,
			Auth:          f.Auth,
		}
		if sa != nil {
			in.Attestation = assembleRemoveAttestation(f, *sa)
		}
		if _, err := RunActivity(record, s.RemoveFromTarget(), in); err != nil {
			return fmt.Errorf("remove delivery for target %s: %w", target.ID, err)
		}
	}

	return nil
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
	sa *SignerAssertion,
) error {
	policy := f.RolloutStrategy.EffectiveVersionConflictPolicy()

	for i, step := range plan.Steps {
		if step.Remove != nil {
			for _, target := range step.Remove.Targets {
				// TODO: need to call the manifest generator on remove hook
				in := RemoveInput{
					Target:        target,
					DeliveryID:    deliveryIDFor(fulfillmentID, target.ID),
					FulfillmentID: fulfillmentID,
					Auth:          f.Auth,
				}
				if sa != nil {
					in.Attestation = assembleRemoveAttestation(f, *sa)
				}
				if _, err := RunActivity(record, s.RemoveFromTarget(), in); err != nil {
					return fmt.Errorf("remove delivery for target %s: %w", target.ID, err)
				}
			}
		}
		if step.Deliver != nil {
			var pending []DeliveryID
			var syncResults []DeliveryResult
			for _, target := range step.Deliver.Targets {
				manifests, err := RunActivity(record, s.GenerateManifests(), GenerateManifestsInput{
					Spec:   f.ManifestStrategy,
					Target: target,
				})
				if err != nil {
					return fmt.Errorf("generate manifests for target %s: %w", target.ID, err)
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
				did := deliveryIDFor(fulfillmentID, target.ID)
				in := DeliverInput{
					Target:        target,
					DeliveryID:    did,
					FulfillmentID: fulfillmentID,
					Manifests:     manifests,
					Auth:          f.Auth,
				}
				if sa != nil {
					in.Attestation = assembleDeliverAttestation(f, manifests, *sa)
				}
				result, err := RunActivity(record, s.DeliverToTarget(), in)
				if err != nil {
					return fmt.Errorf("deliver to target %s: %w", target.ID, err)
				}
				switch result.State {
				case DeliveryStateAccepted:
					pending = append(pending, did)
				case DeliveryStateAuthFailed:
					return fmt.Errorf("%w: delivery %s: %s",
						errAuthPaused, did, result.Message)
				case DeliveryStateFailed:
					return fmt.Errorf("delivery %s failed: %s",
						did, result.Message)
				default:
					syncResults = append(syncResults, result)
				}
			}
			results, err := s.awaitDeliveries(record, pending)
			if err != nil {
				return err
			}
			results = append(results, syncResults...)
			for _, result := range results {
				if len(result.ProvisionedTargets) > 0 || len(result.ProducedSecrets) > 0 {
					if _, err := RunActivity(record, s.ProcessDeliveryOutputs(), result); err != nil {
						return fmt.Errorf("process delivery outputs: %w", err)
					}
					probe.DeliveryOutputsProcessed(result.ProvisionedTargets, len(result.ProducedSecrets))
				}
			}
		}

		if i < len(plan.Steps)-1 && policy == VersionConflictRestart {
			currentGen, err := RunActivity(record, s.CheckGeneration(), fulfillmentID)
			if err != nil {
				return fmt.Errorf("check generation: %w", err)
			}
			if currentGen > startGen {
				return nil
			}
		}
	}
	return nil
}

// awaitDeliveries blocks until every delivery in pending has completed
// and returns the completed results.
func (s *OrchestrationWorkflowSpec) awaitDeliveries(
	record Record,
	pending []DeliveryID,
) (results []DeliveryResult, err error) {
	remaining := make(map[DeliveryID]struct{}, len(pending))
	for _, id := range pending {
		remaining[id] = struct{}{}
	}

	for len(remaining) > 0 {
		event, err := AwaitSignal(record, FulfillmentEventSignal)
		if err != nil {
			return nil, fmt.Errorf("await delivery completion: %w", err)
		}
		if event.DeliveryCompleted == nil {
			continue
		}
		delete(remaining, event.DeliveryCompleted.DeliveryID)
		switch event.DeliveryCompleted.Result.State {
		case DeliveryStateFailed:
			return nil, fmt.Errorf("delivery %s failed: %s",
				event.DeliveryCompleted.DeliveryID,
				event.DeliveryCompleted.Result.Message)
		case DeliveryStateAuthFailed:
			return nil, fmt.Errorf("%w: delivery %s: %s",
				errAuthPaused,
				event.DeliveryCompleted.DeliveryID,
				event.DeliveryCompleted.Result.Message)
		}
		results = append(results, event.DeliveryCompleted.Result)
	}
	return results, nil
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
		index[t.ID] = t
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
		resolvedSet[t.ID] = struct{}{}
	}

	var delta TargetDelta
	for _, t := range resolved {
		if _, wasPrevious := prevSet[t.ID]; wasPrevious {
			delta.Unchanged = append(delta.Unchanged, t)
		} else {
			delta.Added = append(delta.Added, t)
		}
	}

	poolIndex := make(map[TargetID]TargetInfo, len(pool))
	for _, t := range pool {
		poolIndex[t.ID] = t
	}
	for _, id := range previousIDs {
		if _, stillResolved := resolvedSet[id]; !stillResolved {
			if t, ok := poolIndex[id]; ok {
				delta.Removed = append(delta.Removed, t)
			} else {
				delta.Removed = append(delta.Removed, TargetInfo{ID: id})
			}
		}
	}

	return delta
}

func assembleDeliverAttestation(f Fulfillment, manifests []Manifest, sa SignerAssertion) *Attestation {
	return &Attestation{
		Input: SignedInput{
			Provenance: *f.Provenance,
			Signer:     sa,
		},
		Output: &PutManifests{
			Manifests: manifests,
		},
	}
}

func assembleRemoveAttestation(f Fulfillment, sa SignerAssertion) *Attestation {
	return &Attestation{
		Input: SignedInput{
			Provenance: *f.Provenance,
			Signer:     sa,
		},
		Output: &RemoveByDeploymentId{
			DeploymentID: DeploymentID(f.Provenance.Content.ContentID()),
		},
	}
}

// unused import guard
var _ = uuid.New
