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
// catches this to transition to [DeploymentStatePausedAuth] and
// complete the workflow.
var errAuthPaused = errors.New("delivery auth failed: pausing for fresh credentials")

// TargetDelta represents the difference between the previous and current
// resolved target sets for a deployment.
type TargetDelta struct {
	Added     []TargetInfo
	Removed   []TargetInfo
	Unchanged []TargetInfo
}

// RolloutStep is a single step in a rollout plan: either remove from targets
// or deliver to targets. Exactly one of Remove and Deliver is non-nil.
type RolloutStep struct {
	Remove  *RolloutStepRemove  // remove deployment from these targets
	Deliver *RolloutStepDeliver // generate and deliver to these targets
}

// RolloutStepRemove is a step that removes the deployment from the listed targets.
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
	Target       TargetInfo
	DeliveryID   DeliveryID
	DeploymentID DeploymentID
	Manifests    []Manifest
	Auth         DeliveryAuth
	Attestation  *Attestation // nil for token-passthrough deliveries
}

// RemoveInput is the input to the remove-from-target activity.
type RemoveInput struct {
	Target       TargetInfo
	DeliveryID   DeliveryID
	DeploymentID DeploymentID
	Manifests    []Manifest
	Auth         DeliveryAuth
	Attestation  *Attestation // nil for token-passthrough deliveries
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

// DeploymentAndPool is the result of loading a deployment and the target pool
// in a single step. Used to avoid separate durable steps for deployment and pool.
type DeploymentAndPool struct {
	Deployment Deployment
	Pool       []TargetInfo
}

// CompleteReconciliationInput carries the deployment ID and the generation
// that was reconciled so the activity can atomically decide whether a new
// reconciliation is needed.
type CompleteReconciliationInput struct {
	DeploymentID  DeploymentID
	ReconciledGen Generation
}

// CleanupProvisionedTargetsInput is the input to the cleanup-provisioned-targets activity.
type CleanupProvisionedTargetsInput struct {
	DeploymentID DeploymentID
}

// OrchestrationWorkflowSpec is the deployment pipeline expressed as a
// deterministic workflow. Each reconciliation loads the current state,
// runs the full pipeline (or delete), and atomically completes. If
// the deployment's [Generation] has advanced during execution the
// workflow loops and re-runs the pipeline.
//
// Pass this spec to [Registry.RegisterOrchestration] to obtain an
// [OrchestrationWorkflow] that can start instances.
type OrchestrationWorkflowSpec struct {
	Store            Store
	Delivery         DeliveryService
	Strategies       StrategyFactory
	Registry         Registry
	Observer         DeploymentObserver
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

func (s *OrchestrationWorkflowSpec) Name() string { return "orchestrate-deployment" }

// Each method returns a typed [Activity] derived from the spec's own
// dependencies. Infrastructure adapters call these to register activities;
// the workflow body calls them via [RunActivity].

// LoadDeploymentAndPool reads the deployment and target pool in a single
// activity to avoid separate durable steps. Used at workflow start and when
// reloading after a spec change.
func (s *OrchestrationWorkflowSpec) LoadDeploymentAndPool() Activity[DeploymentID, DeploymentAndPool] {
	return NewActivity("load-deployment-and-pool", func(ctx context.Context, id DeploymentID) (DeploymentAndPool, error) {
		tx, err := s.Store.BeginReadOnly(ctx)
		if err != nil {
			return DeploymentAndPool{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		dep, err := tx.Deployments().Get(ctx, id)
		if err != nil {
			return DeploymentAndPool{}, err
		}
		pool, err := tx.Targets().List(ctx)
		if err != nil {
			return DeploymentAndPool{}, err
		}
		if err := tx.Commit(); err != nil {
			return DeploymentAndPool{}, fmt.Errorf("commit: %w", err)
		}
		return DeploymentAndPool{Deployment: dep, Pool: pool}, nil
	})
}

// ResolvePlacement runs the deployment's placement strategy against the
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

// PlanRollout runs the deployment's rollout strategy to produce an
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
			ID:           in.DeliveryID,
			DeploymentID: in.DeploymentID,
			TargetID:     in.Target.ID,
			Manifests:    in.Manifests,
			State:        DeliveryStatePending,
			CreatedAt:    now,
			UpdatedAt:    now,
		}); err != nil {
			return DeliveryResult{}, fmt.Errorf("create delivery record: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return DeliveryResult{}, fmt.Errorf("commit: %w", err)
		}

		signaler := NewDeliverySignaler(
			in.DeploymentID, in.DeliveryID, in.Target,
			s.Store, s.Registry.SignalDeploymentEvent,
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

		delivery, err := tx.Deliveries().GetByDeploymentTarget(ctx, in.DeploymentID, in.Target.ID)
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

// DeleteDeploymentRecord hard-deletes delivery records and the deployment record.
func (s *OrchestrationWorkflowSpec) DeleteDeploymentRecord() Activity[DeploymentID, struct{}] {
	return NewActivity("delete-deployment-record", func(ctx context.Context, id DeploymentID) (struct{}, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		if err := tx.Deliveries().DeleteByDeployment(ctx, id); err != nil {
			return struct{}{}, fmt.Errorf("delete delivery records: %w", err)
		}
		if err := tx.Deployments().Delete(ctx, id); err != nil {
			return struct{}{}, fmt.Errorf("delete deployment: %w", err)
		}
		return struct{}{}, tx.Commit()
	})
}

// CleanupProvisionedTargets removes targets that were provisioned by
// deliveries of this deployment (e.g. kind clusters).
func (s *OrchestrationWorkflowSpec) CleanupProvisionedTargets() Activity[CleanupProvisionedTargetsInput, struct{}] {
	return NewActivity("cleanup-provisioned-targets", func(ctx context.Context, in CleanupProvisionedTargetsInput) (struct{}, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		deliveries, err := tx.Deliveries().ListByDeployment(ctx, in.DeploymentID)
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
		return struct{}{}, tx.Commit()
	})
}

// PersistReconciliationResult persists a [ReconciliationResult] using
// a fresh read-modify-write cycle. The activity reads the latest
// aggregate so that concurrent generation bumps by the service layer
// are preserved.
func (s *OrchestrationWorkflowSpec) PersistReconciliationResult() Activity[ReconciliationResult, struct{}] {
	return NewActivity("persist-reconciliation-result", func(ctx context.Context, r ReconciliationResult) (struct{}, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		fresh, err := tx.Deployments().Get(ctx, r.DeploymentID)
		if err != nil {
			return struct{}{}, fmt.Errorf("get deployment: %w", err)
		}

		fresh.ApplyReconciliationResult(r)
		fresh.UpdatedAt = s.now()
		fresh.Etag = uuid.New().String()

		if err := tx.Deployments().Update(ctx, fresh); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, tx.Commit()
	})
}

// ProcessDeliveryOutputs stores produced secrets in the [Vault] and
// registers provisioned targets. Secrets are stored first so that
// target properties referencing vault refs are valid at registration
// time. Results with no outputs are skipped.
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

		reg := &TargetRegistrar{
			Targets:   tx.Targets(),
			Inventory: tx.Inventory(),
		}
		for _, pt := range result.ProvisionedTargets {
			if err := reg.Register(ctx, TargetInfo{
				ID:                    pt.ID,
				Type:                  pt.Type,
				Name:                  pt.Name,
				Labels:                pt.Labels,
				Properties:            pt.Properties,
				AcceptedResourceTypes: pt.AcceptedResourceTypes,
			}); err != nil {
				return struct{}{}, fmt.Errorf("register target %q: %w", pt.ID, err)
			}
		}
		return struct{}{}, tx.Commit()
	})
}

// AcquireLock sets [Deployment.ActiveWorkflowGen] to the deployment's
// current generation, claiming the orchestration lock. Returns the
// acquired generation. If the lock is already held, this is a
// programming error (the engine's at-most-one instance guarantee
// should prevent it).
//
// NOTE: recovery from a stale lock (orchestration failed after
// acquiring) is deferred follow-up work.
func (s *OrchestrationWorkflowSpec) AcquireLock() Activity[DeploymentID, Generation] {
	return NewActivity("acquire-orchestration-lock", func(ctx context.Context, id DeploymentID) (Generation, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return 0, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		dep, err := tx.Deployments().Get(ctx, id)
		if err != nil {
			return 0, err
		}
		if !dep.AcquireOrchestrationLock() {
			return 0, fmt.Errorf("deployment %q: orchestration lock already held (gen %d)", id, *dep.ActiveWorkflowGen)
		}
		if err := tx.Deployments().Update(ctx, dep); err != nil {
			return 0, err
		}
		return dep.Generation, tx.Commit()
	})
}

// ReleaseLock clears [Deployment.ActiveWorkflowGen] without advancing
// [Deployment.ObservedGeneration]. Used before ContinueAsNew so the
// next execution can re-acquire the lock for a fresh retry attempt.
func (s *OrchestrationWorkflowSpec) ReleaseLock() Activity[DeploymentID, struct{}] {
	return NewActivity("release-orchestration-lock", func(ctx context.Context, id DeploymentID) (struct{}, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		dep, err := tx.Deployments().Get(ctx, id)
		if err != nil {
			return struct{}{}, err
		}
		dep.ReleaseOrchestrationLock()
		if err := tx.Deployments().Update(ctx, dep); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, tx.Commit()
	})
}

// CheckGeneration reads the deployment's current generation from the
// store. Used mid-rollout to detect whether a new mutation has arrived.
func (s *OrchestrationWorkflowSpec) CheckGeneration() Activity[DeploymentID, Generation] {
	return NewActivity("check-generation", func(ctx context.Context, id DeploymentID) (Generation, error) {
		tx, err := s.Store.BeginReadOnly(ctx)
		if err != nil {
			return 0, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		dep, err := tx.Deployments().Get(ctx, id)
		if err != nil {
			return 0, err
		}
		return dep.Generation, tx.Commit()
	})
}

// CompleteReconciliation advances [Deployment.ObservedGeneration] to
// reconciledGen. If the deployment's generation has advanced past
// reconciledGen during the workflow run, needsRestart is returned as
// true, indicating the caller should loop.
func (s *OrchestrationWorkflowSpec) CompleteReconciliation() Activity[CompleteReconciliationInput, bool] {
	return NewActivity("complete-reconciliation", func(ctx context.Context, in CompleteReconciliationInput) (bool, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return false, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		dep, err := tx.Deployments().Get(ctx, in.DeploymentID)
		if err != nil {
			return false, err
		}
		needsRestart := dep.CompleteReconciliation(in.ReconciledGen)
		if err := tx.Deployments().Update(ctx, dep); err != nil {
			return false, err
		}
		return needsRestart, tx.Commit()
	})
}

func (s *OrchestrationWorkflowSpec) observer() DeploymentObserver {
	if s.Observer != nil {
		return s.Observer
	}
	return NoOpDeploymentObserver{}
}

// Run is the deterministic workflow body. Each execution does a single
// pass through the pipeline. On retryable failure the workflow releases
// its lock and returns [ContinueAsNew] to restart with a fresh history.
// On terminal failure the workflow persists [DeploymentStateFailed] and
// completes reconciliation. Run never returns a non-nil error — it
// always terminates in a controlled state or restarts via ContinueAsNew.
func (s *OrchestrationWorkflowSpec) Run(record Record, deploymentID DeploymentID) (struct{}, error) {
	ctx, probe := s.observer().RunStarted(record.Context(), deploymentID)
	defer probe.End()
	_ = ctx

	if _, err := RunActivity(record, s.AcquireLock(), deploymentID); err != nil {
		probe.Error(err)
		if IsTerminal(err) {
			return struct{}{}, nil
		}
		return struct{}{}, s.continueAsNew(record, deploymentID, probe)
	}

	for {
		loaded, err := RunActivity(record, s.LoadDeploymentAndPool(), deploymentID)
		if err != nil {
			probe.Error(err)
			if IsTerminal(err) {
				return struct{}{}, nil
			}
			return struct{}{}, s.releaseLockAndContinue(record, deploymentID, probe)
		}
		dep, pool := loaded.Deployment, loaded.Pool
		startGen := dep.Generation

		switch dep.State {
		case DeploymentStateDeleting:
			if err := s.executeDelete(record, dep, pool, deploymentID); err != nil {
				probe.Error(err)
				if !IsTerminal(err) {
					return struct{}{}, s.releaseLockAndContinue(record, deploymentID, probe)
				}
				s.persistFailedResult(record, deploymentID, dep.Auth, err, probe)
			} else {
				return struct{}{}, nil
			}

		default:
			resolvedIDs, err := s.executePlacementPipeline(record, dep, pool, deploymentID, startGen, probe)
			if errors.Is(err, errAuthPaused) {
				result := NewPausedAuthResult(deploymentID, dep.Auth)
				probe.StateChanged(result.State)
				probe.Error(err)
				if _, persistErr := RunActivity(record, s.PersistReconciliationResult(), result); persistErr != nil {
					probe.Error(persistErr)
					return struct{}{}, s.releaseLockAndContinue(record, deploymentID, probe)
				}
			} else if err != nil {
				probe.Error(err)
				if !IsTerminal(err) {
					return struct{}{}, s.releaseLockAndContinue(record, deploymentID, probe)
				}
				s.persistFailedResult(record, deploymentID, dep.Auth, err, probe)
			} else {
				result := NewActiveResult(deploymentID, resolvedIDs, dep.Auth)
				probe.StateChanged(result.State)
				if _, persistErr := RunActivity(record, s.PersistReconciliationResult(), result); persistErr != nil {
					probe.Error(persistErr)
					return struct{}{}, s.releaseLockAndContinue(record, deploymentID, probe)
				}
			}
		}

		needsRestart, err := RunActivity(record, s.CompleteReconciliation(), CompleteReconciliationInput{
			DeploymentID:  deploymentID,
			ReconciledGen: startGen,
		})
		if err != nil {
			probe.Error(err)
			return struct{}{}, s.releaseLockAndContinue(record, deploymentID, probe)
		}
		if !needsRestart {
			return struct{}{}, nil
		}
	}
}

// persistFailedResult persists a [DeploymentStateFailed] result with
// the cause's message as the status reason. If the persist itself
// fails, the error is reported via probe but the workflow continues
// to [CompleteReconciliation] regardless.
func (s *OrchestrationWorkflowSpec) persistFailedResult(
	record Record,
	deploymentID DeploymentID,
	auth DeliveryAuth,
	cause error,
	probe DeploymentRunProbe,
) {
	result := NewFailedResult(deploymentID, auth, cause.Error())
	probe.StateChanged(result.State)
	if _, err := RunActivity(record, s.PersistReconciliationResult(), result); err != nil {
		probe.Error(err)
	}
}

// releaseLockAndContinue releases the orchestration lock and returns a
// [ContinueAsNew] error to restart with a fresh history.
func (s *OrchestrationWorkflowSpec) releaseLockAndContinue(
	record Record,
	deploymentID DeploymentID,
	probe DeploymentRunProbe,
) error {
	if _, err := RunActivity(record, s.ReleaseLock(), deploymentID); err != nil {
		probe.Error(err)
	}
	return ContinueAsNew(deploymentID)
}

// continueAsNew returns a [ContinueAsNew] error without attempting to
// release the lock (used when the lock was never acquired).
func (s *OrchestrationWorkflowSpec) continueAsNew(
	_ Record,
	deploymentID DeploymentID,
	_ DeploymentRunProbe,
) error {
	return ContinueAsNew(deploymentID)
}

// executePlacementPipeline runs the full resolve → delta → plan → execute
// pipeline and returns the new resolved target IDs.
func (s *OrchestrationWorkflowSpec) executePlacementPipeline(
	record Record,
	dep Deployment,
	pool []TargetInfo,
	deploymentID DeploymentID,
	startGen Generation,
	probe DeploymentRunProbe,
) ([]TargetID, error) {
	resolved, err := RunActivity(record, s.ResolvePlacement(), ResolvePlacementInput{
		Spec: dep.PlacementStrategy,
		Pool: PlacementTargets(pool),
	})
	if err != nil {
		return nil, fmt.Errorf("resolve placement: %w", err)
	}

	if len(resolved) == 0 {
		return nil, nil
	}

	resolvedTargets := ResolvedTargetInfos(resolved, pool)
	delta := ComputeTargetDelta(dep.ResolvedTargets, resolvedTargets, pool)

	plan, err := RunActivity(record, s.PlanRollout(), PlanRolloutInput{
		Spec:  dep.RolloutStrategy,
		Delta: delta,
	})
	if err != nil {
		return nil, fmt.Errorf("plan rollout: %w", err)
	}

	if err := s.executeRolloutPlan(record, dep, plan, deploymentID, startGen, probe); err != nil {
		return nil, err
	}

	ids := make([]TargetID, len(resolved))
	for i, t := range resolved {
		ids[i] = t.ID
	}
	return ids, nil
}

// executeDelete removes the deployment from all currently resolved
// targets, cleans up provisioned targets, and hard-deletes records.
func (s *OrchestrationWorkflowSpec) executeDelete(
	record Record,
	dep Deployment,
	pool []TargetInfo,
	deploymentID DeploymentID,
) error {
	// Phase 1: Resource cleanup.
	var sa *SignerAssertion
	if dep.Provenance != nil {
		looked, err := lookupSignerAssertion(record.Context(), s.Store, dep.Provenance)
		if err != nil {
			return fmt.Errorf("lookup signer enrollment for attestation assembly: %w", err)
		}
		sa = &looked
	}

	targets := targetInfosByID(dep.ResolvedTargets, pool)
	for _, target := range targets {
		in := RemoveInput{
			Target:       target,
			DeliveryID:   deliveryIDFor(deploymentID, target.ID),
			DeploymentID: deploymentID,
			Auth:         dep.Auth,
		}
		if sa != nil {
			in.Attestation = assembleRemoveAttestation(dep, *sa)
		}
		if _, err := RunActivity(record, s.RemoveFromTarget(), in); err != nil {
			return fmt.Errorf("remove delivery for target %s: %w", target.ID, err)
		}
	}

	// Phase 2: Target cleanup (provisioned kind targets).
	if _, err := RunActivity(record, s.CleanupProvisionedTargets(), CleanupProvisionedTargetsInput{
		DeploymentID: deploymentID,
	}); err != nil {
		return fmt.Errorf("cleanup provisioned targets: %w", err)
	}

	// Phase 3: Record deletion.
	if _, err := RunActivity(record, s.DeleteDeploymentRecord(), deploymentID); err != nil {
		return fmt.Errorf("delete deployment record: %w", err)
	}

	return nil
}

// executeRolloutPlan runs each step in a [RolloutPlan]. For deliver
// steps it dispatches all deliveries, then waits for every delivery in
// the step to reach a terminal state before proceeding to the next step.
// Between steps, it checks whether the deployment's generation has
// advanced; if so and the [VersionConflictPolicy] is restart, it aborts
// so the next reconciliation can start fresh.
func (s *OrchestrationWorkflowSpec) executeRolloutPlan(
	record Record,
	dep Deployment,
	plan RolloutPlan,
	deploymentID DeploymentID,
	startGen Generation,
	probe DeploymentRunProbe,
) error {
	policy := dep.RolloutStrategy.EffectiveVersionConflictPolicy()

	var sa *SignerAssertion
	if dep.Provenance != nil {
		looked, err := lookupSignerAssertion(record.Context(), s.Store, dep.Provenance)
		if err != nil {
			return fmt.Errorf("lookup signer enrollment for attestation assembly: %w", err)
		}
		sa = &looked
	}

	for i, step := range plan.Steps {
		if step.Remove != nil {
			for _, target := range step.Remove.Targets {
				// TODO: need to call the manifest generator on remove hook
				in := RemoveInput{
					Target:       target,
					DeliveryID:   deliveryIDFor(deploymentID, target.ID),
					DeploymentID: deploymentID,
					Auth:         dep.Auth,
				}
				if sa != nil {
					in.Attestation = assembleRemoveAttestation(dep, *sa)
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
					Spec:   dep.ManifestStrategy,
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
				did := deliveryIDFor(deploymentID, target.ID)
				in := DeliverInput{
					Target:       target,
					DeliveryID:   did,
					DeploymentID: deploymentID,
					Manifests:    manifests,
					Auth:         dep.Auth,
				}
				if sa != nil {
					in.Attestation = assembleDeliverAttestation(dep, manifests, *sa)
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
					// Synchronous terminal state (e.g. Delivered): collect
					// result directly without waiting for a signal.
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
			currentGen, err := RunActivity(record, s.CheckGeneration(), deploymentID)
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
		event, err := AwaitSignal(record, DeploymentEventSignal)
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
// deployment-target pair. This keeps IDs stable across re-deliveries
// to the same target, which is the current one-delivery-per-pair model.
// TODO: does this need to be deterministic? Do we actually want different IDs on redelivery?
func deliveryIDFor(depID DeploymentID, tgtID TargetID) DeliveryID {
	return DeliveryID(string(depID) + ":" + string(tgtID))
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

func lookupSignerAssertion(ctx context.Context, store Store, prov *Provenance) (SignerAssertion, error) {
	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		return SignerAssertion{}, fmt.Errorf("begin read tx: %w", err)
	}
	defer tx.Rollback()

	found, err := tx.SignerEnrollments().ListBySubject(ctx, prov.Sig.Signer)
	if err != nil {
		return SignerAssertion{}, fmt.Errorf("list signer enrollments: %w", err)
	}
	if len(found) == 0 {
		return SignerAssertion{}, fmt.Errorf("no signer enrollment found for %s / %s", prov.Sig.Signer.Subject, prov.Sig.Signer.Issuer)
	}
	if err := tx.Commit(); err != nil {
		return SignerAssertion{}, fmt.Errorf("commit read tx: %w", err)
	}
	enrollment := found[0]
	return SignerAssertion{
		IdentityToken:   enrollment.IdentityToken,
		RegistryID:      enrollment.RegistryID,
		RegistrySubject: enrollment.RegistrySubject,
	}, nil
}

func assembleDeliverAttestation(dep Deployment, manifests []Manifest, sa SignerAssertion) *Attestation {
	prov := dep.Provenance
	return &Attestation{
		Input: SignedInput{
			Content: DeploymentContent{
				DeploymentID:      dep.ID,
				ManifestStrategy:  dep.ManifestStrategy,
				PlacementStrategy: dep.PlacementStrategy,
			},
			Sig:                prov.Sig,
			Signer:             sa,
			ValidUntil:         prov.ValidUntil,
			OutputConstraints:  prov.OutputConstraints,
			ExpectedGeneration: prov.ExpectedGeneration,
		},
		Output: &PutManifests{
			Manifests: manifests,
		},
	}
}

func assembleRemoveAttestation(dep Deployment, sa SignerAssertion) *Attestation {
	prov := dep.Provenance
	return &Attestation{
		Input: SignedInput{
			Content: DeploymentContent{
				DeploymentID:      dep.ID,
				ManifestStrategy:  dep.ManifestStrategy,
				PlacementStrategy: dep.PlacementStrategy,
			},
			Sig:                prov.Sig,
			Signer:             sa,
			ValidUntil:         prov.ValidUntil,
			OutputConstraints:  prov.OutputConstraints,
			ExpectedGeneration: prov.ExpectedGeneration,
		},
		Output: &RemoveByDeploymentId{
			DeploymentID: dep.ID,
		},
	}
}
