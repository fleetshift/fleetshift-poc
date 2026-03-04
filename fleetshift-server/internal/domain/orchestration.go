package domain

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

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
}

// RemoveInput is the input to the remove-from-target activity.
type RemoveInput struct {
	Target       TargetInfo
	DeliveryID   DeliveryID
	DeploymentID DeploymentID
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

// OrchestrationJournal is the durable execution journal for
// [OrchestrationWorkflow.Run]. It extends [Journal] with the ability
// to await and signal deployment-scoped events. Engines (DBOS,
// go-workflows, sync) implement this interface.
type OrchestrationJournal interface {
	Journal

	// AwaitDeploymentEvent blocks until the engine delivers the next
	// [DeploymentEvent] for this workflow instance.
	AwaitDeploymentEvent() (DeploymentEvent, error)

	// SignalDeploymentEvent delivers a [DeploymentEvent] to the
	// workflow instance identified by deploymentID. The signal is
	// journaled so that it survives engine restarts.
	SignalDeploymentEvent(ctx context.Context, deploymentID DeploymentID, event DeploymentEvent) error
}

// OrchestrationRunner starts orchestration workflows (app-facing API).
// Deployment events are delivered to running instances via the
// [OrchestrationJournal] from within the workflow itself.
type OrchestrationRunner interface {
	Run(ctx context.Context, deploymentID DeploymentID) (WorkflowHandle[struct{}], error)
}

// OrchestrationWorkflow is the deployment pipeline expressed as a
// deterministic workflow. All I/O and strategy invocations run inside
// activities so that placement, manifest, and rollout strategies may
// perform I/O or stateful behavior. Only pure computation (e.g.
// [ComputeTargetDelta]) runs inline in the workflow body.
// Infrastructure packages accept this struct to construct an
// [OrchestrationRunner] backed by a specific durable execution engine.
type OrchestrationWorkflow struct {
	Store            Store
	Delivery         DeliveryService
	Strategies       StrategyFactory
	Observer         DeploymentObserver
	DeliveryObserver DeliveryObserver
	Now              func() time.Time

	// journal is set by Run before any activities execute. Activity
	// closures read it at invocation time to access journal capabilities
	// (e.g. signaling deployment events).
	journal OrchestrationJournal
}

func (w *OrchestrationWorkflow) now() time.Time {
	if w.Now != nil {
		return w.Now()
	}
	return time.Now()
}

func (w *OrchestrationWorkflow) Name() string { return "orchestrate-deployment" }

// Each method returns a typed [Activity] derived from the workflow's own
// dependencies. Infrastructure adapters call these to register activities;
// the workflow body calls them via [RunActivity].

// LoadDeploymentAndPool reads the deployment and target pool in a single
// activity to avoid separate durable steps. Used at workflow start and when
// reloading after a spec change.
func (w *OrchestrationWorkflow) LoadDeploymentAndPool() Activity[DeploymentID, DeploymentAndPool] {
	return NewActivity("load-deployment-and-pool", func(ctx context.Context, id DeploymentID) (DeploymentAndPool, error) {
		tx, err := w.Store.Begin(ctx)
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
func (w *OrchestrationWorkflow) ResolvePlacement() Activity[ResolvePlacementInput, []PlacementTarget] {
	return NewActivity("resolve-placement", func(ctx context.Context, in ResolvePlacementInput) ([]PlacementTarget, error) {
		placement, err := w.Strategies.PlacementStrategy(in.Spec)
		if err != nil {
			return nil, err
		}
		return placement.Resolve(ctx, in.Pool)
	})
}

// PlanRollout runs the deployment's rollout strategy to produce an
// ordered execution plan from the target delta. Invoked as an activity
// so rollout may perform I/O or use state that changes over time.
func (w *OrchestrationWorkflow) PlanRollout() Activity[PlanRolloutInput, RolloutPlan] {
	return NewActivity("plan-rollout", func(ctx context.Context, in PlanRolloutInput) (RolloutPlan, error) {
		rollout := w.Strategies.RolloutStrategy(in.Spec)
		return rollout.Plan(ctx, in.Delta)
	})
}

// GenerateManifests creates manifests for a single target using the
// configured manifest strategy.
func (w *OrchestrationWorkflow) GenerateManifests() Activity[GenerateManifestsInput, []Manifest] {
	return NewActivity("generate-manifests", func(ctx context.Context, in GenerateManifestsInput) ([]Manifest, error) {
		strategy, err := w.Strategies.ManifestStrategy(in.Spec)
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
func (w *OrchestrationWorkflow) DeliverToTarget() Activity[DeliverInput, DeliveryResult] {
	return NewActivity("deliver-to-target", func(ctx context.Context, in DeliverInput) (DeliveryResult, error) {
		tx, err := w.Store.Begin(ctx)
		if err != nil {
			return DeliveryResult{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		now := w.now()
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
			w.Store, w.journal.SignalDeploymentEvent,
			w.DeliveryObserver,
		)

		return w.Delivery.Deliver(context.Background(), in.Target, in.DeliveryID, in.Manifests, signaler)
	})
}

// RemoveFromTarget removes a deployment's manifests from a target.
func (w *OrchestrationWorkflow) RemoveFromTarget() Activity[RemoveInput, struct{}] {
	return NewActivity("remove-from-target", func(ctx context.Context, in RemoveInput) (struct{}, error) {
		return struct{}{}, w.Delivery.Remove(ctx, in.Target, in.DeliveryID, &DeliverySignaler{})
	})
}

// UpdateDeployment persists a deployment's updated state, bumping
// UpdatedAt and regenerating the Etag.
func (w *OrchestrationWorkflow) UpdateDeployment() Activity[Deployment, struct{}] {
	return NewActivity("update-deployment", func(ctx context.Context, d Deployment) (struct{}, error) {
		tx, err := w.Store.Begin(ctx)
		if err != nil {
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		d.UpdatedAt = w.now()
		d.Etag = uuid.New().String()
		if err := tx.Deployments().Update(ctx, d); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, tx.Commit()
	})
}

func (w *OrchestrationWorkflow) observer() DeploymentObserver {
	if w.Observer != nil {
		return w.Observer
	}
	return NoOpDeploymentObserver{}
}

// Run is the deterministic workflow body. It performs initial placement
// (load deployment and pool, resolve placement, rollout) without awaiting
// an event, then enters a loop that blocks on
// [DeploymentWorkflowRunner.AwaitDeploymentEvent] and re-evaluates on each
// event (pool change, manifest invalidation, spec change, delete). The
// workflow exits when it receives a Delete event or encounters a fatal error.
//
// Durable execution: On replay, the engine re-runs Run from the top.
// Completed activities return their recorded results; each AwaitDeploymentEvent()
// is a distinct recorded operation so replay returns the same event for that
// iteration. So we never receive the same logical event twice across iterations.
func (w *OrchestrationWorkflow) Run(journal OrchestrationJournal, deploymentID DeploymentID) (struct{}, error) {
	w.journal = journal

	ctx, probe := w.observer().RunStarted(journal.Context(), deploymentID)
	defer probe.End()
	_ = ctx

	var dep Deployment
	var pool []TargetInfo

	// Initial placement: no event required. Load, run placement pipeline, update.
	loaded, err := RunActivity(journal, w.LoadDeploymentAndPool(), deploymentID)
	if err != nil {
		probe.Error(err)
		return struct{}{}, fmt.Errorf("load deployment and pool: %w", err)
	}
	dep, pool = loaded.Deployment, loaded.Pool

	resolvedIDs, err := w.executePlacementPipeline(journal, dep, pool, pool, deploymentID)
	if err != nil {
		dep.State = DeploymentStateFailed
		probe.StateChanged(dep.State)
		if _, updateErr := RunActivity(journal, w.UpdateDeployment(), dep); updateErr != nil {
			probe.Error(updateErr)
		}
		probe.Error(err)
		return struct{}{}, err
	}
	dep.ResolvedTargets = resolvedIDs

	// Active: executeRolloutPlan awaits all delivery completions per step,
	// so reaching this point means every delivery finished successfully.
	dep.State = DeploymentStateActive
	probe.StateChanged(dep.State)
	if _, err := RunActivity(journal, w.UpdateDeployment(), dep); err != nil {
		probe.Error(err)
		return struct{}{}, fmt.Errorf("update deployment: %w", err)
	}

	for {
		event, err := journal.AwaitDeploymentEvent()
		if err != nil {
			probe.Error(err)
			return struct{}{}, fmt.Errorf("await deployment event: %w", err)
		}
		probe.EventReceived(event)

		if event.Delete {
			if err := w.executeDelete(journal, dep, pool, deploymentID); err != nil {
				probe.Error(err)
				return struct{}{}, err
			}
			return struct{}{}, nil
		}

		if event.SpecChanged {
			loaded, err := RunActivity(journal, w.LoadDeploymentAndPool(), deploymentID)
			if err != nil {
				probe.Error(err)
				return struct{}{}, fmt.Errorf("reload deployment and pool: %w", err)
			}
			dep, pool = loaded.Deployment, loaded.Pool
		}

		previousPool := pool
		if event.PoolChange != nil {
			pool = ApplyPoolChange(pool, *event.PoolChange)
		}

		if event.ManifestInvalidated {
			if err := w.executeManifestInvalidation(journal, dep, pool, deploymentID); err != nil {
				probe.Error(err)
				return struct{}{}, err
			}
		} else {
			resolvedIDs, err := w.executePlacementPipeline(journal, dep, pool, previousPool, deploymentID)
			if err != nil {
				dep.State = DeploymentStateFailed
				probe.StateChanged(dep.State)
				if _, updateErr := RunActivity(journal, w.UpdateDeployment(), dep); updateErr != nil {
					probe.Error(updateErr)
				}
				probe.Error(err)
				return struct{}{}, err
			}
			dep.ResolvedTargets = resolvedIDs
		}

		dep.State = DeploymentStateActive
		probe.StateChanged(dep.State)
		if _, err := RunActivity(journal, w.UpdateDeployment(), dep); err != nil {
			probe.Error(err)
			return struct{}{}, fmt.Errorf("update deployment: %w", err)
		}
	}
}

// executePlacementPipeline runs the full resolve → delta → plan → execute
// pipeline and returns the new resolved target IDs. previousPool is the
// pool before any pool change was applied; it provides full [TargetInfo]
// for targets that were previously resolved but have since left the pool.
func (w *OrchestrationWorkflow) executePlacementPipeline(
	journal OrchestrationJournal,
	dep Deployment,
	pool []TargetInfo,
	previousPool []TargetInfo,
	deploymentID DeploymentID,
) ([]TargetID, error) {
	resolved, err := RunActivity(journal, w.ResolvePlacement(), ResolvePlacementInput{
		Spec: dep.PlacementStrategy,
		Pool: PlacementTargets(pool),
	})
	if err != nil {
		return nil, fmt.Errorf("resolve placement: %w", err)
	}

	if len(resolved) == 0 {
		return nil, fmt.Errorf("placement resolved to zero targets")
	}

	resolvedTargets := ResolvedTargetInfos(resolved, pool)
	deltaPool := mergePools(previousPool, pool)
	delta := ComputeTargetDelta(dep.ResolvedTargets, resolvedTargets, deltaPool)

	plan, err := RunActivity(journal, w.PlanRollout(), PlanRolloutInput{
		Spec:  dep.RolloutStrategy,
		Delta: delta,
	})
	if err != nil {
		return nil, fmt.Errorf("plan rollout: %w", err)
	}

	if err := w.executeRolloutPlan(journal, dep, plan, deploymentID); err != nil {
		return nil, err
	}

	ids := make([]TargetID, len(resolved))
	for i, t := range resolved {
		ids[i] = t.ID
	}
	return ids, nil
}

// executeManifestInvalidation re-generates and delivers manifests for
// the currently resolved targets without re-resolving placement.
func (w *OrchestrationWorkflow) executeManifestInvalidation(
	journal OrchestrationJournal,
	dep Deployment,
	pool []TargetInfo,
	deploymentID DeploymentID,
) error {
	targets := TargetInfosByID(dep.ResolvedTargets, pool)
	delta := TargetDelta{Unchanged: targets}

	plan, err := RunActivity(journal, w.PlanRollout(), PlanRolloutInput{
		Spec:  dep.RolloutStrategy,
		Delta: delta,
	})
	if err != nil {
		return fmt.Errorf("plan rollout (manifest invalidation): %w", err)
	}
	return w.executeRolloutPlan(journal, dep, plan, deploymentID)
}

// executeDelete removes the deployment from all currently resolved
// targets and updates the deployment state.
func (w *OrchestrationWorkflow) executeDelete(
	journal OrchestrationJournal,
	dep Deployment,
	pool []TargetInfo,
	deploymentID DeploymentID,
) error {
	targets := TargetInfosByID(dep.ResolvedTargets, pool)
	for _, target := range targets {
		if _, err := RunActivity(journal, w.RemoveFromTarget(), RemoveInput{
			Target:       target,
			DeliveryID:   deliveryIDFor(deploymentID, target.ID),
			DeploymentID: deploymentID,
		}); err != nil {
			return fmt.Errorf("remove delivery for target %s: %w", target.ID, err)
		}
	}

	dep.ResolvedTargets = nil
	dep.State = DeploymentStateDeleting
	if _, err := RunActivity(journal, w.UpdateDeployment(), dep); err != nil {
		return fmt.Errorf("update deployment: %w", err)
	}
	return nil
}

// executeRolloutPlan runs each step in a [RolloutPlan]. For deliver
// steps it dispatches all deliveries, then waits for every delivery in
// the step to reach a terminal state before proceeding to the next step.
func (w *OrchestrationWorkflow) executeRolloutPlan(
	journal OrchestrationJournal,
	dep Deployment,
	plan RolloutPlan,
	deploymentID DeploymentID,
) error {
	for _, step := range plan.Steps {
		if step.Remove != nil {
			for _, target := range step.Remove.Targets {
				// TODO: need to call the manifest generator on remove hook
				if _, err := RunActivity(journal, w.RemoveFromTarget(), RemoveInput{
					Target:       target,
					DeliveryID:   deliveryIDFor(deploymentID, target.ID),
					DeploymentID: deploymentID,
				}); err != nil {
					return fmt.Errorf("remove delivery for target %s: %w", target.ID, err)
				}
			}
			continue
		}
		if step.Deliver != nil {
			var pending []DeliveryID
			for _, target := range step.Deliver.Targets {
				manifests, err := RunActivity(journal, w.GenerateManifests(), GenerateManifestsInput{
					Spec:   dep.ManifestStrategy,
					Target: target,
				})
				if err != nil {
					return fmt.Errorf("generate manifests for target %s: %w", target.ID, err)
				}
				did := deliveryIDFor(deploymentID, target.ID)
				if _, err := RunActivity(journal, w.DeliverToTarget(), DeliverInput{
					Target:       target,
					DeliveryID:   did,
					DeploymentID: deploymentID,
					Manifests:    manifests,
				}); err != nil {
					return fmt.Errorf("deliver to target %s: %w", target.ID, err)
				}
				pending = append(pending, did)
			}
			if _, err := w.awaitDeliveries(journal, pending); err != nil {
				return err
			}
			continue
		}
	}
	return nil
}

// awaitDeliveries blocks until every delivery in pending has completed.
// Non-delivery events that arrive while waiting are collected and
// returned for the caller to process later. A Delete event aborts
// immediately with an error.
func (w *OrchestrationWorkflow) awaitDeliveries(
	journal OrchestrationJournal,
	pending []DeliveryID,
) ([]DeploymentEvent, error) {
	remaining := make(map[DeliveryID]struct{}, len(pending))
	for _, id := range pending {
		remaining[id] = struct{}{}
	}

	var deferred []DeploymentEvent
	for len(remaining) > 0 {
		event, err := journal.AwaitDeploymentEvent()
		if err != nil {
			return nil, fmt.Errorf("await delivery completion: %w", err)
		}
		if event.Delete {
			return nil, fmt.Errorf("deployment deleted while awaiting deliveries")
		}
		if event.DeliveryCompleted != nil {
			delete(remaining, event.DeliveryCompleted.DeliveryID)
			if event.DeliveryCompleted.Result.State == DeliveryStateFailed {
				return nil, fmt.Errorf("delivery %s failed: %s",
					event.DeliveryCompleted.DeliveryID,
					event.DeliveryCompleted.Result.Message)
			}
			continue
		}
		deferred = append(deferred, event)
	}
	return deferred, nil
}

// deliveryIDFor produces a deterministic [DeliveryID] for a
// deployment-target pair. This keeps IDs stable across re-deliveries
// to the same target, which is the current one-delivery-per-pair model.
// TODO: does this need to be deterministic? Do we actually want different IDs on redelivery?
func deliveryIDFor(depID DeploymentID, tgtID TargetID) DeliveryID {
	return DeliveryID(string(depID) + ":" + string(tgtID))
}

// mergePools returns the union of two pools. Entries in current take
// precedence over entries in previous for the same TargetID.
func mergePools(previous, current []TargetInfo) []TargetInfo {
	index := make(map[TargetID]TargetInfo, len(previous)+len(current))
	for _, t := range previous {
		index[t.ID] = t
	}
	for _, t := range current {
		index[t.ID] = t
	}
	out := make([]TargetInfo, 0, len(index))
	for _, t := range index {
		out = append(out, t)
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
