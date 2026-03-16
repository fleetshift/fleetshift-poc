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
	Auth         DeliveryAuth
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

// OrchestrationWorkflowSpec is the deployment pipeline expressed as a
// deterministic workflow. All I/O and strategy invocations run inside
// activities so that placement, manifest, and rollout strategies may
// perform I/O or stateful behavior. Only pure computation (e.g.
// [ComputeTargetDelta]) runs inline in the workflow body.
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
		tx, err := s.Store.Begin(ctx)
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

		return s.Delivery.Deliver(context.Background(), in.Target, in.DeliveryID, in.Manifests, in.Auth, signaler)
	})
}

// RemoveFromTarget removes a deployment's manifests from a target.
func (s *OrchestrationWorkflowSpec) RemoveFromTarget() Activity[RemoveInput, struct{}] {
	return NewActivity("remove-from-target", func(ctx context.Context, in RemoveInput) (struct{}, error) {
		return struct{}{}, s.Delivery.Remove(ctx, in.Target, in.DeliveryID, &DeliverySignaler{})
	})
}

// UpdateDeployment persists a deployment's updated state, bumping
// UpdatedAt and regenerating the Etag.
func (s *OrchestrationWorkflowSpec) UpdateDeployment() Activity[Deployment, struct{}] {
	return NewActivity("update-deployment", func(ctx context.Context, d Deployment) (struct{}, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		d.UpdatedAt = s.now()
		d.Etag = uuid.New().String()
		if err := tx.Deployments().Update(ctx, d); err != nil {
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

func (s *OrchestrationWorkflowSpec) observer() DeploymentObserver {
	if s.Observer != nil {
		return s.Observer
	}
	return NoOpDeploymentObserver{}
}

// Run is the deterministic workflow body. It performs initial placement
// (load deployment and pool, resolve placement, rollout) without awaiting
// an event, then enters a loop that blocks on [AwaitSignal] and
// re-evaluates on each event (pool change, manifest invalidation, spec
// change, delete). The workflow exits when it receives a Delete event or
// encounters a fatal error.
func (s *OrchestrationWorkflowSpec) Run(record Record, deploymentID DeploymentID) (struct{}, error) {
	ctx, probe := s.observer().RunStarted(record.Context(), deploymentID)
	defer probe.End()
	_ = ctx

	var dep Deployment
	var pool []TargetInfo

	loaded, err := RunActivity(record, s.LoadDeploymentAndPool(), deploymentID)
	if err != nil {
		probe.Error(err)
		return struct{}{}, fmt.Errorf("load deployment and pool: %w", err)
	}
	dep, pool = loaded.Deployment, loaded.Pool

	resolvedIDs, err := s.executePlacementPipeline(record, dep, pool, pool, deploymentID, probe)
	if err != nil {
		dep.State = DeploymentStateFailed
		probe.StateChanged(dep.State)
		if _, updateErr := RunActivity(record, s.UpdateDeployment(), dep); updateErr != nil {
			probe.Error(updateErr)
		}
		probe.Error(err)
		return struct{}{}, err
	}
	dep.ResolvedTargets = resolvedIDs

	dep.State = DeploymentStateActive
	probe.StateChanged(dep.State)
	if _, err := RunActivity(record, s.UpdateDeployment(), dep); err != nil {
		probe.Error(err)
		return struct{}{}, fmt.Errorf("update deployment: %w", err)
	}

	for {
		event, err := AwaitSignal(record, DeploymentEventSignal)
		if err != nil {
			probe.Error(err)
			return struct{}{}, fmt.Errorf("await deployment event: %w", err)
		}
		probe.EventReceived(event)

		if event.Delete {
			if err := s.executeDelete(record, dep, pool, deploymentID); err != nil {
				probe.Error(err)
				return struct{}{}, err
			}
			return struct{}{}, nil
		}

		if event.SpecChanged {
			loaded, err := RunActivity(record, s.LoadDeploymentAndPool(), deploymentID)
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
			if err := s.executeManifestInvalidation(record, dep, pool, deploymentID, probe); err != nil {
				probe.Error(err)
				return struct{}{}, err
			}
		} else {
			resolvedIDs, err := s.executePlacementPipeline(record, dep, pool, previousPool, deploymentID, probe)
			if err != nil {
				dep.State = DeploymentStateFailed
				probe.StateChanged(dep.State)
				if _, updateErr := RunActivity(record, s.UpdateDeployment(), dep); updateErr != nil {
					probe.Error(updateErr)
				}
				probe.Error(err)
				return struct{}{}, err
			}
			dep.ResolvedTargets = resolvedIDs
		}

		dep.State = DeploymentStateActive
		probe.StateChanged(dep.State)
		if _, err := RunActivity(record, s.UpdateDeployment(), dep); err != nil {
			probe.Error(err)
			return struct{}{}, fmt.Errorf("update deployment: %w", err)
		}
	}
}

// executePlacementPipeline runs the full resolve → delta → plan → execute
// pipeline and returns the new resolved target IDs. previousPool is the
// pool before any pool change was applied; it provides full [TargetInfo]
// for targets that were previously resolved but have since left the pool.
func (s *OrchestrationWorkflowSpec) executePlacementPipeline(
	record Record,
	dep Deployment,
	pool []TargetInfo,
	previousPool []TargetInfo,
	deploymentID DeploymentID,
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
		return nil, fmt.Errorf("placement resolved to zero targets")
	}

	resolvedTargets := ResolvedTargetInfos(resolved, pool)
	deltaPool := mergePools(previousPool, pool)
	delta := ComputeTargetDelta(dep.ResolvedTargets, resolvedTargets, deltaPool)

	plan, err := RunActivity(record, s.PlanRollout(), PlanRolloutInput{
		Spec:  dep.RolloutStrategy,
		Delta: delta,
	})
	if err != nil {
		return nil, fmt.Errorf("plan rollout: %w", err)
	}

	if err := s.executeRolloutPlan(record, dep, plan, deploymentID, probe); err != nil {
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
func (s *OrchestrationWorkflowSpec) executeManifestInvalidation(
	record Record,
	dep Deployment,
	pool []TargetInfo,
	deploymentID DeploymentID,
	probe DeploymentRunProbe,
) error {
	targets := TargetInfosByID(dep.ResolvedTargets, pool)
	delta := TargetDelta{Unchanged: targets}

	plan, err := RunActivity(record, s.PlanRollout(), PlanRolloutInput{
		Spec:  dep.RolloutStrategy,
		Delta: delta,
	})
	if err != nil {
		return fmt.Errorf("plan rollout (manifest invalidation): %w", err)
	}
	return s.executeRolloutPlan(record, dep, plan, deploymentID, probe)
}

// executeDelete removes the deployment from all currently resolved
// targets and updates the deployment state.
func (s *OrchestrationWorkflowSpec) executeDelete(
	record Record,
	dep Deployment,
	pool []TargetInfo,
	deploymentID DeploymentID,
) error {
	targets := TargetInfosByID(dep.ResolvedTargets, pool)
	for _, target := range targets {
		if _, err := RunActivity(record, s.RemoveFromTarget(), RemoveInput{
			Target:       target,
			DeliveryID:   deliveryIDFor(deploymentID, target.ID),
			DeploymentID: deploymentID,
		}); err != nil {
			return fmt.Errorf("remove delivery for target %s: %w", target.ID, err)
		}
	}

	dep.ResolvedTargets = nil
	dep.State = DeploymentStateDeleting
	if _, err := RunActivity(record, s.UpdateDeployment(), dep); err != nil {
		return fmt.Errorf("update deployment: %w", err)
	}
	return nil
}

// executeRolloutPlan runs each step in a [RolloutPlan]. For deliver
// steps it dispatches all deliveries, then waits for every delivery in
// the step to reach a terminal state before proceeding to the next step.
func (s *OrchestrationWorkflowSpec) executeRolloutPlan(
	record Record,
	dep Deployment,
	plan RolloutPlan,
	deploymentID DeploymentID,
	probe DeploymentRunProbe,
) error {
	for _, step := range plan.Steps {
		if step.Remove != nil {
			for _, target := range step.Remove.Targets {
				// TODO: need to call the manifest generator on remove hook
				if _, err := RunActivity(record, s.RemoveFromTarget(), RemoveInput{
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
				if _, err := RunActivity(record, s.DeliverToTarget(), DeliverInput{
					Target:       target,
					DeliveryID:   did,
					DeploymentID: deploymentID,
					Manifests:    manifests,
				}); err != nil {
					return fmt.Errorf("deliver to target %s: %w", target.ID, err)
				}
				pending = append(pending, did)
			}
			_, results, err := s.awaitDeliveries(record, pending)
			if err != nil {
				return err
			}
			for _, result := range results {
				if len(result.ProvisionedTargets) > 0 || len(result.ProducedSecrets) > 0 {
					if _, err := RunActivity(record, s.ProcessDeliveryOutputs(), result); err != nil {
						return fmt.Errorf("process delivery outputs: %w", err)
					}
					probe.DeliveryOutputsProcessed(result.ProvisionedTargets, len(result.ProducedSecrets))
				}
			}
			continue
		}
	}
	return nil
}

// awaitDeliveries blocks until every delivery in pending has completed.
// Non-delivery events that arrive while waiting are collected and
// returned for the caller to process later. Completed delivery results
// are also returned so the caller can process outputs. A Delete event
// aborts immediately with an error.
func (s *OrchestrationWorkflowSpec) awaitDeliveries(
	record Record,
	pending []DeliveryID,
) (deferred []DeploymentEvent, results []DeliveryResult, err error) {
	remaining := make(map[DeliveryID]struct{}, len(pending))
	for _, id := range pending {
		remaining[id] = struct{}{}
	}

	for len(remaining) > 0 {
		event, err := AwaitSignal(record, DeploymentEventSignal)
		if err != nil {
			return nil, nil, fmt.Errorf("await delivery completion: %w", err)
		}
		if event.Delete {
			return nil, nil, fmt.Errorf("deployment deleted while awaiting deliveries")
		}
		if event.DeliveryCompleted != nil {
			delete(remaining, event.DeliveryCompleted.DeliveryID)
			if event.DeliveryCompleted.Result.State == DeliveryStateFailed {
				return nil, nil, fmt.Errorf("delivery %s failed: %s",
					event.DeliveryCompleted.DeliveryID,
					event.DeliveryCompleted.Result.Message)
			}
			results = append(results, event.DeliveryCompleted.Result)
			continue
		}
		deferred = append(deferred, event)
	}
	return deferred, results, nil
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
