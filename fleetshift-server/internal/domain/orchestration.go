package domain

import (
	"context"
	"fmt"
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
	DeploymentID DeploymentID
	Manifests    []Manifest
}

// RemoveInput is the input to the remove-from-target activity.
type RemoveInput struct {
	Target       TargetInfo
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

// OrchestrationWorkflow is the deployment pipeline expressed as a
// deterministic workflow. All I/O and strategy invocations run inside
// activities so that placement, manifest, and rollout strategies may
// perform I/O or stateful behavior. Only pure computation (e.g.
// [ComputeTargetDelta]) runs inline in the workflow body.
// Infrastructure packages accept this struct to construct an
// [OrchestrationRunner] backed by a specific durable execution engine.
type OrchestrationWorkflow struct {
	Deployments DeploymentRepository
	Targets     TargetRepository
	Delivery    DeliveryService
	Strategies  StrategyFactory
}

func (w *OrchestrationWorkflow) Name() string { return "orchestrate-deployment" }

// Each method returns a typed [Activity] derived from the workflow's own
// dependencies. Infrastructure adapters call these to register activities;
// the workflow body calls them via [RunActivity].

// LoadDeployment reads a deployment from the repository.
func (w *OrchestrationWorkflow) LoadDeployment() Activity[DeploymentID, Deployment] {
	return NewActivity("load-deployment", w.Deployments.Get)
}

// LoadTargetPool reads the full set of registered targets.
func (w *OrchestrationWorkflow) LoadTargetPool() Activity[struct{}, []TargetInfo] {
	return NewActivity("load-target-pool", func(ctx context.Context, _ struct{}) ([]TargetInfo, error) {
		return w.Targets.List(ctx)
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

// DeliverToTarget delivers manifests to a target.
func (w *OrchestrationWorkflow) DeliverToTarget() Activity[DeliverInput, DeliveryResult] {
	return NewActivity("deliver-to-target", func(ctx context.Context, in DeliverInput) (DeliveryResult, error) {
		return w.Delivery.Deliver(ctx, in.Target, in.DeploymentID, in.Manifests)
	})
}

// RemoveFromTarget removes a deployment's manifests from a target.
func (w *OrchestrationWorkflow) RemoveFromTarget() Activity[RemoveInput, struct{}] {
	return NewActivity("remove-from-target", func(ctx context.Context, in RemoveInput) (struct{}, error) {
		return struct{}{}, w.Delivery.Remove(ctx, in.Target, in.DeploymentID)
	})
}

// UpdateDeployment persists a deployment's updated state.
func (w *OrchestrationWorkflow) UpdateDeployment() Activity[Deployment, struct{}] {
	return NewActivity("update-deployment", func(ctx context.Context, d Deployment) (struct{}, error) {
		return struct{}{}, w.Deployments.Update(ctx, d)
	})
}

// Run is the deterministic workflow body.
func (w *OrchestrationWorkflow) Run(runner DurableRunner, deploymentID DeploymentID) (struct{}, error) {
	dep, err := RunActivity(runner, w.LoadDeployment(), deploymentID)
	if err != nil {
		return struct{}{}, fmt.Errorf("load deployment: %w", err)
	}

	pool, err := RunActivity(runner, w.LoadTargetPool(), struct{}{})
	if err != nil {
		return struct{}{}, fmt.Errorf("load target pool: %w", err)
	}

	resolved, err := RunActivity(runner, w.ResolvePlacement(), ResolvePlacementInput{
		Spec: dep.PlacementStrategy,
		Pool: PlacementTargets(pool),
	})
	if err != nil {
		return struct{}{}, fmt.Errorf("resolve placement: %w", err)
	}

	resolvedTargets := ResolvedTargetInfos(resolved, pool)
	delta := ComputeTargetDelta(dep.ResolvedTargets, resolvedTargets, pool)

	plan, err := RunActivity(runner, w.PlanRollout(), PlanRolloutInput{
		Spec:  dep.RolloutStrategy,
		Delta: delta,
	})
	if err != nil {
		return struct{}{}, fmt.Errorf("plan rollout: %w", err)
	}

	for _, step := range plan.Steps {
		if step.Remove != nil {
			for _, target := range step.Remove.Targets {
				if _, err := RunActivity(runner, w.RemoveFromTarget(), RemoveInput{
					Target:       target,
					DeploymentID: deploymentID,
				}); err != nil {
					return struct{}{}, fmt.Errorf("remove delivery for target %s: %w", target.ID, err)
				}
			}
			continue
		}
		if step.Deliver != nil {
			for _, target := range step.Deliver.Targets {
				manifests, err := RunActivity(runner, w.GenerateManifests(), GenerateManifestsInput{
					Spec:   dep.ManifestStrategy,
					Target: target,
				})
				if err != nil {
					return struct{}{}, fmt.Errorf("generate manifests for target %s: %w", target.ID, err)
				}
				if _, err := RunActivity(runner, w.DeliverToTarget(), DeliverInput{
					Target:       target,
					DeploymentID: deploymentID,
					Manifests:    manifests,
				}); err != nil {
					return struct{}{}, fmt.Errorf("deliver to target %s: %w", target.ID, err)
				}
			}
			continue
		}
	}

	resolvedIDs := make([]TargetID, len(resolved))
	for i, t := range resolved {
		resolvedIDs[i] = t.ID
	}
	dep.ResolvedTargets = resolvedIDs
	dep.State = DeploymentStateActive

	if _, err := RunActivity(runner, w.UpdateDeployment(), dep); err != nil {
		return struct{}{}, fmt.Errorf("update deployment: %w", err)
	}

	return struct{}{}, nil
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
