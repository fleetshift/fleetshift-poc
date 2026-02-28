package application

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// OrchestrationService executes the deployment pipeline: resolve placement,
// compute delta, plan rollout, generate manifests, deliver.
type OrchestrationService struct {
	Targets     domain.TargetRepository
	Deployments domain.DeploymentRepository
	Delivery    domain.DeliveryService
	Strategies  domain.StrategyFactory
}

// Orchestrate runs the full deployment pipeline for the given deployment.
func (o *OrchestrationService) Orchestrate(ctx context.Context, deploymentID domain.DeploymentID) error {
	dep, err := o.Deployments.Get(ctx, deploymentID)
	if err != nil {
		return fmt.Errorf("load deployment: %w", err)
	}

	pool, err := o.Targets.List(ctx)
	if err != nil {
		return fmt.Errorf("load target pool: %w", err)
	}

	placement, err := o.Strategies.PlacementStrategy(dep.PlacementStrategy)
	if err != nil {
		return fmt.Errorf("create placement strategy: %w", err)
	}

	resolved, err := placement.Resolve(ctx, pool)
	if err != nil {
		return fmt.Errorf("resolve placement: %w", err)
	}

	delta := computeDelta(dep.ResolvedTargets, resolved, pool)

	for _, removed := range delta.Removed {
		if err := o.Delivery.Remove(ctx, removed, deploymentID); err != nil {
			return fmt.Errorf("remove delivery for target %s: %w", removed.ID, err)
		}
	}

	rollout := o.Strategies.RolloutStrategy(dep.RolloutStrategy)
	plan, err := rollout.Plan(ctx, delta)
	if err != nil {
		return fmt.Errorf("plan rollout: %w", err)
	}

	manifest, err := o.Strategies.ManifestStrategy(dep.ManifestStrategy)
	if err != nil {
		return fmt.Errorf("create manifest strategy: %w", err)
	}

	for _, batch := range plan.Batches {
		for _, target := range batch.Targets {
			gctx := domain.GenerateContext{Target: target}
			manifests, err := manifest.Generate(ctx, gctx)
			if err != nil {
				return fmt.Errorf("generate manifests for target %s: %w", target.ID, err)
			}
			if _, err := o.Delivery.Deliver(ctx, target, deploymentID, manifests); err != nil {
				return fmt.Errorf("deliver to target %s: %w", target.ID, err)
			}
		}
	}

	resolvedIDs := make([]domain.TargetID, len(resolved))
	for i, t := range resolved {
		resolvedIDs[i] = t.ID
	}
	dep.ResolvedTargets = resolvedIDs
	dep.State = domain.DeploymentStateActive

	if err := o.Deployments.Update(ctx, dep); err != nil {
		return fmt.Errorf("update deployment: %w", err)
	}

	return nil
}

func computeDelta(previousIDs []domain.TargetID, resolved []domain.TargetInfo, pool []domain.TargetInfo) domain.TargetDelta {
	prevSet := make(map[domain.TargetID]struct{}, len(previousIDs))
	for _, id := range previousIDs {
		prevSet[id] = struct{}{}
	}

	resolvedSet := make(map[domain.TargetID]struct{}, len(resolved))
	for _, t := range resolved {
		resolvedSet[t.ID] = struct{}{}
	}

	var delta domain.TargetDelta
	for _, t := range resolved {
		if _, wasPrevious := prevSet[t.ID]; wasPrevious {
			delta.Unchanged = append(delta.Unchanged, t)
		} else {
			delta.Added = append(delta.Added, t)
		}
	}

	poolIndex := make(map[domain.TargetID]domain.TargetInfo, len(pool))
	for _, t := range pool {
		poolIndex[t.ID] = t
	}
	for _, id := range previousIDs {
		if _, stillResolved := resolvedSet[id]; !stillResolved {
			if t, ok := poolIndex[id]; ok {
				delta.Removed = append(delta.Removed, t)
			} else {
				delta.Removed = append(delta.Removed, domain.TargetInfo{ID: id})
			}
		}
	}

	return delta
}
