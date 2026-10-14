package domain

import "context"

// ManifestStrategy determines what manifests to deploy to a given target.
type ManifestStrategy interface {
	Generate(ctx context.Context, gctx GenerateContext) ([]Manifest, error)
	OnRemoved(ctx context.Context, target TargetID) error
}

// PlacementStrategy determines which targets to select from the available pool.
//
// Resolve receives only the placement view of each target ([PlacementTarget]:
// ID, Name, Labels). Properties and other target metadata are not visible, so
// placement cannot depend on them and the platform need not invalidate when
// they change. The strategy may filter, rank, and limit the pool. The platform
// treats the returned slice order as meaningful.
type PlacementStrategy interface {
	Resolve(ctx context.Context, pool []PlacementTarget) ([]PlacementTarget, error)
}

// RolloutStrategy determines the pacing and ordering of delivery across targets.
//
// Plan receives the delta (what changed in the target set) and returns an
// ordered execution plan. The platform executes batches sequentially.
type RolloutStrategy interface {
	Plan(ctx context.Context, delta TargetDelta) (RolloutPlan, error)
}
