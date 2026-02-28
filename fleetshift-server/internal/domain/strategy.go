package domain

import "context"

// ManifestStrategy determines what manifests to deploy to a given target.
type ManifestStrategy interface {
	Generate(ctx context.Context, gctx GenerateContext) ([]Manifest, error)
	OnRemoved(ctx context.Context, target TargetID) error
}

// PlacementStrategy determines which targets to select from the available pool.
//
// Resolve is given the full target pool and returns the resolved target set.
// The strategy may filter, rank, and limit the pool. The platform treats the
// returned slice order as meaningful.
type PlacementStrategy interface {
	Resolve(ctx context.Context, pool []TargetInfo) ([]TargetInfo, error)
}

// RolloutStrategy determines the pacing and ordering of delivery across targets.
//
// Plan receives the delta (what changed in the target set) and returns an
// ordered execution plan. The platform executes batches sequentially.
type RolloutStrategy interface {
	Plan(ctx context.Context, delta TargetDelta) (RolloutPlan, error)
}
