package domain

// ManifestStrategyType identifies the kind of manifest strategy.
type ManifestStrategyType string

const (
	ManifestStrategyInline ManifestStrategyType = "inline"
)

// ManifestStrategySpec is the user-provided specification for manifest generation.
type ManifestStrategySpec struct {
	Type      ManifestStrategyType
	Manifests []Manifest // required for "inline"
}

// PlacementStrategyType identifies the kind of placement strategy.
type PlacementStrategyType string

const (
	PlacementStrategyStatic   PlacementStrategyType = "static"
	PlacementStrategyAll      PlacementStrategyType = "all"
	PlacementStrategySelector PlacementStrategyType = "selector"
)

// TargetSelector selects targets by label matching.
type TargetSelector struct {
	MatchLabels map[string]string
}

// PlacementStrategySpec is the user-provided specification for target selection.
type PlacementStrategySpec struct {
	Type           PlacementStrategyType
	Targets        []TargetID      // for "static"
	TargetSelector *TargetSelector // for "selector"
}

// RolloutStrategyType identifies the kind of rollout strategy.
type RolloutStrategyType string

const (
	RolloutStrategyImmediate RolloutStrategyType = "immediate"
)

// RolloutStrategySpec is the user-provided specification for rollout pacing.
type RolloutStrategySpec struct {
	Type RolloutStrategyType
}
