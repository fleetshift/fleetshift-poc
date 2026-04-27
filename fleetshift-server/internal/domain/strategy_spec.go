package domain

// ManifestStrategyType identifies the kind of manifest strategy.
type ManifestStrategyType string

const (
	ManifestStrategyInline ManifestStrategyType = "inline"
	ManifestStrategyAddon  ManifestStrategyType = "addon"
)

// ManifestStrategySpec is the user-provided specification for manifest generation.
type ManifestStrategySpec struct {
	Type      ManifestStrategyType
	Manifests []Manifest // required for "inline"
	AddonName string     // required for "addon"
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

// VersionConflictPolicy determines behavior when a new generation is
// detected mid-rollout.
type VersionConflictPolicy string

const (
	// VersionConflictRestart aborts the current rollout and lets the
	// next reconciliation workflow start fresh. This is the default.
	VersionConflictRestart VersionConflictPolicy = "restart"

	// VersionConflictCompleteAll finishes the entire rollout before
	// yielding to the next generation.
	VersionConflictCompleteAll VersionConflictPolicy = "complete_all"
)

// RolloutStrategySpec is the user-provided specification for rollout pacing.
type RolloutStrategySpec struct {
	Type                  RolloutStrategyType
	VersionConflictPolicy VersionConflictPolicy `json:",omitempty"` // defaults to VersionConflictRestart
}

// EffectiveVersionConflictPolicy returns the configured policy, defaulting
// to [VersionConflictRestart] when unset.
func (s *RolloutStrategySpec) EffectiveVersionConflictPolicy() VersionConflictPolicy {
	if s == nil || s.VersionConflictPolicy == "" {
		return VersionConflictRestart
	}
	return s.VersionConflictPolicy
}
