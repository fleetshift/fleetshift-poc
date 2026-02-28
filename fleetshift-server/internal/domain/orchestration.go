package domain

// TargetDelta represents the difference between the previous and current
// resolved target sets for a deployment.
type TargetDelta struct {
	Added     []TargetInfo
	Removed   []TargetInfo
	Unchanged []TargetInfo
}

// RolloutBatch represents a group of targets to update together.
type RolloutBatch struct {
	Targets []TargetInfo
}

// RolloutPlan is the output of a rollout strategy: an ordered sequence of batches.
type RolloutPlan struct {
	Batches []RolloutBatch
}

// GenerateContext provides the target context for manifest generation.
type GenerateContext struct {
	Target TargetInfo
	Config map[string]any
}
