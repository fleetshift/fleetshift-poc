package domain

// PoolChange describes a change to the target pool. The workflow applies
// each change incrementally to its in-memory pool. Exactly one of the
// patterns below should be used per event:
//
//   - Set replaces the entire pool (initial load, periodic resync).
//   - Added/Removed/Updated are incremental mutations that may be
//     combined in a single event.
type PoolChange struct {
	Set     []TargetInfo // replace entire pool
	Added   []TargetInfo // targets joining the pool
	Removed []TargetID   // targets leaving the pool
	Updated []TargetInfo // targets whose placement-relevant state changed
}

// DeliveryCompletionEvent records the terminal outcome of a single delivery.
type DeliveryCompletionEvent struct {
	DeliveryID DeliveryID
	Result     DeliveryResult
}

// AuthResumedEvent carries fresh credentials from a user resuming a
// deployment that is in [DeploymentStatePausedAuth].
type AuthResumedEvent struct {
	Auth DeliveryAuth
}

// DeploymentEvent is the discriminated envelope delivered to a running
// orchestration workflow via [DeploymentWorkflowRunner.AwaitDeploymentEvent].
// Exactly one field is non-nil per event.
type DeploymentEvent struct {
	PoolChange          *PoolChange              // fleet membership or label change
	ManifestInvalidated bool                     // re-generate manifests for resolved set
	SpecChanged         bool                     // deployment spec changed; reload from repo
	Delete              bool                     // tear down delivery and exit
	DeliveryCompleted   *DeliveryCompletionEvent // a delivery reached a terminal state
	AuthResumed         *AuthResumedEvent        // fresh credentials from a resume
}

// ApplyPoolChange produces a new pool by applying a [PoolChange] to the
// current pool. If change.Set is non-nil it replaces the pool entirely;
// otherwise Added, Updated, and Removed are applied incrementally.
func ApplyPoolChange(pool []TargetInfo, change PoolChange) []TargetInfo {
	if change.Set != nil {
		return change.Set
	}

	index := make(map[TargetID]TargetInfo, len(pool))
	for _, t := range pool {
		index[t.ID] = t
	}
	for _, t := range change.Added {
		index[t.ID] = t
	}
	for _, t := range change.Updated {
		index[t.ID] = t
	}
	for _, id := range change.Removed {
		delete(index, id)
	}

	result := make([]TargetInfo, 0, len(index))
	for _, t := range index {
		result = append(result, t)
	}
	return result
}

// TargetInfosByID looks up each id in pool and returns the matching
// [TargetInfo] values. Unknown IDs are silently skipped.
func TargetInfosByID(ids []TargetID, pool []TargetInfo) []TargetInfo {
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
