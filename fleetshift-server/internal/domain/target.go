package domain

// TargetInfo describes a registered target. It is the full state the platform
// knows: stored in the target repository, passed to delivery and manifest
// generation, and exposed via API. Properties are not used for placement;
// only the placement view (see [PlacementTarget]) is passed to placement
// strategies and considered for invalidation.
type TargetInfo struct {
	ID         TargetID
	Name       string
	Labels     map[string]string
	Properties map[string]string
}

// PlacementTarget is the subset of target state shared with placement
// strategies. Only these fields are visible to placement and drive
// re-resolution when they change. Properties and other target metadata
// are excluded so they can change without triggering placement invalidation.
type PlacementTarget struct {
	ID     TargetID
	Name   string
	Labels map[string]string
}

// ToPlacementTarget returns the placement view of a target (Labels only;
// Properties are omitted).
func ToPlacementTarget(t TargetInfo) PlacementTarget {
	labels := make(map[string]string, len(t.Labels))
	for k, v := range t.Labels {
		labels[k] = v
	}
	return PlacementTarget{ID: t.ID, Name: t.Name, Labels: labels}
}

// PlacementTargets returns the placement view of each target in the slice.
func PlacementTargets(pool []TargetInfo) []PlacementTarget {
	out := make([]PlacementTarget, len(pool))
	for i, t := range pool {
		out[i] = ToPlacementTarget(t)
	}
	return out
}

// ResolvedTargetInfos maps resolved placement targets back to full target info
// by looking up each ID in the full pool. Order of the resolved slice is
// preserved. Targets not found in the pool are omitted (caller can treat that
// as an error if the pool is expected to be complete).
func ResolvedTargetInfos(resolved []PlacementTarget, pool []TargetInfo) []TargetInfo {
	index := make(map[TargetID]TargetInfo, len(pool))
	for _, t := range pool {
		index[t.ID] = t
	}
	out := make([]TargetInfo, 0, len(resolved))
	for _, p := range resolved {
		if t, ok := index[p.ID]; ok {
			out = append(out, t)
		}
	}
	return out
}
