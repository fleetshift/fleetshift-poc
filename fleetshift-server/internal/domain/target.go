package domain

// TargetState indicates where a target is in its lifecycle.
type TargetState string

const (
	TargetStateDiscovered  TargetState = "discovered"
	TargetStateInitializing TargetState = "initializing"
	TargetStateReady       TargetState = "ready"
	TargetStateDraining    TargetState = "draining"
	TargetStateTerminated  TargetState = "terminated"
)

// TargetInfo describes a registered target. It is the full state the platform
// knows: stored in the target repository, passed to delivery and manifest
// generation, and exposed via API. Properties are not used for placement;
// only the placement view (see [PlacementTarget]) is passed to placement
// strategies and considered for invalidation.
type TargetInfo struct {
	ID                    TargetID
	InventoryItemID       InventoryItemID
	Type                  TargetType
	Name                  string
	State                 TargetState
	Labels                map[string]string
	Properties            map[string]string
	AcceptedResourceTypes []ResourceType
}

// PlacementTarget is the subset of target state shared with placement
// strategies. Only these fields are visible to placement and drive
// re-resolution when they change. Properties and other target metadata
// are excluded so they can change without triggering placement invalidation.
//
// Type is included because it is a fundamental, immutable characteristic
// of a target (changing type = registering a new target). Placement
// strategies may use it to filter by target type, but are not required to.
//
// State is included so placement strategies can enforce readiness
// requirements (only [TargetStateReady] targets are eligible by default).
//
// AcceptedResourceTypes is included because it is a fundamental,
// immutable characteristic of a target. Placement strategies may use it
// to filter by supported manifest types, but are not required to.
type PlacementTarget struct {
	ID                    TargetID
	Type                  TargetType
	Name                  string
	State                 TargetState
	Labels                map[string]string
	AcceptedResourceTypes []ResourceType
}

// ToPlacementTarget returns the placement view of a target (Labels only;
// Properties are omitted).
func ToPlacementTarget(t TargetInfo) PlacementTarget {
	labels := make(map[string]string, len(t.Labels))
	for k, v := range t.Labels {
		labels[k] = v
	}
	art := make([]ResourceType, len(t.AcceptedResourceTypes))
	copy(art, t.AcceptedResourceTypes)
	return PlacementTarget{ID: t.ID, Type: t.Type, Name: t.Name, State: t.State, Labels: labels, AcceptedResourceTypes: art}
}

// PlacementTargets returns the placement view of each target in the slice.
// Only targets in [TargetStateReady] (or with empty state, for backward
// compatibility) are included; targets in other states are filtered out.
func PlacementTargets(pool []TargetInfo) []PlacementTarget {
	out := make([]PlacementTarget, 0, len(pool))
	for _, t := range pool {
		if t.State != TargetStateReady && t.State != "" {
			continue
		}
		out = append(out, ToPlacementTarget(t))
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

