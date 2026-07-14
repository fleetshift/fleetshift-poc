package domain

import "maps"

// TargetState indicates where a target is in its lifecycle.
type TargetState string

const (
	TargetStateDiscovered   TargetState = "discovered"
	TargetStateInitializing TargetState = "initializing"
	TargetStateReady        TargetState = "ready"
	TargetStateDraining     TargetState = "draining"
	TargetStateTerminated   TargetState = "terminated"
)

// TargetInfo describes a registered target. It is the full state the platform
// knows: stored in the target repository, passed to delivery and manifest
// generation, and exposed via API. Properties are not used for placement;
// only the placement view (see [PlacementTarget]) is passed to placement
// strategies and considered for invalidation.
//
// Construct new instances with [NewTargetInfo]; reconstitute from
// persistence with [TargetInfoFromSnapshot]. Read via accessor methods.
type TargetInfo struct {
	id                    TargetID
	inventoryItemID       InventoryItemID
	targetType            TargetType
	name                  string
	state                 TargetState
	labels                map[string]string
	properties            map[string]string
	acceptedManifestTypes []ManifestType
}

// NewTargetInfo creates a brand-new [TargetInfo]. The
// [InventoryItemID] is derived as "target:<id>", enforcing the
// platform's naming convention for target-linked inventory items.
// Use this on creation paths; use [TargetInfoFromSnapshot] only for
// reconstituting from persistence.
func NewTargetInfo(id TargetID, targetType TargetType, name string, state TargetState, labels map[string]string, properties map[string]string, acceptedManifestTypes []ManifestType) TargetInfo {
	return TargetInfo{
		id:                    id,
		inventoryItemID:       InventoryItemID("target:" + string(id)),
		targetType:            targetType,
		name:                  name,
		state:                 state,
		labels:                labels,
		properties:            properties,
		acceptedManifestTypes: acceptedManifestTypes,
	}
}

// ID returns the target's unique identifier.
func (t TargetInfo) ID() TargetID { return t.id }

// InventoryItemID returns the linked inventory item's identifier.
func (t TargetInfo) InventoryItemID() InventoryItemID { return t.inventoryItemID }

// Type returns the target type (e.g. "kubernetes").
func (t TargetInfo) Type() TargetType { return t.targetType }

// Name returns the target's human-readable name.
func (t TargetInfo) Name() string { return t.name }

// State returns the current lifecycle state.
func (t TargetInfo) State() TargetState { return t.state }

// Labels returns the target's label set.
func (t TargetInfo) Labels() map[string]string { return t.labels }

// Properties returns the target's properties map.
func (t TargetInfo) Properties() map[string]string { return t.properties }

// AcceptedManifestTypes returns the manifest types the target accepts.
func (t TargetInfo) AcceptedManifestTypes() []ManifestType { return t.acceptedManifestTypes }

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
// AcceptedManifestTypes is included because it is a fundamental,
// immutable characteristic of a target. Placement strategies may use it
// to filter by supported manifest types, but are not required to.
type PlacementTarget struct {
	ID                    TargetID
	Type                  TargetType
	Name                  string
	State                 TargetState
	Labels                map[string]string
	AcceptedManifestTypes []ManifestType
}

// ToPlacementTarget returns the placement view of a target (Labels only;
// Properties are omitted).
func ToPlacementTarget(t TargetInfo) PlacementTarget {
	labels := make(map[string]string, len(t.labels))
	maps.Copy(labels, t.labels)
	amt := make([]ManifestType, len(t.acceptedManifestTypes))
	copy(amt, t.acceptedManifestTypes)
	return PlacementTarget{ID: t.id, Type: t.targetType, Name: t.name, State: t.state, Labels: labels, AcceptedManifestTypes: amt}
}

// PlacementTargets returns the placement view of each target in the slice.
// Only targets in [TargetStateReady] (or with empty state, for backward
// compatibility) are included; targets in other states are filtered out.
func PlacementTargets(pool []TargetInfo) []PlacementTarget {
	out := make([]PlacementTarget, 0, len(pool))
	for _, t := range pool {
		if t.state != TargetStateReady && t.state != "" {
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
		index[t.id] = t
	}
	out := make([]TargetInfo, 0, len(resolved))
	for _, p := range resolved {
		if t, ok := index[p.ID]; ok {
			out = append(out, t)
		}
	}
	return out
}
