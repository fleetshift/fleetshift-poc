package domain

import (
	"fmt"
	"time"
)

// FulfillmentID uniquely identifies a fulfillment.
type FulfillmentID string

// StrategyVersion is a monotonically increasing counter for a
// strategy within a [Fulfillment]. Each strategy type (manifest,
// placement, rollout) has its own independent version stream.
type StrategyVersion int64

// FulfillmentState indicates the lifecycle state of a fulfillment.
type FulfillmentState string

const (
	FulfillmentStateCreating   FulfillmentState = "creating"
	FulfillmentStateActive     FulfillmentState = "active"
	FulfillmentStateDeleting   FulfillmentState = "deleting"
	FulfillmentStateFailed     FulfillmentState = "failed"
	FulfillmentStatePausedAuth FulfillmentState = "paused_auth"
)

// Fulfillment is the kernel aggregate that owns strategies, state,
// generation, auth, provenance, and delivery. Orchestration operates
// on this type directly. User-facing concepts (Deployment,
// ManagedResource) hold a [FulfillmentID] reference and coordinate
// mutations through a domain service.
type Fulfillment struct {
	ID FulfillmentID

	// Strategies -- materialized from version tables on load.
	ManifestStrategy         ManifestStrategySpec
	ManifestStrategyVersion  StrategyVersion
	PlacementStrategy        PlacementStrategySpec
	PlacementStrategyVersion StrategyVersion
	RolloutStrategy          *RolloutStrategySpec
	RolloutStrategyVersion   StrategyVersion

	ResolvedTargets    []TargetID
	State              FulfillmentState
	StatusReason       string
	Auth               DeliveryAuth
	Provenance         *Provenance
	AttestationRef     *AttestationRef
	Generation         Generation
	ObservedGeneration Generation
	ActiveWorkflowGen  *Generation
	CreatedAt          time.Time
	UpdatedAt          time.Time

	// loadedGeneration is the generation value as read from the
	// database. Domain methods that represent user-initiated mutations
	// set Generation = loadedGeneration + 1, ensuring generation
	// advances exactly once per logical write transaction regardless
	// of how many fields change. The repository sets this on hydration
	// and persists Generation on save.
	loadedGeneration Generation

	pendingManifest  []ManifestStrategyRecord
	pendingPlacement []PlacementStrategyRecord
	pendingRollout   []RolloutStrategyRecord
}

// SetLoadedGeneration records the generation as read from persistence.
// Repository implementations call this after hydration so that
// [advanceGeneration] can enforce the single-bump invariant.
func (f *Fulfillment) SetLoadedGeneration(gen Generation) {
	f.loadedGeneration = gen
}

// advanceGeneration advances Generation to loadedGeneration + 1.
// Calling it multiple times within the same transaction is idempotent
// — generation advances by exactly one relative to the loaded value.
func (f *Fulfillment) advanceGeneration() {
	f.Generation = f.loadedGeneration + 1
}

// Resume transitions a paused fulfillment back to active reconciliation
// by replacing its delivery credentials and optionally its provenance,
// then bumping the generation to trigger orchestration.
//
// Returns [ErrInvalidArgument] if the fulfillment is not in
// [FulfillmentStatePausedAuth], or if the fulfillment previously had
// provenance but no replacement is supplied (re-signing is required).
//
// TODO: revisit the provenance requirement
// TODO: also revisit state requirement – maybe it's fine to "resume" something that is still active with new auth
func (f *Fulfillment) Resume(auth DeliveryAuth, provenance *Provenance) error {
	if f.State != FulfillmentStatePausedAuth {
		return fmt.Errorf("%w: fulfillment is in state %q, not paused_auth",
			ErrInvalidArgument, f.State)
	}
	if f.Provenance != nil && provenance == nil {
		return fmt.Errorf(
			"%w: fulfillment has provenance; re-signing is required to resume",
			ErrInvalidArgument)
	}
	f.Auth = auth
	if provenance != nil {
		f.Provenance = provenance
	}
	f.advanceGeneration()
	return nil
}

// Touch advances the generation and updates the timestamp without
// changing any other fulfillment state. This is useful for forcing
// orchestration to re-evaluate a fulfillment (e.g. diagnostics or
// administrative nudges).
func (f *Fulfillment) Touch(now time.Time) {
	f.UpdatedAt = now
	f.advanceGeneration()
}

// TransitionToDeleting moves the fulfillment into the
// [FulfillmentStateDeleting] lifecycle state and advances the
// generation so orchestration picks up the transition.
func (f *Fulfillment) TransitionToDeleting(auth DeliveryAuth) {
	f.Auth = auth
	f.State = FulfillmentStateDeleting
	f.advanceGeneration()
}

// AdvanceManifestStrategy advances the manifest strategy version,
// updates the materialized spec, collects a pending strategy record
// for the repository to flush, and advances generation. Multiple
// strategy advances within the same transaction are safe — generation
// advances are idempotent relative to loadedGeneration.
func (f *Fulfillment) AdvanceManifestStrategy(spec ManifestStrategySpec, now time.Time) {
	f.ManifestStrategyVersion++
	f.ManifestStrategy = spec
	f.pendingManifest = append(f.pendingManifest, ManifestStrategyRecord{
		FulfillmentID: f.ID,
		Version:       f.ManifestStrategyVersion,
		Spec:          spec,
		CreatedAt:     now,
	})
	f.advanceGeneration()
}

// AdvancePlacementStrategy advances the placement strategy version,
// updates the materialized spec, collects a pending strategy record
// for the repository to flush, and advances generation.
func (f *Fulfillment) AdvancePlacementStrategy(spec PlacementStrategySpec, now time.Time) {
	f.PlacementStrategyVersion++
	f.PlacementStrategy = spec
	f.pendingPlacement = append(f.pendingPlacement, PlacementStrategyRecord{
		FulfillmentID: f.ID,
		Version:       f.PlacementStrategyVersion,
		Spec:          spec,
		CreatedAt:     now,
	})
	f.advanceGeneration()
}

// AdvanceRolloutStrategy advances the rollout strategy version,
// updates the materialized spec, collects a pending strategy record
// for the repository to flush, and advances generation.
func (f *Fulfillment) AdvanceRolloutStrategy(spec *RolloutStrategySpec, now time.Time) {
	f.RolloutStrategyVersion++
	f.RolloutStrategy = spec
	f.pendingRollout = append(f.pendingRollout, RolloutStrategyRecord{
		FulfillmentID: f.ID,
		Version:       f.RolloutStrategyVersion,
		Spec:          spec,
		CreatedAt:     now,
	})
	f.advanceGeneration()
}

// ApplyReconciliationResult merges the observable state produced by a
// reconciliation workflow onto this fulfillment. Bookkeeping fields
// (Generation, ObservedGeneration) are left untouched so that
// concurrent service-layer mutations are preserved.
func (f *Fulfillment) ApplyReconciliationResult(r ReconciliationResult) {
	f.State = r.State
	f.StatusReason = r.StatusReason
	f.ResolvedTargets = r.ResolvedTargets
	f.Auth = r.Auth
}

// CompleteReconciliation advances [ObservedGeneration] to reconciledGen.
// If [Generation] has advanced past reconciledGen, needsRestart is true,
// indicating the caller should loop. When converged (!needsRestart),
// the orchestration lock ([ActiveWorkflowGen]) is cleared.
func (f *Fulfillment) CompleteReconciliation(reconciledGen Generation) (needsRestart bool) {
	f.ObservedGeneration = reconciledGen
	needsRestart = f.Generation > reconciledGen
	if !needsRestart {
		f.ActiveWorkflowGen = nil
	}
	return needsRestart
}

// AcquireOrchestrationLock sets [ActiveWorkflowGen] to the current
// [Generation], indicating an orchestration workflow is running.
// Returns false if the lock is already held.
func (f *Fulfillment) AcquireOrchestrationLock() bool {
	if f.ActiveWorkflowGen != nil {
		return false
	}
	gen := f.Generation
	f.ActiveWorkflowGen = &gen
	return true
}

// ReleaseOrchestrationLock clears [ActiveWorkflowGen] without
// advancing [ObservedGeneration]. Used before ContinueAsNew so the
// next execution can re-acquire the lock for a fresh attempt.
func (f *Fulfillment) ReleaseOrchestrationLock() {
	f.ActiveWorkflowGen = nil
}

// ManifestStrategyRecord is an append-only version record for manifest strategies.
type ManifestStrategyRecord struct {
	FulfillmentID FulfillmentID
	Version       StrategyVersion
	Spec          ManifestStrategySpec
	CreatedAt     time.Time
}

// PlacementStrategyRecord is an append-only version record for placement strategies.
type PlacementStrategyRecord struct {
	FulfillmentID FulfillmentID
	Version       StrategyVersion
	Spec          PlacementStrategySpec
	CreatedAt     time.Time
}

// RolloutStrategyRecord is an append-only version record for rollout strategies.
type RolloutStrategyRecord struct {
	FulfillmentID FulfillmentID
	Version       StrategyVersion
	Spec          *RolloutStrategySpec
	CreatedAt     time.Time
}

// PendingStrategyRecords holds strategy version records that have been
// collected by Advance* methods but not yet flushed to storage.
type PendingStrategyRecords struct {
	Manifest  []ManifestStrategyRecord
	Placement []PlacementStrategyRecord
	Rollout   []RolloutStrategyRecord
}

// DrainPendingStrategyRecords returns and clears the collected
// strategy version records. Called by the repository implementation
// inside Create and Update.
func (f *Fulfillment) DrainPendingStrategyRecords() PendingStrategyRecords {
	p := PendingStrategyRecords{
		Manifest:  f.pendingManifest,
		Placement: f.pendingPlacement,
		Rollout:   f.pendingRollout,
	}
	f.pendingManifest = nil
	f.pendingPlacement = nil
	f.pendingRollout = nil
	return p
}

// ReconciliationResult captures the observable state produced by a
// single reconciliation workflow run. It is the typed output that the
// workflow hands to the [PersistReconciliationResult] activity, making the
// contract between workflow and persistence explicit.
type ReconciliationResult struct {
	FulfillmentID   FulfillmentID
	State           FulfillmentState
	StatusReason    string // human-readable; populated on failure, cleared on success
	ResolvedTargets []TargetID
	Auth            DeliveryAuth
}
