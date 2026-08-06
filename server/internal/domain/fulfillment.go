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
	FulfillmentStateCreating FulfillmentState = "creating"
	FulfillmentStateActive   FulfillmentState = "active"
	FulfillmentStateDeleting FulfillmentState = "deleting"
	FulfillmentStateFailed   FulfillmentState = "failed"
)

// Fulfillment is the kernel aggregate that owns strategies, state,
// generation, auth, provenance, and delivery. Orchestration operates
// on this type directly. User-facing concepts (Deployment,
// ManagedResource) hold a [FulfillmentID] reference and coordinate
// mutations through a domain service.
//
// Construct new instances with [NewFulfillment]; reconstitute from
// persistence with [FulfillmentFromSnapshot]. Mutations go through
// domain methods; reads go through accessor methods.
type Fulfillment struct {
	id FulfillmentID

	// Strategies -- materialized from version tables on load.
	manifestStrategy         ManifestStrategySpec
	manifestStrategyVersion  StrategyVersion
	placementStrategy        PlacementStrategySpec
	placementStrategyVersion StrategyVersion
	rolloutStrategy          *RolloutStrategySpec
	rolloutStrategyVersion   StrategyVersion

	resolvedTargets    []TargetID
	state              FulfillmentState
	pauseReason        string
	statusReason       string
	auth               DeliveryAuth
	provenance         *Provenance
	attestationRef     *AttestationRef
	generation         Generation
	observedGeneration Generation
	activeWorkflowGen  *Generation
	createdAt          time.Time
	updatedAt          time.Time

	// loadedGeneration is the generation value as read from the
	// database. Domain methods that represent user-initiated mutations
	// set generation = loadedGeneration + 1, ensuring generation
	// advances exactly once per logical write transaction regardless
	// of how many fields change. [FulfillmentFromSnapshot] sets this
	// on hydration; the repository persists generation on save.
	loadedGeneration Generation

	pendingManifest  []ManifestStrategyRecord
	pendingPlacement []PlacementStrategyRecord
	pendingRollout   []RolloutStrategyRecord
}

// NewFulfillment creates a brand-new [Fulfillment] in the
// [FulfillmentStateCreating] lifecycle state. Use this on creation
// paths; use [FulfillmentFromSnapshot] only for reconstituting from
// persistence.
//
// After construction, call [Fulfillment.AdvanceManifestStrategy],
// [Fulfillment.AdvancePlacementStrategy], and optionally
// [Fulfillment.AdvanceRolloutStrategy] to attach initial strategies.
func NewFulfillment(id FulfillmentID, auth DeliveryAuth, provenance *Provenance, attestRef *AttestationRef, now time.Time) *Fulfillment {
	return &Fulfillment{
		id:             id,
		state:          FulfillmentStateCreating,
		auth:           auth,
		provenance:     provenance,
		attestationRef: attestRef,
		createdAt:      now,
		updatedAt:      now,
	}
}

// advanceGeneration advances generation to loadedGeneration + 1.
// Calling it multiple times within the same transaction is idempotent
// — generation advances by exactly one relative to the loaded value.
func (f *Fulfillment) advanceGeneration() {
	f.generation = f.loadedGeneration + 1
}

// Resume transitions a paused fulfillment back to active reconciliation
// by replacing its delivery credentials and optionally its provenance,
// then bumping the generation to trigger orchestration.
//
// Returns [ErrInvalidArgument] if the fulfillment is not paused, or if
// the fulfillment previously had provenance but no replacement is
// supplied (re-signing is required).
//
// TODO: revisit the provenance requirement
// TODO: also revisit status requirement; maybe it's fine to "resume"
// something not paused, to change the auth
func (f *Fulfillment) Resume(auth DeliveryAuth, provenance *Provenance) error {
	if f.pauseReason == "" {
		return fmt.Errorf("%w: fulfillment is not paused", ErrInvalidArgument)
	}
	if f.provenance != nil && provenance == nil {
		return fmt.Errorf(
			"%w: fulfillment has provenance; re-signing is required to resume",
			ErrInvalidArgument)
	}
	f.auth = auth
	if provenance != nil {
		f.provenance = provenance
	}
	f.pauseReason = ""
	f.advanceGeneration()
	return nil
}

// Touch advances the generation and updates the timestamp without
// changing any other fulfillment state. This is useful for forcing
// orchestration to re-evaluate a fulfillment (e.g. diagnostics or
// administrative nudges).
func (f *Fulfillment) Touch(now time.Time) {
	f.updatedAt = now
	f.advanceGeneration()
}

// TransitionToDeleting moves the fulfillment into the
// [FulfillmentStateDeleting] lifecycle state and advances the
// generation so orchestration picks up the transition.
func (f *Fulfillment) TransitionToDeleting(auth DeliveryAuth) {
	f.auth = auth
	f.state = FulfillmentStateDeleting
	f.pauseReason = ""
	f.advanceGeneration()
}

// AdvanceManifestStrategy advances the manifest strategy version,
// updates the materialized spec, collects a pending strategy record
// for the repository to flush, and advances generation. Multiple
// strategy advances within the same transaction are safe — generation
// advances are idempotent relative to loadedGeneration.
func (f *Fulfillment) AdvanceManifestStrategy(spec ManifestStrategySpec, now time.Time) {
	f.manifestStrategyVersion++
	f.manifestStrategy = spec
	f.pendingManifest = append(f.pendingManifest, ManifestStrategyRecord{
		FulfillmentID: f.id,
		Version:       f.manifestStrategyVersion,
		Spec:          spec,
		CreatedAt:     now,
	})
	f.advanceGeneration()
}

// AdvancePlacementStrategy advances the placement strategy version,
// updates the materialized spec, collects a pending strategy record
// for the repository to flush, and advances generation.
func (f *Fulfillment) AdvancePlacementStrategy(spec PlacementStrategySpec, now time.Time) {
	f.placementStrategyVersion++
	f.placementStrategy = spec
	f.pendingPlacement = append(f.pendingPlacement, PlacementStrategyRecord{
		FulfillmentID: f.id,
		Version:       f.placementStrategyVersion,
		Spec:          spec,
		CreatedAt:     now,
	})
	f.advanceGeneration()
}

// AdvanceRolloutStrategy advances the rollout strategy version,
// updates the materialized spec, collects a pending strategy record
// for the repository to flush, and advances generation.
func (f *Fulfillment) AdvanceRolloutStrategy(spec *RolloutStrategySpec, now time.Time) {
	f.rolloutStrategyVersion++
	f.rolloutStrategy = spec
	f.pendingRollout = append(f.pendingRollout, RolloutStrategyRecord{
		FulfillmentID: f.id,
		Version:       f.rolloutStrategyVersion,
		Spec:          spec,
		CreatedAt:     now,
	})
	f.advanceGeneration()
}

// ApplyReconciliationResult merges the observable state produced by a
// reconciliation workflow onto this fulfillment. When PauseReason is
// set on the result, the lifecycle state is left unchanged and only
// the pause reason is applied. Otherwise state advances normally and
// any prior pause is cleared. Bookkeeping fields (Generation,
// ObservedGeneration) are left untouched so that concurrent
// service-layer mutations are preserved.
func (f *Fulfillment) ApplyReconciliationResult(r ReconciliationResult) {
	if r.PauseReason != "" {
		f.pauseReason = r.PauseReason
	} else {
		f.state = r.State
		f.pauseReason = ""
	}
	f.statusReason = r.StatusReason
	f.resolvedTargets = r.ResolvedTargets
	f.auth = r.Auth
}

// CompleteReconciliation advances observedGeneration to reconciledGen
// and stamps updatedAt. If generation has advanced past reconciledGen,
// needsRestart is true, indicating the caller should loop. When
// converged (!needsRestart), the orchestration lock
// (activeWorkflowGen) is cleared.
func (f *Fulfillment) CompleteReconciliation(reconciledGen Generation, now time.Time) (needsRestart bool) {
	f.observedGeneration = reconciledGen
	f.updatedAt = now
	needsRestart = f.generation > reconciledGen
	if !needsRestart {
		f.activeWorkflowGen = nil
	}
	return needsRestart
}

// AcquireOrchestrationLock sets activeWorkflowGen to the current
// generation, indicating an orchestration workflow is running.
// Returns false if the lock is already held.
func (f *Fulfillment) AcquireOrchestrationLock() bool {
	if f.activeWorkflowGen != nil {
		return false
	}
	gen := f.generation
	f.activeWorkflowGen = &gen
	return true
}

// ReleaseOrchestrationLock clears activeWorkflowGen without
// advancing observedGeneration. Used before ContinueAsNew so the
// next execution can re-acquire the lock for a fresh attempt.
func (f *Fulfillment) ReleaseOrchestrationLock() {
	f.activeWorkflowGen = nil
}

// DrainPendingStrategyRecords returns the pending strategy records
// collected by Advance* methods and nils the internal buffers.
// Repositories call this to extract records for flushing to storage,
// ensuring each record is written exactly once. Subsequent calls (or
// [Fulfillment.Snapshot]) will see empty pending buffers.
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

// Accessor methods -- read-only getters for private fields.

// ID returns the fulfillment's unique identifier.
func (f *Fulfillment) ID() FulfillmentID { return f.id }

// State returns the current lifecycle state.
func (f *Fulfillment) State() FulfillmentState { return f.state }

// Paused reports whether orchestration is paused for this fulfillment.
func (f *Fulfillment) Paused() bool { return f.pauseReason != "" }

// Reconciling reports whether the fulfillment is in a transitional
// lifecycle state (creating or deleting) and is not paused. A paused
// fulfillment is not actively reconciling even if its lifecycle state
// has not yet settled.
func (f *Fulfillment) Reconciling() bool {
	return (f.state == FulfillmentStateCreating || f.state == FulfillmentStateDeleting) && f.pauseReason == ""
}

// PauseReason returns the human-readable reason for the pause, or
// empty if the fulfillment is not paused.
func (f *Fulfillment) PauseReason() string { return f.pauseReason }

// Generation returns the current generation counter.
func (f *Fulfillment) Generation() Generation { return f.generation }

// ObservedGeneration returns the last reconciled generation.
func (f *Fulfillment) ObservedGeneration() Generation { return f.observedGeneration }

// ActiveWorkflowGen returns the generation at which the orchestration
// lock was acquired, or nil if no workflow is active.
func (f *Fulfillment) ActiveWorkflowGen() *Generation { return f.activeWorkflowGen }

// Auth returns the delivery credentials.
func (f *Fulfillment) Auth() DeliveryAuth { return f.auth }

// Provenance returns the cryptographic provenance, if any.
func (f *Fulfillment) Provenance() *Provenance { return f.provenance }

// AttestationRef returns the attestation reference, if any.
func (f *Fulfillment) AttestationRef() *AttestationRef { return f.attestationRef }

// ResolvedTargets returns the resolved placement targets.
func (f *Fulfillment) ResolvedTargets() []TargetID { return f.resolvedTargets }

// StatusReason returns the human-readable status reason.
func (f *Fulfillment) StatusReason() string { return f.statusReason }

// ManifestStrategy returns the current manifest strategy spec.
func (f *Fulfillment) ManifestStrategy() ManifestStrategySpec { return f.manifestStrategy }

// ManifestStrategyVersion returns the current manifest strategy version.
func (f *Fulfillment) ManifestStrategyVersion() StrategyVersion { return f.manifestStrategyVersion }

// PlacementStrategy returns the current placement strategy spec.
func (f *Fulfillment) PlacementStrategy() PlacementStrategySpec { return f.placementStrategy }

// PlacementStrategyVersion returns the current placement strategy version.
func (f *Fulfillment) PlacementStrategyVersion() StrategyVersion { return f.placementStrategyVersion }

// RolloutStrategy returns the current rollout strategy spec, if any.
func (f *Fulfillment) RolloutStrategy() *RolloutStrategySpec { return f.rolloutStrategy }

// RolloutStrategyVersion returns the current rollout strategy version.
func (f *Fulfillment) RolloutStrategyVersion() StrategyVersion { return f.rolloutStrategyVersion }

// CreatedAt returns the creation timestamp.
func (f *Fulfillment) CreatedAt() time.Time { return f.createdAt }

// UpdatedAt returns the last-updated timestamp.
func (f *Fulfillment) UpdatedAt() time.Time { return f.updatedAt }

// ReconciliationResult captures the observable state produced by a
// single reconciliation workflow run. It is the typed output that the
// workflow hands to the [PersistReconciliationResult] activity, making the
// contract between workflow and persistence explicit.
//
// When PauseReason is non-empty the fulfillment is being paused; State
// is ignored and the lifecycle state is left unchanged by
// [Fulfillment.ApplyReconciliationResult].
type ReconciliationResult struct {
	FulfillmentID   FulfillmentID
	State           FulfillmentState
	PauseReason     string // non-empty pauses fulfillment without changing lifecycle state
	StatusReason    string // human-readable; populated on failure, cleared on success
	ResolvedTargets []TargetID
	Auth            DeliveryAuth
}
