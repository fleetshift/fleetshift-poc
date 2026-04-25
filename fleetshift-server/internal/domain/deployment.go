package domain

import "time"

// DeploymentState indicates the lifecycle state of a deployment.
type DeploymentState string

const (
	DeploymentStateCreating    DeploymentState = "creating"
	DeploymentStateActive      DeploymentState = "active"
	DeploymentStateDeleting    DeploymentState = "deleting"
	DeploymentStateFailed      DeploymentState = "failed"
	DeploymentStatePausedAuth  DeploymentState = "paused_auth"
)

// Generation is a monotonically increasing counter on a [Deployment].
// It is bumped on every mutation (create, invalidation, resume, delete)
// and compared against [ObservedGeneration] to detect pending work.
type Generation int64

// Deployment is the composition of manifest, placement, and rollout strategies.
type Deployment struct {
	ID                 DeploymentID
	UID                string
	ManifestStrategy   ManifestStrategySpec
	PlacementStrategy  PlacementStrategySpec
	RolloutStrategy    *RolloutStrategySpec // nil means immediate
	ResolvedTargets    []TargetID
	State              DeploymentState
	// TODO: consider replacing status reason with conditions
	StatusReason       string       // human-readable explanation for the current state; cleared on success
	Auth               DeliveryAuth // passthrough credentials; may change over time (e.g. token refresh)
	Provenance         *Provenance  // nil for token-passthrough deployments
	Generation         Generation   // incremented on every mutation; starts at 1
	ObservedGeneration Generation   // last generation fully reconciled by a workflow
	ActiveWorkflowGen  *Generation  // non-nil while an orchestration workflow holds the lock
	CreatedAt          time.Time
	UpdatedAt          time.Time
	Etag               string
}

// BumpGeneration increments the deployment's generation counter.
func (d *Deployment) BumpGeneration() {
	d.Generation++
}

// ReconciliationResult captures the observable state produced by a
// single reconciliation workflow run. It is the typed output that the
// workflow hands to the [PersistReconciliationResult] activity, making the
// contract between workflow and persistence explicit.
type ReconciliationResult struct {
	DeploymentID    DeploymentID
	State           DeploymentState
	StatusReason    string // human-readable; populated on failure, cleared on success
	ResolvedTargets []TargetID
	Auth            DeliveryAuth
}

// NewActiveResult builds a result for a successful reconciliation.
func NewActiveResult(id DeploymentID, resolvedTargets []TargetID, auth DeliveryAuth) ReconciliationResult {
	return ReconciliationResult{
		DeploymentID:    id,
		State:           DeploymentStateActive,
		ResolvedTargets: resolvedTargets,
		Auth:            auth,
	}
}

// NewFailedResult builds a result for a failed reconciliation pipeline.
func NewFailedResult(id DeploymentID, auth DeliveryAuth, reason string) ReconciliationResult {
	return ReconciliationResult{
		DeploymentID: id,
		State:        DeploymentStateFailed,
		StatusReason: reason,
		Auth:         auth,
	}
}

// NewPausedAuthResult builds a result when a delivery reports an
// authentication failure and the deployment should pause.
func NewPausedAuthResult(id DeploymentID, auth DeliveryAuth) ReconciliationResult {
	return ReconciliationResult{
		DeploymentID: id,
		State:        DeploymentStatePausedAuth,
		Auth:         auth,
	}
}

// ApplyReconciliationResult merges the observable state produced by a
// reconciliation workflow onto this deployment. Bookkeeping fields
// (Generation, ObservedGeneration) are left untouched so that
// concurrent service-layer mutations are preserved.
func (d *Deployment) ApplyReconciliationResult(r ReconciliationResult) {
	d.State = r.State
	d.StatusReason = r.StatusReason
	d.ResolvedTargets = r.ResolvedTargets
	d.Auth = r.Auth
}

// CompleteReconciliation advances [ObservedGeneration] to reconciledGen.
// If [Generation] has advanced past reconciledGen, needsRestart is true,
// indicating the caller should loop. When converged (!needsRestart),
// the orchestration lock ([ActiveWorkflowGen]) is cleared.
func (d *Deployment) CompleteReconciliation(reconciledGen Generation) (needsRestart bool) {
	d.ObservedGeneration = reconciledGen
	needsRestart = d.Generation > reconciledGen
	if !needsRestart {
		d.ActiveWorkflowGen = nil
	}
	return needsRestart
}

// AcquireOrchestrationLock sets [ActiveWorkflowGen] to the current
// [Generation], indicating an orchestration workflow is running.
// Returns false if the lock is already held.
func (d *Deployment) AcquireOrchestrationLock() bool {
	if d.ActiveWorkflowGen != nil {
		return false
	}
	gen := d.Generation
	d.ActiveWorkflowGen = &gen
	return true
}

// ReleaseOrchestrationLock clears [ActiveWorkflowGen] without
// advancing [ObservedGeneration]. Used before ContinueAsNew so the
// next execution can re-acquire the lock for a fresh attempt.
func (d *Deployment) ReleaseOrchestrationLock() {
	d.ActiveWorkflowGen = nil
}
