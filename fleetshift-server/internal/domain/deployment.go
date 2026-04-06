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
	ID                DeploymentID
	UID               string
	ManifestStrategy  ManifestStrategySpec
	PlacementStrategy PlacementStrategySpec
	RolloutStrategy   *RolloutStrategySpec // nil means immediate
	ResolvedTargets   []TargetID
	State             DeploymentState
	Auth              DeliveryAuth // passthrough credentials; may change over time (e.g. token refresh)
	Provenance        *Provenance  // nil for token-passthrough deployments
	Generation         Generation  // incremented on every mutation; starts at 1
	ObservedGeneration Generation  // last generation fully reconciled by a workflow
	CreatedAt         time.Time
	UpdatedAt         time.Time
	Etag              string
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
func NewFailedResult(id DeploymentID, auth DeliveryAuth) ReconciliationResult {
	return ReconciliationResult{
		DeploymentID: id,
		State:        DeploymentStateFailed,
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

// NewDeletedResult builds a result after all deliveries have been
// removed during a delete pipeline.
func NewDeletedResult(id DeploymentID) ReconciliationResult {
	return ReconciliationResult{
		DeploymentID: id,
		State:        DeploymentStateDeleting,
	}
}

// ApplyReconciliationResult merges the observable state produced by a
// reconciliation workflow onto this deployment. Bookkeeping fields
// (Generation, ObservedGeneration) are left untouched so that
// concurrent service-layer mutations are preserved.
func (d *Deployment) ApplyReconciliationResult(r ReconciliationResult) {
	d.State = r.State
	d.ResolvedTargets = r.ResolvedTargets
	d.Auth = r.Auth
}

// CompleteReconciliation advances [ObservedGeneration] to reconciledGen.
// If [Generation] has advanced past reconciledGen, needsRestart is true,
// indicating the caller should start a new reconciliation workflow.
func (d *Deployment) CompleteReconciliation(reconciledGen Generation) (needsRestart bool) {
	d.ObservedGeneration = reconciledGen
	return d.Generation > reconciledGen
}
