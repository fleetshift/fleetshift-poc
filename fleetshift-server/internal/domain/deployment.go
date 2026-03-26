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
	Auth              DeliveryAuth // authorization context; may change over time (e.g. token refresh)
	Generation         Generation // incremented on every mutation; starts at 1
	ObservedGeneration Generation // last generation fully reconciled by a workflow
	Reconciling        bool       // true while a reconciliation workflow is running
	CreatedAt         time.Time
	UpdatedAt         time.Time
	Etag              string
}

// BumpGeneration increments the deployment's generation counter.
func (d *Deployment) BumpGeneration() {
	d.Generation++
}

// TryAcquireReconciliation attempts to acquire the reconciliation lock.
// Returns true if the lock was acquired (was not already held).
func (d *Deployment) TryAcquireReconciliation() bool {
	if d.Reconciling {
		return false
	}
	d.Reconciling = true
	return true
}

// MarkActive records a successful reconciliation: the deployment is
// now active with the given resolved targets.
func (d *Deployment) MarkActive(resolvedTargets []TargetID) {
	d.State = DeploymentStateActive
	d.ResolvedTargets = resolvedTargets
}

// MarkFailed records that the reconciliation pipeline failed.
func (d *Deployment) MarkFailed() {
	d.State = DeploymentStateFailed
}

// MarkPausedAuth records that a delivery reported an authentication
// failure and the deployment is paused until resumed.
func (d *Deployment) MarkPausedAuth() {
	d.State = DeploymentStatePausedAuth
}

// MarkDeleted clears the resolved targets after all deliveries have
// been removed. The state remains [DeploymentStateDeleting] so the
// service layer can perform final cleanup (e.g. row deletion).
func (d *Deployment) MarkDeleted() {
	d.ResolvedTargets = nil
	d.State = DeploymentStateDeleting
}

// ApplyReconciliationResult merges the observable state produced by a
// reconciliation workflow onto this deployment. Bookkeeping fields
// (Generation, ObservedGeneration, Reconciling) are left untouched
// so that concurrent service-layer mutations are preserved.
func (d *Deployment) ApplyReconciliationResult(source Deployment) {
	d.State = source.State
	d.ResolvedTargets = source.ResolvedTargets
	d.Auth = source.Auth
}

// CompleteReconciliation releases the reconciliation lock and advances
// [ObservedGeneration] to reconciledGen. If [Generation] has advanced
// past reconciledGen, the lock is re-acquired and needsRestart is true,
// indicating the caller should start a new reconciliation workflow.
func (d *Deployment) CompleteReconciliation(reconciledGen Generation) (needsRestart bool) {
	d.ObservedGeneration = reconciledGen
	if d.Generation > reconciledGen {
		d.Reconciling = true
		return true
	}
	d.Reconciling = false
	return false
}
