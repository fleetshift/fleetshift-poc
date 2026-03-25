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
	CreatedAt         time.Time
	UpdatedAt         time.Time
	Etag              string
}
