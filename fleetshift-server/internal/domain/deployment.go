package domain

// DeploymentState indicates the lifecycle state of a deployment.
type DeploymentState string

const (
	DeploymentStatePending   DeploymentState = "pending"
	DeploymentStateActive    DeploymentState = "active"
	DeploymentStateDeleting  DeploymentState = "deleting"
)

// Deployment is the composition of manifest, placement, and rollout strategies.
type Deployment struct {
	ID                DeploymentID
	ManifestStrategy  ManifestStrategySpec
	PlacementStrategy PlacementStrategySpec
	RolloutStrategy   *RolloutStrategySpec // nil means immediate
	ResolvedTargets   []TargetID
	State             DeploymentState
}
