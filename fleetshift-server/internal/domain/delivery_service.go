package domain

import "context"

// DeliveryService is the port through which the orchestration pipeline
// delivers manifests to targets. The real implementation sends manifests
// through the fleetlet; the initial implementation records deliveries
// in the database.
type DeliveryService interface {
	Deliver(ctx context.Context, target TargetInfo, deploymentID DeploymentID, manifests []Manifest) (DeliveryResult, error)
	Remove(ctx context.Context, target TargetInfo, deploymentID DeploymentID) error
}
