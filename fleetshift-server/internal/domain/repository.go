package domain

import "context"

// TargetRepository persists and retrieves target metadata.
type TargetRepository interface {
	Create(ctx context.Context, target TargetInfo) error
	Get(ctx context.Context, id TargetID) (TargetInfo, error)
	List(ctx context.Context) ([]TargetInfo, error)
	Delete(ctx context.Context, id TargetID) error
}

// DeploymentRepository persists and retrieves deployments.
type DeploymentRepository interface {
	Create(ctx context.Context, d Deployment) error
	Get(ctx context.Context, id DeploymentID) (Deployment, error)
	List(ctx context.Context) ([]Deployment, error)
	Update(ctx context.Context, d Deployment) error
	Delete(ctx context.Context, id DeploymentID) error
}

// DeliveryRecordRepository persists delivery records for each
// deployment-target pair.
type DeliveryRecordRepository interface {
	Put(ctx context.Context, record DeliveryRecord) error
	Get(ctx context.Context, deploymentID DeploymentID, targetID TargetID) (DeliveryRecord, error)
	ListByDeployment(ctx context.Context, deploymentID DeploymentID) ([]DeliveryRecord, error)
	DeleteByDeployment(ctx context.Context, deploymentID DeploymentID) error
}
