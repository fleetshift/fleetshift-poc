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

// InventoryRepository persists and retrieves inventory items.
type InventoryRepository interface {
	Create(ctx context.Context, item InventoryItem) error
	Get(ctx context.Context, id InventoryItemID) (InventoryItem, error)
	List(ctx context.Context) ([]InventoryItem, error)
	ListByType(ctx context.Context, t InventoryType) ([]InventoryItem, error)
	Update(ctx context.Context, item InventoryItem) error
	Delete(ctx context.Context, id InventoryItemID) error
}

// DeliveryRepository persists deliveries for each deployment-target pair.
type DeliveryRepository interface {
	Put(ctx context.Context, d Delivery) error
	Get(ctx context.Context, id DeliveryID) (Delivery, error)
	GetByDeploymentTarget(ctx context.Context, deploymentID DeploymentID, targetID TargetID) (Delivery, error)
	ListByDeployment(ctx context.Context, deploymentID DeploymentID) ([]Delivery, error)
	DeleteByDeployment(ctx context.Context, deploymentID DeploymentID) error
}
