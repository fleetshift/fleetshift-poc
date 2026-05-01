package domain

import "context"

// TargetRepository persists and retrieves target metadata.
type TargetRepository interface {
	Create(ctx context.Context, target TargetInfo) error
	CreateOrUpdate(ctx context.Context, target TargetInfo) error
	Get(ctx context.Context, id TargetID) (TargetInfo, error)
	List(ctx context.Context) ([]TargetInfo, error)
	Delete(ctx context.Context, id TargetID) error
}

// FulfillmentRepository persists and retrieves fulfillments.
// Create and Update drain pending strategy records (via
// [Fulfillment.DrainPendingStrategyRecords]) and insert them.
// Get materializes current strategy specs by joining the version tables.
type FulfillmentRepository interface {
	Create(ctx context.Context, f Fulfillment) error
	Get(ctx context.Context, id FulfillmentID) (Fulfillment, error)
	Update(ctx context.Context, f Fulfillment) error
	Delete(ctx context.Context, id FulfillmentID) error
}

// DeploymentRepository persists and retrieves the thin deployment
// aggregate. Mutations that affect orchestration state go through
// [FulfillmentRepository].
type DeploymentRepository interface {
	Create(ctx context.Context, d Deployment) error
	Get(ctx context.Context, id DeploymentID) (Deployment, error)
	GetView(ctx context.Context, id DeploymentID) (DeploymentView, error)
	ListView(ctx context.Context) ([]DeploymentView, error)
	Delete(ctx context.Context, id DeploymentID) error
}

// InventoryRepository persists and retrieves inventory items.
type InventoryRepository interface {
	Create(ctx context.Context, item InventoryItem) error
	CreateOrUpdate(ctx context.Context, item InventoryItem) error
	Get(ctx context.Context, id InventoryItemID) (InventoryItem, error)
	List(ctx context.Context) ([]InventoryItem, error)
	ListByType(ctx context.Context, t InventoryType) ([]InventoryItem, error)
	Update(ctx context.Context, item InventoryItem) error
	Delete(ctx context.Context, id InventoryItemID) error
}

// DeliveryRepository persists deliveries for each fulfillment-target pair.
type DeliveryRepository interface {
	Put(ctx context.Context, d Delivery) error
	Get(ctx context.Context, id DeliveryID) (Delivery, error)
	GetByFulfillmentTarget(ctx context.Context, fID FulfillmentID, tID TargetID) (Delivery, error)
	ListByFulfillment(ctx context.Context, fID FulfillmentID) ([]Delivery, error)
	DeleteByFulfillment(ctx context.Context, fID FulfillmentID) error
}
