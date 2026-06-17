package domain

import (
	"context"
)

// TargetRepository persists and retrieves target metadata.
type TargetRepository interface {
	Create(ctx context.Context, target TargetInfo) error
	CreateOrUpdate(ctx context.Context, target TargetInfo) error
	Get(ctx context.Context, id TargetID) (TargetInfo, error)
	List(ctx context.Context) ([]TargetInfo, error)
	Delete(ctx context.Context, id TargetID) error
}

// FulfillmentRepository persists and retrieves fulfillments.
// Create and Update read pending strategy records from [Fulfillment.Snapshot]
// and flush them to storage, then call [Fulfillment.DrainPendingStrategyRecords]
// to clear the buffers. Get materializes current strategy specs by joining
// the version tables.
type FulfillmentRepository interface {
	Create(ctx context.Context, f *Fulfillment) error
	Get(ctx context.Context, id FulfillmentID) (*Fulfillment, error)
	Update(ctx context.Context, f *Fulfillment) error
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
	ListActive(ctx context.Context, targetIDs []TargetID) ([]Delivery, error)
	DeleteByFulfillment(ctx context.Context, fID FulfillmentID) error
}

// ManagedResourceRepository persists managed resource types, versioned
// intents, and instance HEAD records. Grouped into a single repository
// because these three tables form a cohesive aggregate boundary.
//
// Intent versioning is owned by the [ManagedResource] aggregate.
// CreateInstance (and future UpdateInstance) read pending intents from
// [ManagedResource.Snapshot] and flush them to storage, then call
// [ManagedResource.DrainPendingIntents] to clear the buffer.
type ManagedResourceRepository interface {
	// Type registration
	CreateType(ctx context.Context, def ManagedResourceTypeDef) error
	GetType(ctx context.Context, rt ResourceType) (ManagedResourceTypeDef, error)
	ListTypes(ctx context.Context) ([]ManagedResourceTypeDef, error)
	DeleteType(ctx context.Context, rt ResourceType) error

	// Versioned intent (read-only; writes go through the aggregate drain)
	GetIntent(ctx context.Context, rt ResourceType, name ResourceName, version IntentVersion) (ResourceIntent, error)
	// Hard-delete all intent versions for a managed resource instance.
	// Used by managed-resource cleanup after delivery-side deletion completes.
	DeleteIntents(ctx context.Context, rt ResourceType, name ResourceName) error

	// Instance aggregate (Create drains pending intents)
	CreateInstance(ctx context.Context, mr *ManagedResource) error
	GetInstance(ctx context.Context, rt ResourceType, name ResourceName) (*ManagedResource, error)
	GetView(ctx context.Context, rt ResourceType, name ResourceName) (ManagedResourceView, error)
	ListViewsByType(ctx context.Context, rt ResourceType) ([]ManagedResourceView, error)
	DeleteInstance(ctx context.Context, rt ResourceType, name ResourceName) error
}

// ResourceIdentityRepository persists and retrieves canonical platform
// resource identities. The [PlatformResource] aggregate owns its child
// entities (representations, aliases, relationships); the repository
// reconciles the full aggregate state on Create/Update.
type ResourceIdentityRepository interface {
	Create(ctx context.Context, r *PlatformResource) error
	Get(ctx context.Context, uid PlatformResourceUID) (*PlatformResource, error)
	GetByName(ctx context.Context, name RelativeResourceName) (*PlatformResource, error)
	Update(ctx context.Context, r *PlatformResource) error
	ListByCollection(ctx context.Context, collection CollectionID) ([]*PlatformResource, error)

	// Cross-resource lookups (can't live on the aggregate).
	ResolveAlias(ctx context.Context, alias Alias) (PlatformResourceUID, error)
	GetRepresentation(ctx context.Context, name FullResourceName) (ResourceRepresentation, error)
}
