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
	Get(ctx context.Context, name ResourceName) (Deployment, error)
	GetView(ctx context.Context, name ResourceName) (DeploymentView, error)
	ListView(ctx context.Context) ([]DeploymentView, error)
	Delete(ctx context.Context, name ResourceName) error
}

// InventoryRepository persists and retrieves inventory items.
type InventoryRepository interface {
	Create(ctx context.Context, item InventoryItem) error
	CreateOrUpdate(ctx context.Context, item InventoryItem) error
	Get(ctx context.Context, id InventoryItemID) (InventoryItem, error)
	List(ctx context.Context) ([]InventoryItem, error)
	ListByType(ctx context.Context, t InventoryItemType) ([]InventoryItem, error)
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

// ExtensionResourceRepository persists extension resource types,
// versioned intents, instance records, and managed state. Grouped into
// a single repository because these tables form a cohesive aggregate
// boundary for the extension resource model.
//
// Intent versioning is owned by the [ExtensionResource] aggregate (via
// [ManagedState]). Create reads pending intents from the aggregate's
// [ExtensionResource.Snapshot] and flushes them to storage. The
// aggregate is only valid within the scope of a single transaction; on
// the next read, [ExtensionResourceFromSnapshot] naturally produces an
// aggregate with no pending intents.
type ExtensionResourceRepository interface {
	// Type registration
	CreateType(ctx context.Context, def ExtensionResourceType) error
	GetType(ctx context.Context, rt ResourceType) (ExtensionResourceType, error)
	ListTypes(ctx context.Context) ([]ExtensionResourceType, error)
	DeleteType(ctx context.Context, rt ResourceType) error

	// Instance aggregate
	Create(ctx context.Context, r *ExtensionResource) error
	Get(ctx context.Context, rt ResourceType, name ResourceName) (*ExtensionResource, error)
	GetByUID(ctx context.Context, uid ExtensionResourceUID) (*ExtensionResource, error)
	ListByResourceType(ctx context.Context, rt ResourceType) ([]*ExtensionResource, error)
	Delete(ctx context.Context, rt ResourceType, name ResourceName) error

	// Read views (join extension resource + managed state + intent + fulfillment)
	GetView(ctx context.Context, rt ResourceType, name ResourceName) (ExtensionResourceView, error)
	ListViewsByType(ctx context.Context, rt ResourceType) ([]ExtensionResourceView, error)

	// Versioned intent (read-only; writes go through the aggregate drain)
	GetIntent(ctx context.Context, rt ResourceType, name ResourceName, version IntentVersion) (ResourceIntent, error)
	// Hard-delete all intent versions for an extension resource instance.
	// Used by managed-resource cleanup after delivery-side deletion completes.
	DeleteIntents(ctx context.Context, rt ResourceType, name ResourceName) error
}

// ResourceIdentityRepository persists and retrieves canonical platform
// resource identities. The [PlatformResource] aggregate owns its child
// entities (representations, aliases, relationships); the repository
// reconciles the full aggregate state on Create/Update.
type ResourceIdentityRepository interface {
	Create(ctx context.Context, r *PlatformResource) error
	Get(ctx context.Context, uid PlatformResourceUID) (*PlatformResource, error)
	GetByName(ctx context.Context, name ResourceName) (*PlatformResource, error)
	Update(ctx context.Context, r *PlatformResource) error
	ListByCollection(ctx context.Context, collection CollectionName) ([]*PlatformResource, error)

	// Cross-resource lookups (can't live on the aggregate).
	ResolveAlias(ctx context.Context, alias Alias) (PlatformResourceUID, error)
	GetRepresentation(ctx context.Context, name FullResourceName) (ResourceRepresentation, error)
}
