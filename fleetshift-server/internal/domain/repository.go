package domain

import (
	"context"
	"encoding/json"
	"time"
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
	Get(ctx context.Context, name FullResourceName) (*ExtensionResource, error)
	GetByUID(ctx context.Context, uid ExtensionResourceUID) (*ExtensionResource, error)
	ListByResourceType(ctx context.Context, rt ResourceType) ([]*ExtensionResource, error)
	Delete(ctx context.Context, name FullResourceName) error

	// Read views (join extension resource + managed state + intent + fulfillment + inventory)
	GetView(ctx context.Context, name FullResourceName) (ExtensionResourceView, error)
	ListViewsByType(ctx context.Context, rt ResourceType) ([]ExtensionResourceView, error)

	// Versioned intent (read-only; writes go through the aggregate drain).
	// Intents are owned by their extension resource; ON DELETE CASCADE
	// handles cleanup when the parent is deleted.
	GetIntent(ctx context.Context, uid ExtensionResourceUID, version IntentVersion) (ResourceIntent, error)

	// Inventory mutations -- UID-addressed, narrow command methods (not
	// a general Save). Both require the extension_resources row to
	// already exist; callers resolve identity and create the resource
	// at the application-service layer before calling these.
	//
	// ReplaceInventory treats each [InventoryReplacement] as the
	// complete latest inventory state for its resource: fields absent
	// from the replacement are cleared/deleted from latest state, with
	// the exception of Observation -- see its field doc.
	ReplaceInventory(ctx context.Context, replacements []InventoryReplacement) error

	// ApplyInventoryDeltas applies incremental, field-level changes:
	// fields absent from an [InventoryDelta] are left unchanged.
	ApplyInventoryDeltas(ctx context.Context, deltas []InventoryDelta) error

	// Observation history (append-only; populated as a side effect of
	// ReplaceInventory/ApplyInventoryDeltas, never written directly).
	ListObservations(ctx context.Context, uid ExtensionResourceUID, limit int) ([]Observation, error)

	// Condition transition history (append-only; populated as a side
	// effect of ReplaceInventory/ApplyInventoryDeltas when a supplied
	// [Condition] represents a genuine state change).
	ListConditionTransitions(ctx context.Context, uid ExtensionResourceUID, conditionType *ConditionType, limit int) ([]ConditionTransition, error)
}

// InventoryReplacement is a command DTO -- not a domain object --
// describing the complete latest inventory state for a single
// extension resource. See [ExtensionResourceRepository.ReplaceInventory].
//
// Labels is the complete observed label set; nil and empty both
// normalize to an empty latest label set. Conditions is the complete
// current condition set -- conditions absent from the replacement are
// deleted from latest state (without a transition row in this pass).
//
// Observation is the one field that does not follow the
// "absence = deletion" rule that governs Labels/Conditions above: a
// nil Observation, or a non-nil Observation pointing to the JSON
// literal null, leaves the latest observation untouched and appends
// no history row -- there is no "clear the observation" operation.
// Any other non-nil value replaces the latest observation and appends
// a history row, unless it's identical to the current latest
// observation (repositories dedup to avoid noisy history on repeat
// resyncs).
type InventoryReplacement struct {
	ExtensionResourceUID ExtensionResourceUID
	Labels               map[string]string
	Observation          *json.RawMessage
	Conditions           []Condition
	ObservedAt           time.Time
	ReceivedAt           time.Time
}

// InventoryDelta is a command DTO -- not a domain object -- describing
// incremental, field-level changes to a single extension resource's
// inventory state. See [ExtensionResourceRepository.ApplyInventoryDeltas].
//
// Fields left at their zero value are unchanged: SetLabels/DeleteLabels
// only touch the named keys, and UpsertConditions/DeleteConditions only
// touch the named condition types. A delta with no field-level changes
// is a valid heartbeat that still bumps resource-level freshness.
//
// Observation follows the same pointer semantics as
// [InventoryReplacement.Observation]: nil, or non-nil pointing to the
// JSON literal null, leaves the latest observation untouched and
// appends no history row; any other non-nil value replaces latest and
// appends a history row (subject to the same dedup rule).
type InventoryDelta struct {
	ExtensionResourceUID ExtensionResourceUID

	SetLabels    map[string]string
	DeleteLabels []string

	Observation *json.RawMessage

	UpsertConditions []Condition
	DeleteConditions []ConditionType

	ObservedAt time.Time
	ReceivedAt time.Time
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
