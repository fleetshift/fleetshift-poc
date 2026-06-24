package domain

import (
	"context"
	"encoding/json"
	"time"
)

// InventoryCondition captures a single status condition from an observed
// resource (e.g. Ready, Available, Progressing).
type InventoryCondition struct {
	Type               string
	Status             string
	Reason             string
	Message            string
	LastTransitionTime *time.Time
}

// InventoryEdge represents a relationship between two inventory items
// (e.g. Pod → ReplicaSet, Pod → Node).
type InventoryEdge struct {
	EdgeType   string
	SourceUID  string
	DestUID    string
	SourceKind string
	DestKind   string
}

// InventoryItem is an entry in the platform's universal catalog.
// Addons report inventory items with typed, addon-defined properties.
// Some inventory items are also targets (e.g. clusters); most are
// purely informational (e.g. nodes, namespaces, Helm releases).
//
// Construct new instances with [NewInventoryItem]; reconstitute from
// persistence with [InventoryItemFromSnapshot]. Read via accessor
// methods.
type InventoryItem struct {
	id               InventoryItemID
	inventoryType    InventoryType
	name             string
	properties       json.RawMessage
	labels           map[string]string
	sourceDeliveryID *DeliveryID
	createdAt        time.Time
	updatedAt        time.Time
	targetID         TargetID
	observed         json.RawMessage
	conditions       []InventoryCondition
	observedAt       *time.Time
}

// NewInventoryItem creates a brand-new [InventoryItem]. Use this on
// creation paths; use [InventoryItemFromSnapshot] only for
// reconstituting from persistence.
func NewInventoryItem(id InventoryItemID, invType InventoryType, name string, properties json.RawMessage, labels map[string]string, sourceDeliveryID *DeliveryID, now time.Time) InventoryItem {
	return InventoryItem{
		id:               id,
		inventoryType:    invType,
		name:             name,
		properties:       properties,
		labels:           labels,
		sourceDeliveryID: sourceDeliveryID,
		createdAt:        now,
		updatedAt:        now,
	}
}

// NewObservedInventoryItem creates a brand-new [InventoryItem] with
// observation fields. Use this for items reported by an index agent;
// use [NewInventoryItem] for items without observation fields;
// use [InventoryItemFromSnapshot] only for reconstituting from persistence.
func NewObservedInventoryItem(
	id InventoryItemID, invType InventoryType, name string,
	properties json.RawMessage, labels map[string]string,
	targetID TargetID, observed json.RawMessage,
	conditions []InventoryCondition, createdAt, observedAt time.Time,
) InventoryItem {
	return InventoryItem{
		id:            id,
		inventoryType: invType,
		name:          name,
		properties:    properties,
		labels:        labels,
		targetID:      targetID,
		observed:      observed,
		conditions:    conditions,
		observedAt:    &observedAt,
		createdAt:     createdAt,
		updatedAt:     observedAt,
	}
}

// ID returns the item's unique identifier.
func (i InventoryItem) ID() InventoryItemID { return i.id }

// Type returns the inventory type.
func (i InventoryItem) Type() InventoryType { return i.inventoryType }

// Name returns the item's human-readable name.
func (i InventoryItem) Name() string { return i.name }

// Properties returns the raw JSON properties.
func (i InventoryItem) Properties() json.RawMessage { return i.properties }

// Labels returns the item's label set.
func (i InventoryItem) Labels() map[string]string { return i.labels }

// SourceDeliveryID returns the delivery that produced this item, if any.
func (i InventoryItem) SourceDeliveryID() *DeliveryID { return i.sourceDeliveryID }

// CreatedAt returns the creation timestamp.
func (i InventoryItem) CreatedAt() time.Time { return i.createdAt }

// UpdatedAt returns the last-updated timestamp.
func (i InventoryItem) UpdatedAt() time.Time { return i.updatedAt }

// TargetID returns the target this item was observed on.
func (i InventoryItem) TargetID() TargetID { return i.targetID }

// Observed returns the raw JSON of extracted observation fields.
func (i InventoryItem) Observed() json.RawMessage { return i.observed }

// Conditions returns the status conditions from the observed resource.
func (i InventoryItem) Conditions() []InventoryCondition { return i.conditions }

// ObservedAt returns the timestamp when this observation was made.
func (i InventoryItem) ObservedAt() *time.Time { return i.observedAt }

// InventoryWriter is the addon's interface for writing observed
// inventory data to the platform. It models the addon-to-platform
// direction of the indexing protocol.
//
// In-process addons receive the application layer's implementation
// directly. Remote addons (via fleetlet) would receive a channel
// adapter implementing this same interface.
type InventoryWriter interface {
	// ApplyDelta upserts and deletes inventory items in a single
	// transaction. This is the incremental update path — the addon
	// sends only what changed since the last delta.
	ApplyDelta(ctx context.Context, targetID TargetID, upserts []InventoryItem, deletedIDs []InventoryItemID, edgeAdds []InventoryEdge, edgeDels []InventoryEdge) error

	// Resync atomically replaces all items for a target+type. This
	// is the full-sync path — used on initial list and after errors
	// to guarantee the platform's view matches the source of truth.
	// Edges are not affected — edge management is handled exclusively
	// by the incremental ApplyDelta path.
	Resync(ctx context.Context, targetID TargetID, inventoryType InventoryType, items []InventoryItem) error
}
