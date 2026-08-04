package domain

import (
	"encoding/json"
	"time"
)

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
	inventoryType    InventoryItemType
	name             string
	properties       json.RawMessage
	labels           map[string]string
	sourceDeliveryID *DeliveryID
	createdAt        time.Time
	updatedAt        time.Time
}

// NewInventoryItem creates a brand-new [InventoryItem]. Use this on
// creation paths; use [InventoryItemFromSnapshot] only for
// reconstituting from persistence.
func NewInventoryItem(id InventoryItemID, invType InventoryItemType, name string, properties json.RawMessage, labels map[string]string, sourceDeliveryID *DeliveryID, now time.Time) InventoryItem {
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

// ID returns the item's unique identifier.
func (i InventoryItem) ID() InventoryItemID { return i.id }

// Type returns the inventory item type.
func (i InventoryItem) Type() InventoryItemType { return i.inventoryType }

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
