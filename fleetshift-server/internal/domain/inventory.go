package domain

import (
	"encoding/json"
	"time"
)

// InventoryItem is an entry in the platform's universal catalog.
// Addons report inventory items with typed, addon-defined properties.
// Some inventory items are also targets (e.g. clusters); most are
// purely informational (e.g. nodes, namespaces, Helm releases).
type InventoryItem struct {
	ID               InventoryItemID
	Type             InventoryType
	Name             string
	Properties       json.RawMessage
	Labels           map[string]string
	SourceDeliveryID *DeliveryID
	CreatedAt        time.Time
	UpdatedAt        time.Time
}
