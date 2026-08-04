package domain

import (
	"context"
	"encoding/json"
	"time"
)

// InventoryReporter is the addon's client interface for reporting
// inventory state back to the platform. It models the addon-to-platform
// direction of inventory reporting and is the single channel addons use
// for ApplyDeltaBatch-style updates.
//
// In-process addons receive the application layer's thin adapter
// directly. Remote addons (via fleetlet) would receive a gRPC client
// stub implementing this same interface.
type InventoryReporter interface {
	// ApplyDeltaBatch applies a batch of field-level inventory deltas
	// in one round trip through the platform's inventory write path.
	ApplyDeltaBatch(ctx context.Context, batch InventoryDeltaBatch) error
}

// InventoryDeltaBatch is a batch of addon-facing inventory delta
// reports. Callers should coalesce related updates into one batch when
// possible.
type InventoryDeltaBatch struct {
	Reports []InventoryDeltaReport
}

// InventoryDeltaReport describes incremental or full-replace changes
// to a single extension resource's inventory state, identified by
// resource type and name. Semantics match [InventoryDelta]: nil
// ReplaceLabels/ReplaceConditions mean unchanged; non-nil (including
// empty) replaces the entire map/set. Labels and conditions share the
// Upsert/Delete/Replace shape; Replace* is mutually exclusive with the
// corresponding incremental ops.
type InventoryDeltaReport struct {
	ResourceType ResourceType
	Name         ResourceName

	UpsertAliases  AliasSet
	DeleteAliases  []AliasRef
	ReplaceAliases AliasSet

	ReplaceLabels map[string]string
	UpsertLabels  map[string]string
	DeleteLabels  []string

	Observation *json.RawMessage

	ReplaceConditions []Condition
	UpsertConditions  []Condition
	DeleteConditions  []ConditionType

	ObservedAt time.Time
}
