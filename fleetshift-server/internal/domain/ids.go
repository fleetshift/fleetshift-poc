package domain

import "fmt"

// TargetID uniquely identifies a target within the platform.
type TargetID string

// NewTargetID validates and returns a [TargetID]. It rejects empty values.
func NewTargetID(s string) (TargetID, error) {
	if s == "" {
		return "", fmt.Errorf("target id: %w: must not be empty", ErrInvalidArgument)
	}
	return TargetID(s), nil
}

// TargetType identifies the kind of target and determines which
// [DeliveryAgent] handles delivery. Built-in types include "kubernetes",
// "platform", and "local"; addons register additional types.
type TargetType string

// Generation is a monotonically increasing counter on a [Fulfillment].
// It is bumped on every mutation (create, invalidation, resume, delete)
// and compared against [ObservedGeneration] to detect pending work.
type Generation int64

// Etag is a weak domain-state concurrency token (RFC 9110 Section
// 8.8.1, AIP-154). It is opaque, W/-prefixed, and changes whenever
// any API-visible state changes.
type Etag string

// DeliveryID uniquely identifies a delivery (one deployment-target pair).
type DeliveryID string

// InventoryItemID uniquely identifies an item in the inventory catalog.
type InventoryItemID string

// InventoryType classifies an [InventoryItem] and determines the schema
// of its Properties. Addons register inventory types (e.g.
// "docker.daemon", "kind.cluster", "kubernetes.node").
type InventoryType string
