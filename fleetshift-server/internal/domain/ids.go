package domain

// TargetID uniquely identifies a target within the platform.
type TargetID string

// TargetType identifies the kind of target and determines which
// [DeliveryAgent] handles delivery. Built-in types include "kubernetes",
// "platform", and "local"; addons register additional types.
type TargetType string

// DeploymentID uniquely identifies a deployment.
type DeploymentID string

// DeliveryID uniquely identifies a delivery (one deployment-target pair).
type DeliveryID string

// InventoryItemID uniquely identifies an item in the inventory catalog.
type InventoryItemID string

// InventoryType classifies an [InventoryItem] and determines the schema
// of its Properties. Addons register inventory types (e.g.
// "docker.daemon", "kind.cluster", "kubernetes.node").
type InventoryType string
