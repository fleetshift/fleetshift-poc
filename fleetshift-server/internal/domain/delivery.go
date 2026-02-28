package domain

import "time"

// DeliveryState indicates the overall delivery outcome for a single
// deployment-target pair.
type DeliveryState string

const (
	DeliveryStatePending  DeliveryState = "pending"
	DeliveryStateDelivered DeliveryState = "delivered"
	DeliveryStateFailed   DeliveryState = "failed"
)

// DeliveryRecord captures what was delivered to a target and its status.
type DeliveryRecord struct {
	DeploymentID DeploymentID
	TargetID     TargetID
	Manifests    []Manifest
	State        DeliveryState
	UpdatedAt    time.Time
}

// DeliveryResult is the outcome of a single delivery attempt.
type DeliveryResult struct {
	State DeliveryState
}
