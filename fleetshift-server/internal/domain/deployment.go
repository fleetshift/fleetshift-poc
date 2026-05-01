package domain

import "time"

// Deployment is the thin user-facing aggregate. It holds
// deployment-specific identity and a reference to the [Fulfillment]
// that owns the orchestration state.
type Deployment struct {
	ID            DeploymentID
	UID           string
	FulfillmentID FulfillmentID
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Etag          string
}

// DeploymentView is the read model that joins a [Deployment] with its
// [Fulfillment]. Constructed by the repository via joins; never
// written directly. The transport layer maps this to the proto
// Deployment message.
type DeploymentView struct {
	Deployment  Deployment
	Fulfillment Fulfillment
}
