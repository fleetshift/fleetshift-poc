package domain

import "time"

// Deployment is the thin user-facing aggregate. It holds
// deployment-specific identity and a reference to the [Fulfillment]
// that owns the orchestration state.
//
// Construct new instances with [NewDeployment]; reconstitute from
// persistence with [DeploymentFromSnapshot]. Read via accessor methods.
type Deployment struct {
	id            DeploymentID
	uid           string
	fulfillmentID FulfillmentID
	createdAt     time.Time
	updatedAt     time.Time
}

// NewDeployment creates a brand-new [Deployment]. Use this on creation
// paths; use [DeploymentFromSnapshot] only for reconstituting from
// persistence.
func NewDeployment(id DeploymentID, uid string, fulfillmentID FulfillmentID, now time.Time) Deployment {
	return Deployment{
		id:            id,
		uid:           uid,
		fulfillmentID: fulfillmentID,
		createdAt:     now,
		updatedAt:     now,
	}
}

// ID returns the deployment's unique identifier.
func (d Deployment) ID() DeploymentID { return d.id }

// UID returns the deployment's external UID.
func (d Deployment) UID() string { return d.uid }

// FulfillmentID returns the linked fulfillment's identifier.
func (d Deployment) FulfillmentID() FulfillmentID { return d.fulfillmentID }

// CreatedAt returns the creation timestamp.
func (d Deployment) CreatedAt() time.Time { return d.createdAt }

// UpdatedAt returns the last-updated timestamp.
func (d Deployment) UpdatedAt() time.Time { return d.updatedAt }

// DeploymentView is the read model that joins a [Deployment] with its
// [Fulfillment]. Constructed by the repository via joins; never
// written directly. The transport layer maps this to the proto
// Deployment message.
type DeploymentView struct {
	Deployment  Deployment
	Fulfillment Fulfillment
}
