package domain

import (
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// DeploymentUID is the opaque, stable identifier for a deployment
// instance. Generated once at creation time and never changes. The
// underlying type is [uuid.UUID] so structural validity is encoded
// in the type system.
type DeploymentUID uuid.UUID

// NewDeploymentUID generates a new random [DeploymentUID].
func NewDeploymentUID() DeploymentUID {
	return DeploymentUID(uuid.New())
}

// ParseDeploymentUID parses a string into a [DeploymentUID].
func ParseDeploymentUID(s string) (DeploymentUID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return DeploymentUID{}, fmt.Errorf("deployment uid: %w", err)
	}
	return DeploymentUID(u), nil
}

// String returns the canonical UUID string representation.
func (u DeploymentUID) String() string { return uuid.UUID(u).String() }

// MarshalText implements [encoding.TextMarshaler] for JSON string encoding.
func (u DeploymentUID) MarshalText() ([]byte, error) { return uuid.UUID(u).MarshalText() }

// UnmarshalText implements [encoding.TextUnmarshaler] for JSON string decoding.
func (u *DeploymentUID) UnmarshalText(data []byte) error {
	return (*uuid.UUID)(u).UnmarshalText(data)
}

// Value implements [driver.Valuer] for SQL persistence.
func (u DeploymentUID) Value() (driver.Value, error) { return uuid.UUID(u).String(), nil }

// Scan implements [sql.Scanner] for SQL hydration.
func (u *DeploymentUID) Scan(src any) error { return (*uuid.UUID)(u).Scan(src) }

// IsZero returns true when the UID is the zero (nil) UUID.
func (u DeploymentUID) IsZero() bool { return uuid.UUID(u) == uuid.Nil }

// Deployment is the thin user-facing aggregate. It holds
// deployment-specific identity and a reference to the [Fulfillment]
// that owns the orchestration state.
//
// Construct new instances with [NewDeployment]; reconstitute from
// persistence with [DeploymentFromSnapshot]. Read via accessor methods.
type Deployment struct {
	name          ResourceName
	uid           DeploymentUID
	fulfillmentID FulfillmentID
	createdAt     time.Time
	updatedAt     time.Time
}

// NewDeployment creates a brand-new [Deployment]. Use this on creation
// paths; use [DeploymentFromSnapshot] only for reconstituting from
// persistence.
func NewDeployment(name ResourceName, uid DeploymentUID, fulfillmentID FulfillmentID, now time.Time) Deployment {
	return Deployment{
		name:          name,
		uid:           uid,
		fulfillmentID: fulfillmentID,
		createdAt:     now,
		updatedAt:     now,
	}
}

// Name returns the collection-qualified resource name
// (e.g. "deployments/my-deploy").
func (d Deployment) Name() ResourceName { return d.name }

// UID returns the deployment's external UID.
func (d Deployment) UID() DeploymentUID { return d.uid }

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
