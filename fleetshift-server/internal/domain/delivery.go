package domain

import (
	"encoding/json"
	"time"
)

// DeliveryAuth carries the caller's credentials into a delivery. Agents
// use this to act on behalf of the caller (e.g., bootstrapping RBAC for
// the user who created a cluster).
type DeliveryAuth struct {
	Caller   *SubjectClaims // identity of the user who initiated the delivery
	Audience []Audience     // token audience; used to derive target OIDC client ID
	Token    RawToken       // verified JWT; agents use for passthrough to target APIs
}

// DeliveryState indicates where a delivery is in its lifecycle.
type DeliveryState string

const (
	DeliveryStatePending     DeliveryState = "pending"
	DeliveryStateAccepted    DeliveryState = "accepted"
	DeliveryStateProgressing DeliveryState = "progressing"
	DeliveryStateDelivered   DeliveryState = "delivered"
	DeliveryStateFailed      DeliveryState = "failed"
	DeliveryStatePartial     DeliveryState = "partial"
	DeliveryStateAuthFailed  DeliveryState = "auth_failed"
)

// Delivery is a first-class entity capturing a single
// deployment-to-target delivery and its lifecycle.
type Delivery struct {
	ID           DeliveryID
	DeploymentID DeploymentID
	TargetID     TargetID
	Manifests    []Manifest
	State        DeliveryState
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// DeliveryResult is the outcome of a single delivery attempt.
// Agents may include structured outputs (provisioned targets, produced
// secrets) that the platform processes after delivery completion.
type DeliveryResult struct {
	State              DeliveryState
	Message            string
	ProvisionedTargets []ProvisionedTarget
	ProducedSecrets    []ProducedSecret
}

// ProvisionedTarget declares a target that a delivery created and that
// the platform should register. Properties should include vault refs
// for any associated secrets (e.g. "kubeconfig_ref").
type ProvisionedTarget struct {
	ID                    TargetID
	Type                  TargetType
	Name                  string
	Labels                map[string]string
	Properties            map[string]string
	AcceptedResourceTypes []ResourceType
}

// ProducedSecret declares a secret that a delivery produced and that
// the platform should store in the [Vault].
type ProducedSecret struct {
	Ref   SecretRef
	Value []byte
}

// DeliveryEventKind classifies a [DeliveryEvent].
type DeliveryEventKind string

const (
	DeliveryEventProgress DeliveryEventKind = "progress"
	DeliveryEventWarning  DeliveryEventKind = "warning"
	DeliveryEventError    DeliveryEventKind = "error"
)

// DeliveryEvent is a single entry in a delivery's event log.
type DeliveryEvent struct {
	Timestamp time.Time
	Kind      DeliveryEventKind
	Message   string
	Detail    json.RawMessage
}

