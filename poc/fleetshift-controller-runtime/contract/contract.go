// Package contract is a FleetShift-shaped delivery protocol surface copied
// for this POC. It intentionally does not import fleetshift-server/internal
// packages (same constraint as poc/ocm-work-agent-adapter).
//
// The shapes mirror fleetshift-server/internal/domain delivery agent and
// reporter interfaces so a real addon could swap this package for the
// platform types with only import-path changes.
package contract

import (
	"context"
	"encoding/json"
	"time"
)

// Tiny types for identifiers that would otherwise be bare strings.
type (
	DeliveryID    string
	TargetID      string
	TargetType    string
	FulfillmentID string
	ManifestType  string
	Generation    int64
	RawToken      string
)

// DeliveryAuth carries the caller's passthrough credentials into a delivery.
type DeliveryAuth struct {
	Token RawToken
}

// Attestation is an opaque stand-in for the signed delivery attestation.
// The POC does not verify it; real addons would.
type Attestation struct {
	Raw json.RawMessage
}

// Manifest is an opaque payload delivered to a target.
type Manifest struct {
	ManifestType ManifestType
	Raw          json.RawMessage
}

// TargetInfo describes a delivery target. Properties are addon-defined.
type TargetInfo struct {
	ID         TargetID
	Type       TargetType
	Name       string
	Properties map[string]string
}

// DeliveryState is the delivery lifecycle state.
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

// IsTerminal reports whether the state is terminal.
func (s DeliveryState) IsTerminal() bool {
	switch s {
	case DeliveryStateDelivered, DeliveryStateFailed,
		DeliveryStatePartial, DeliveryStateAuthFailed:
		return true
	default:
		return false
	}
}

// DeliveryOperation is deliver or remove.
type DeliveryOperation string

const (
	DeliveryOperationDeliver DeliveryOperation = "deliver"
	DeliveryOperationRemove  DeliveryOperation = "remove"
)

// DeliveryEventKind classifies a DeliveryEvent.
type DeliveryEventKind string

const (
	DeliveryEventProgress DeliveryEventKind = "progress"
	DeliveryEventWarning  DeliveryEventKind = "warning"
	DeliveryEventError    DeliveryEventKind = "error"
)

// DeliveryEvent is a non-terminal progress/warning/error entry.
type DeliveryEvent struct {
	Timestamp time.Time
	Kind      DeliveryEventKind
	Message   string
	Detail    json.RawMessage
}

// ProvisionedTarget declares a target created by a delivery.
type ProvisionedTarget struct {
	ID         TargetID
	Type       TargetType
	Name       string
	Properties map[string]string
}

// ProducedSecret declares a secret produced by a delivery.
type ProducedSecret struct {
	Ref   string
	Value []byte
}

// DeliveryResult is a terminal (or state-transition) outcome.
type DeliveryResult struct {
	State              DeliveryState
	Message            string
	ProvisionedTargets []ProvisionedTarget
	ProducedSecrets    []ProducedSecret
}

// ActiveDelivery is the enriched view returned by ListActiveDeliveries.
type ActiveDelivery struct {
	DeliveryID  DeliveryID
	Fulfillment FulfillmentID
	Target      TargetInfo
	Manifests   []Manifest
	Generation  Generation
	Operation   DeliveryOperation
	State       DeliveryState
	Auth        DeliveryAuth
	Attestation *Attestation
}

// DeliveryAgent is the platform → addon direction of the delivery contract.
// Errors are for dispatch/infrastructure failures only; outcomes flow through
// DeliveryReporter.
type DeliveryAgent interface {
	Deliver(ctx context.Context, target TargetInfo, deliveryID DeliveryID, manifests []Manifest, auth DeliveryAuth, attestation *Attestation, generation Generation) error
	Remove(ctx context.Context, target TargetInfo, deliveryID DeliveryID, manifests []Manifest, auth DeliveryAuth, attestation *Attestation, generation Generation) error
}

// DeliveryReporter is the addon → platform direction of the delivery contract.
type DeliveryReporter interface {
	ReportEvent(ctx context.Context, deliveryID DeliveryID, generation Generation, event DeliveryEvent) error
	ReportResult(ctx context.Context, deliveryID DeliveryID, generation Generation, result DeliveryResult) error
	ListActiveDeliveries(ctx context.Context, targetIDs []TargetID) ([]ActiveDelivery, error)
}
