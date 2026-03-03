package domain

import (
	"encoding/json"
	"time"
)

// DeliveryState indicates where a delivery is in its lifecycle.
type DeliveryState string

const (
	DeliveryStatePending     DeliveryState = "pending"
	DeliveryStateAccepted    DeliveryState = "accepted"
	DeliveryStateProgressing DeliveryState = "progressing"
	DeliveryStateDelivered   DeliveryState = "delivered"
	DeliveryStateFailed      DeliveryState = "failed"
	DeliveryStatePartial     DeliveryState = "partial"
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
type DeliveryResult struct {
	State   DeliveryState
	Message string
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

// DeliveryObserver receives events emitted by a [DeliveryAgent]
// during delivery. Agents call [Emit] for progress, warnings, and
// errors as they happen, then call [Done] exactly once when the
// delivery reaches a terminal state. The orchestration layer provides
// an implementation that updates delivery state and signals the
// workflow; agents are unaware of storage or signaling.
type DeliveryObserver interface {
	Emit(event DeliveryEvent)
	Done(result DeliveryResult)
}

// NopDeliveryObserver is a [DeliveryObserver] that discards all events.
type NopDeliveryObserver struct{}

func (NopDeliveryObserver) Emit(DeliveryEvent)    {}
func (NopDeliveryObserver) Done(DeliveryResult) {}
