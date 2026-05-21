package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

// DeliveryAuth carries the caller's passthrough credentials into a
// delivery. Agents use this to act on behalf of the caller (e.g.,
// bootstrapping RBAC for the user who created a cluster).
//
// Cryptographic provenance is stored on [Deployment.Provenance], not
// here. When provenance is present the orchestration assembles a
// self-contained [Attestation] and passes it to the delivery agent
// separately.
type DeliveryAuth struct {
	Caller   *SubjectClaims // identity of the user who initiated the delivery
	Audience []Audience     // token audience; used to derive target OIDC client ID
	// TODO: Never store this; should eventually refer to in memory or pause if unavailable
	Token RawToken // verified JWT; agents use for passthrough to target APIs
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

// IsTerminal reports whether the state represents a completed delivery
// that should not transition further.
func (s DeliveryState) IsTerminal() bool {
	switch s {
	case DeliveryStateDelivered, DeliveryStateFailed,
		DeliveryStatePartial, DeliveryStateAuthFailed:
		return true
	default:
		return false
	}
}

// deliveryStateOrder defines the forward-only ordering of non-terminal
// delivery states. Terminal states are not ordered relative to each
// other; they are reachable from any non-terminal state.
var deliveryStateOrder = map[DeliveryState]int{
	DeliveryStatePending:     0,
	DeliveryStateAccepted:    1,
	DeliveryStateProgressing: 2,
}

// knownDeliveryStates is the complete set of valid delivery states.
// Used by [Delivery.TransitionTo] to reject unknown values.
var knownDeliveryStates = map[DeliveryState]struct{}{
	DeliveryStatePending:     {},
	DeliveryStateAccepted:    {},
	DeliveryStateProgressing: {},
	DeliveryStateDelivered:   {},
	DeliveryStateFailed:      {},
	DeliveryStatePartial:     {},
	DeliveryStateAuthFailed:  {},
}

// Delivery is a first-class entity capturing a single
// fulfillment-to-target delivery and its lifecycle.
type Delivery struct {
	ID            DeliveryID
	FulfillmentID FulfillmentID
	TargetID      TargetID
	Manifests     []Manifest
	Generation    Generation // fulfillment generation at dispatch; used for stale-delivery fencing
	State         DeliveryState
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// TransitionTo moves the delivery to the given state if the transition
// is legal. It returns [ErrIllegalStateTransition] if the current state
// does not permit the requested transition (e.g. from a terminal
// state, or backward along the lifecycle). A same-state transition is
// a no-op (no error, no timestamp update).
func (d *Delivery) TransitionTo(state DeliveryState, now time.Time) error {
	if _, ok := knownDeliveryStates[state]; !ok {
		return fmt.Errorf("%w: unknown delivery state %q", ErrIllegalStateTransition, state)
	}
	if d.State == state {
		return nil
	}
	if d.State.IsTerminal() {
		return fmt.Errorf(
			"%w: delivery %s is in terminal state %q, cannot transition to %q",
			ErrIllegalStateTransition, d.ID, d.State, state)
	}
	if !state.IsTerminal() {
		fromOrd, fromOk := deliveryStateOrder[d.State]
		toOrd, toOk := deliveryStateOrder[state]
		if fromOk && toOk && toOrd <= fromOrd {
			return fmt.Errorf(
				"%w: delivery %s cannot move backward from %q to %q",
				ErrIllegalStateTransition, d.ID, d.State, state)
		}
	}
	d.State = state
	d.UpdatedAt = now
	return nil
}

// ActiveDelivery is the enriched view of a [Delivery] returned by
// [DeliveryReporter.ListActiveDeliveries]. It bundles the delivery
// record with the full context an addon needs to resume work after a
// restart: target connection info, caller auth, and (when the
// fulfillment was signed) the re-assembled attestation.
//
// Stale deliveries — where the fulfillment's generation has advanced
// past the delivery's — are excluded because their auth and
// attestation cannot be correctly reconstructed from the current
// fulfillment state.
//
// TODO: A Pending delivery returned here may also arrive via
// [DeliveryAgent.Deliver] if the addon starts up while a
// DeliverToTarget activity is in flight. Addons must deduplicate
// by DeliveryID across both paths. See OME-77.
type ActiveDelivery struct {
	Delivery    Delivery
	Target      TargetInfo
	Auth        DeliveryAuth
	Attestation *Attestation // nil for unsigned (token-passthrough) fulfillments
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
