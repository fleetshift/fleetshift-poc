package domain

import "context"

// DeliveryReporter is the addon's client interface for communicating
// delivery lifecycle updates back to the platform. It models the
// addon-to-platform direction of the delivery protocol and is the
// single channel for all delivery state transitions.
//
// All delivery outcomes — including the initial acceptance or
// immediate rejection — flow through this interface. This unifies the
// state machine: the platform never infers delivery state from the
// return value of [DeliveryAgent.Deliver].
//
// In-process addons receive the application layer's implementation
// directly. Remote addons (via fleetlet) would receive a gRPC client
// stub implementing this same interface.
type DeliveryReporter interface {
	// ReportEvent records a non-terminal delivery event (progress,
	// warning, error). On the first call for a delivery, the platform
	// transitions the delivery to [DeliveryStateProgressing].
	ReportEvent(ctx context.Context, deliveryID DeliveryID, event DeliveryEvent) error

	// ReportResult records a delivery state transition and, for
	// terminal states, signals the fulfillment workflow so
	// orchestration can proceed.
	ReportResult(ctx context.Context, deliveryID DeliveryID, result DeliveryResult) error

	// ListActiveDeliveries returns non-terminal deliveries enriched
	// with target info, caller auth, and (when signed) attestation.
	// Addons call this at startup to recover in-progress work after a
	// restart. Stale deliveries whose fulfillment generation has
	// advanced are excluded because their auth and attestation cannot
	// be correctly reconstructed.
	//
	// If targetIDs is non-empty, only deliveries destined for those
	// targets are returned; otherwise all active deliveries are
	// returned.
	ListActiveDeliveries(ctx context.Context, targetIDs []TargetID) ([]ActiveDelivery, error)
}
