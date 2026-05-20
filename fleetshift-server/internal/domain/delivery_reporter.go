package domain

import "context"

// DeliveryReporter is the addon's client interface for communicating
// delivery lifecycle updates back to the platform. It models the
// addon-to-platform direction of the delivery protocol.
//
// In-process addons receive the application layer's implementation
// directly. Remote addons (via fleetlet) would receive a gRPC client
// stub implementing this same interface.
type DeliveryReporter interface {
	// ReportEvent records a non-terminal delivery event (progress,
	// warning, error). On the first call for a delivery, the platform
	// transitions the delivery to [DeliveryStateProgressing].
	ReportEvent(ctx context.Context, deliveryID DeliveryID, event DeliveryEvent) error

	// ReportResult records the terminal outcome of a delivery,
	// updates the delivery's state, and signals the fulfillment
	// workflow so orchestration can proceed.
	ReportResult(ctx context.Context, deliveryID DeliveryID, result DeliveryResult) error

	// ListActiveDeliveries returns deliveries that have been started
	// but are not yet in a terminal state. Addons call this at
	// startup to recover in-progress work after a restart.
	//
	// If targetIDs is non-empty, only deliveries destined for those
	// targets are returned; otherwise all active deliveries are
	// returned.
	ListActiveDeliveries(ctx context.Context, targetIDs []TargetID) ([]Delivery, error)
}
