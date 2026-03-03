package domain

import "context"

// DeliveryService is the port through which the orchestration pipeline
// delivers manifests to targets. The real implementation routes to
// per-target-type [DeliveryAgent] implementations; the initial
// implementation records deliveries in the database.
type DeliveryService interface {
	Deliver(ctx context.Context, target TargetInfo, deliveryID DeliveryID, manifests []Manifest, signaler *DeliverySignaler) (DeliveryResult, error)
	Remove(ctx context.Context, target TargetInfo, deliveryID DeliveryID, signaler *DeliverySignaler) error
}
