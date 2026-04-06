package domain

import "context"

// DeliveryService is the port through which the orchestration pipeline
// delivers manifests to targets. The real implementation routes to
// per-target-type [DeliveryAgent] implementations; the initial
// implementation records deliveries in the database.
//
// Deliver must return [DeliveryStateAccepted] immediately and perform
// the actual work asynchronously. Once the work completes (successfully
// or not), the agent calls [DeliverySignaler.Done] from a goroutine —
// never synchronously inside Deliver. This guarantees that the workflow
// signal sent by Done runs outside the activity, avoiding deadlocks in
// durable engines that hold locks during activity execution.
type DeliveryService interface {
	Deliver(ctx context.Context, target TargetInfo, deliveryID DeliveryID, manifests []Manifest, auth DeliveryAuth, attestation *Attestation, signaler *DeliverySignaler) (DeliveryResult, error)
	Remove(ctx context.Context, target TargetInfo, deliveryID DeliveryID, signaler *DeliverySignaler) error
}
