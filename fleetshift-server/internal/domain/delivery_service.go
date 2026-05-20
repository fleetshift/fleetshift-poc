package domain

import "context"

// DeliveryService is the port through which the orchestration pipeline
// delivers manifests to targets. The real implementation routes to
// per-target-type [DeliveryAgent] implementations.
//
// Deliver dispatches the delivery and returns immediately. An error
// return means the delivery was never started (e.g. no agent
// registered for the target type). All delivery outcomes — accepted,
// rejected, failed, delivered — are reported asynchronously through
// the agent's [DeliveryReporter]. This guarantees that workflow
// signals run outside the activity, avoiding deadlocks in durable
// engines that hold locks during activity execution.
type DeliveryService interface {
	Deliver(ctx context.Context, target TargetInfo, deliveryID DeliveryID, manifests []Manifest, auth DeliveryAuth, attestation *Attestation, generation Generation) error
	Remove(ctx context.Context, target TargetInfo, deliveryID DeliveryID, manifests []Manifest, auth DeliveryAuth, attestation *Attestation, generation Generation) error
}
