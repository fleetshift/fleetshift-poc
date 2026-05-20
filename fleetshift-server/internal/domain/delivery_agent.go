package domain

import "context"

// DeliveryAgent handles delivery for a specific [TargetType]. Addons
// provide DeliveryAgent implementations for their target types; the
// platform routes delivery to the correct agent based on
// [TargetInfo.Type]. In-process addons implement this interface
// directly; remote addons implement it via a fleetlet channel adapter.
//
// Deliver dispatches delivery to the agent. The method returns an
// error only for infrastructure/dispatch failures (e.g. unreachable
// agent, invalid routing). All delivery outcomes — including immediate
// rejections — are reported asynchronously via the agent's injected
// [DeliveryReporter].
type DeliveryAgent interface {
	Deliver(ctx context.Context, target TargetInfo, deliveryID DeliveryID, manifests []Manifest, auth DeliveryAuth, attestation *Attestation) error
	Remove(ctx context.Context, target TargetInfo, deliveryID DeliveryID, manifests []Manifest, auth DeliveryAuth, attestation *Attestation) error
}
