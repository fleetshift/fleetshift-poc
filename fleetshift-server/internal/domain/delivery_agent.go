package domain

import "context"

// DeliveryAgent handles delivery for a specific [TargetType]. Addons
// provide DeliveryAgent implementations for their target types; the
// platform routes delivery to the correct agent based on
// [TargetInfo.Type]. In-process addons implement this interface
// directly; remote addons implement it via a fleetlet channel adapter.
//
// Agents report delivery progress and results back to the platform
// via a [DeliveryReporter] injected at construction, not per-call.
type DeliveryAgent interface {
	Deliver(ctx context.Context, target TargetInfo, deliveryID DeliveryID, manifests []Manifest, auth DeliveryAuth, attestation *Attestation) (DeliveryResult, error)
	Remove(ctx context.Context, target TargetInfo, deliveryID DeliveryID, manifests []Manifest, auth DeliveryAuth, attestation *Attestation) error
}
