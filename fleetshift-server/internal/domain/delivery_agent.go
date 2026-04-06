package domain

import "context"

// DeliveryAgent handles delivery for a specific [TargetType]. Addons
// provide DeliveryAgent implementations for their target types; the
// platform routes delivery to the correct agent based on
// [TargetInfo.Type]. In-process addons implement this interface
// directly; remote addons implement it via a fleetlet channel adapter.
type DeliveryAgent interface {
	Deliver(ctx context.Context, target TargetInfo, deliveryID DeliveryID, manifests []Manifest, auth DeliveryAuth, attestation *Attestation, signaler *DeliverySignaler) (DeliveryResult, error)
	Remove(ctx context.Context, target TargetInfo, deliveryID DeliveryID, signaler *DeliverySignaler) error
}
