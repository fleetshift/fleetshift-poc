package kubernetes

import "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"

// Descriptor returns the addon descriptor for the generic Kubernetes
// delivery agent. It declares a delivery capability for Kubernetes
// targets using token-passthrough delivery (no fleetlet).
func Descriptor() domain.AddonDescriptor {
	return domain.AddonDescriptor{
		ID:   "kubernetes",
		Name: "Kubernetes Delivery Agent",
		Capabilities: []domain.Capability{
			domain.DeliveryCapability{TargetType: TargetType},
		},
	}
}
