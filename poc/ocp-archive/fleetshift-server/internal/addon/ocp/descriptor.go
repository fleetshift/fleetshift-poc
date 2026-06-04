package ocp

import "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"

// Descriptor returns the addon descriptor for the OCP cluster
// provider. It declares a delivery capability for OCP-managed targets.
func Descriptor() domain.AddonDescriptor {
	return domain.AddonDescriptor{
		ID:   "ocp",
		Name: "OCP Cluster Provider",
		Capabilities: []domain.Capability{
			domain.DeliveryCapability{TargetType: TargetType},
		},
	}
}
