package kind

import "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"

// Descriptor returns the addon descriptor for the kind cluster
// provider. It declares a delivery capability for kind-managed targets.
func Descriptor() domain.AddonDescriptor {
	return domain.AddonDescriptor{
		ID:   "kind",
		Name: "Kind Cluster Provider",
		Capabilities: []domain.Capability{
			domain.DeliveryCapability{TargetType: TargetType},
		},
	}
}
