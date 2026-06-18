package kubernetes

import "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"

// TargetType is the [domain.TargetType] for Kubernetes clusters
// managed by direct connection (no fleetlet).
const TargetType domain.TargetType = "kubernetes"

// ManifestResourceType is the [domain.ResourceType] for generic
// Kubernetes manifests applied via server-side apply.
const ManifestResourceType domain.ResourceType = "kubernetes"

// Descriptor returns the addon descriptor for the Kubernetes addon.
// It declares delivery and indexing capabilities for Kubernetes targets
// connected directly (no fleetlet).
func Descriptor() domain.AddonDescriptor {
	return domain.AddonDescriptor{
		ID:   "kubernetes",
		Name: "Kubernetes Agent",
		Capabilities: []domain.Capability{
			domain.DeliveryCapability{TargetType: TargetType},
			domain.IndexCapability{TargetType: TargetType},
		},
	}
}
