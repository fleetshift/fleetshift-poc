package gcphcp

import (
	_ "embed"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const specProtoPath = "addons/gcphcp/v1/gcphcp_cluster_spec.proto"

//go:embed gcphcp_cluster_spec.proto
var gcphcpClusterSpecProto string

// TargetType is the [domain.TargetType] for gcphcp-managed targets.
const TargetType domain.TargetType = "gcphcp"

// ClusterResourceType is the [domain.ResourceType] for GCP HCP cluster
// managed resources.
const ClusterResourceType domain.ResourceType = "api.gcphcp.cluster"

// KubernetesTargetType is the [domain.TargetType] for Kubernetes
// clusters provisioned by the GCP HCP addon.
const KubernetesTargetType domain.TargetType = "kubernetes"

// Descriptor returns the addon descriptor for the GCP Hosted Control Plane
// provider. It declares a delivery capability for gcphcp-managed targets
// and a managed resource capability for GCP HCP cluster provisioning.
func Descriptor() domain.AddonDescriptor {
	return domain.AddonDescriptor{
		ID:   "gcphcp",
		Name: "GCP Hosted Control Plane Provider",
		Capabilities: []domain.Capability{
			domain.DeliveryCapability{TargetType: TargetType},
			domain.ManagedResourceCapability{ResourceType: ClusterResourceType},
		},
	}
}

// Schema returns the managed resource schema for the GCP HCP cluster
// resource type. It carries the proto definition and fulfillment
// relation that the platform uses to compile the dynamic API surface
// and route fulfillments to the GCP HCP delivery agent.
func Schema(addonTargetID domain.TargetID) domain.ManagedResourceSchema {
	return domain.ManagedResourceSchema{
		ResourceType: ClusterResourceType,
		Singular:     "GCPHCPCluster",
		Plural:       "GCPHCPClusters",
		ProtoFiles: map[string]string{
			specProtoPath: gcphcpClusterSpecProto,
		},
		EntryFile:   specProtoPath,
		SpecMessage: "addons.gcphcp.v1.GCPHCPClusterSpec",
		Relation: domain.RegisteredSelfTarget{
			AddonTarget: addonTargetID,
		},
	}
}
