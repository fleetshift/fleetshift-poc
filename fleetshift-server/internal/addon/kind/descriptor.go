package kind

import (
	_ "embed"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const specProtoPath = "addons/kind/v1/kind_cluster_spec.proto"

//go:embed kind_cluster_spec.proto
var kindClusterSpecProto string

// Descriptor returns the addon descriptor for the kind cluster
// provider. It declares a delivery capability for kind-managed targets
// and a managed resource capability for kind cluster provisioning.
func Descriptor() domain.AddonDescriptor {
	return domain.AddonDescriptor{
		ID:   "kind",
		Name: "Kind Cluster Provider",
		Capabilities: []domain.Capability{
			domain.DeliveryCapability{TargetType: TargetType},
			domain.ManagedResourceCapability{ResourceType: ClusterResourceType},
		},
	}
}

// Schema returns the managed resource schema for the kind cluster
// resource type. It carries the proto definition and fulfillment
// relation that the platform uses to compile the dynamic API surface
// and route fulfillments to the kind delivery agent.
func Schema() domain.ManagedResourceSchema {
	return domain.ManagedResourceSchema{
		ResourceType: ClusterResourceType,
		Singular:     "KindCluster",
		Plural:       "KindClusters",
		ProtoFiles: map[string]string{
			specProtoPath: kindClusterSpecProto,
		},
		EntryFile:   specProtoPath,
		SpecMessage: "addons.kind.v1.KindClusterSpec",
		Relation: domain.RegisteredSelfTarget{
			AddonTarget: "kind-local",
		},
	}
}
