package clustermgmt

import (
	_ "embed"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// clusterSpecProto holds the proto source for ClusterSpec, embedded from
// the canonical .proto file that lives alongside this package. In a real
// deployment the addon workload would transmit this content at connect
// time; for the in-process POC we embed it at compile time.
//
//go:embed cluster_spec.proto
var clusterSpecProto string

const specProtoPath = "addons/cluster_mgmt/v1/cluster_spec.proto"

// Descriptor returns the addon descriptor for the cluster management
// managed resource type. It declares a managed resource capability for
// the "clusters" resource type.
func Descriptor() domain.AddonDescriptor {
	return domain.AddonDescriptor{
		ID:   "cluster-mgmt",
		Name: "Cluster Management",
		Capabilities: []domain.Capability{
			domain.ManagedResourceCapability{
				ResourceType: "clusters",
			},
		},
	}
}

// Schema returns the workload-provided schema for the clusters managed
// resource type. This is provided at connect time and carries
// everything the platform needs to compile and register the API surface.
func Schema() domain.ManagedResourceSchema {
	return domain.ManagedResourceSchema{
		ResourceType: "clusters",
		Singular:     "Cluster",
		Plural:       "clusters",
		ProtoFiles: map[string]string{
			specProtoPath: clusterSpecProto,
		},
		EntryFile:   specProtoPath,
		SpecMessage: "addons.cluster_mgmt.v1.ClusterSpec",
		Relation: domain.RegisteredSelfTarget{
			AddonTarget: "kind-local",
		},
	}
}
