package kind

import (
	_ "embed"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const specProtoPath = "addons/kind/v1/kind_cluster_spec.proto"

//go:embed kind_cluster_spec.proto
var kindClusterSpecProto string

// NodeResourceType is the inventory-only [domain.ResourceType] for
// Kubernetes Nodes discovered inside a kind cluster.
const NodeResourceType domain.ResourceType = "kind.fleetshift.io/Node"

// Descriptor returns the addon descriptor for the kind cluster
// provider. It declares a delivery capability for kind-managed targets,
// a managed+inventory capability for kind Clusters, and an
// inventory-only capability for Nodes.
func Descriptor() domain.AddonDescriptor {
	return domain.AddonDescriptor{
		ID:   "kind.fleetshift.io",
		Name: "Kind Cluster Provider",
		Capabilities: []domain.Capability{
			domain.DeliveryCapability{TargetType: TargetType},
			domain.ManagedResourceCapability{ResourceType: ClusterResourceType},
			domain.InventoryResourceCapability{ResourceType: ClusterResourceType},
			domain.InventoryResourceCapability{ResourceType: NodeResourceType},
		},
	}
}

// Schema returns the extension resource schema for the kind cluster
// resource type. It carries the proto definition and fulfillment
// relation that the platform uses to compile the dynamic API surface
// and route fulfillments to the kind delivery agent, plus an inventory
// section so continuous cluster observations can be reported.
func Schema() domain.ExtensionResourceSchema {
	return domain.ExtensionResourceSchema{
		ResourceType: ClusterResourceType,
		ProtoPackage: "kind.fleetshift.v1",
		Version:      "v1",
		CollectionID: "clusters",
		Singular:     "Cluster",
		Plural:       "Clusters",
		ProtoFiles: map[string]string{
			specProtoPath: kindClusterSpecProto,
		},
		EntryFile: specProtoPath,
		Management: &domain.ManagementSchema{
			SpecMessage: "addons.kind.v1.KindClusterSpec",
			Relation:    domain.NewRegisteredSelfTarget("kind-local", ManagedClusterManifestType),
		},
		Inventory: &domain.InventorySchema{},
	}
}

// NodeSchema returns the inventory-only extension resource schema for
// kind Nodes. Nodes are addressed as flat nodes/{name} until nested
// HTTP routes exist; the parent cluster is linked in observation.
func NodeSchema() domain.ExtensionResourceSchema {
	return domain.ExtensionResourceSchema{
		ResourceType: NodeResourceType,
		ProtoPackage: "kind.fleetshift.v1",
		Version:      "v1",
		CollectionID: "nodes",
		Singular:     "Node",
		Plural:       "Nodes",
		Inventory:    &domain.InventorySchema{},
	}
}
