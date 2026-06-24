package managedresource

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

func stubSpecDescriptor() protoreflect.MessageDescriptor {
	return (&timestamppb.Timestamp{}).ProtoReflect().Descriptor()
}

func TestBuildServiceDescriptors_InvalidInputs(t *testing.T) {
	tests := []struct {
		name string
		cfg  *ResourceTypeConfig
		spec bool // false = nil spec descriptor
	}{
		{name: "nil config", cfg: nil, spec: true},
		{name: "empty singular", cfg: &ResourceTypeConfig{CollectionConfig: dynamicapi.CollectionConfig{Singular: "", Plural: "Clusters", CollectionID: "clusters"}, ProtoPackage: "pkg"}, spec: true},
		{name: "empty plural", cfg: &ResourceTypeConfig{CollectionConfig: dynamicapi.CollectionConfig{Singular: "Cluster", Plural: "", CollectionID: "clusters"}, ProtoPackage: "pkg"}, spec: true},
		{name: "empty proto package", cfg: &ResourceTypeConfig{CollectionConfig: dynamicapi.CollectionConfig{Singular: "Cluster", Plural: "Clusters", CollectionID: "clusters"}, ProtoPackage: ""}, spec: true},
		{name: "empty collection ID", cfg: &ResourceTypeConfig{CollectionConfig: dynamicapi.CollectionConfig{Singular: "Cluster", Plural: "Clusters", CollectionID: ""}, ProtoPackage: "pkg"}, spec: true},
		{name: "nil spec descriptor", cfg: &ResourceTypeConfig{CollectionConfig: dynamicapi.CollectionConfig{Singular: "Cluster", Plural: "Clusters", CollectionID: "clusters"}, ProtoPackage: "pkg"}, spec: false},
		{name: "lowercase singular", cfg: &ResourceTypeConfig{CollectionConfig: dynamicapi.CollectionConfig{Singular: "cluster", Plural: "Clusters", CollectionID: "clusters"}, ProtoPackage: "pkg"}, spec: true},
		{name: "lowercase plural", cfg: &ResourceTypeConfig{CollectionConfig: dynamicapi.CollectionConfig{Singular: "Cluster", Plural: "clusters", CollectionID: "clusters"}, ProtoPackage: "pkg"}, spec: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var specDesc protoreflect.MessageDescriptor
			if tt.spec {
				specDesc = stubSpecDescriptor()
			}
			_, err := BuildServiceDescriptors(tt.cfg, specDesc)
			if err == nil {
				t.Error("expected error for invalid input, got nil")
			}
		})
	}
}
