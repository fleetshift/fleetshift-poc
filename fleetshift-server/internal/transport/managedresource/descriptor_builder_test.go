package managedresource

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		{name: "empty singular", cfg: &ResourceTypeConfig{Singular: "", Plural: "Clusters", ProtoPackage: "pkg"}, spec: true},
		{name: "empty plural", cfg: &ResourceTypeConfig{Singular: "Cluster", Plural: "", ProtoPackage: "pkg"}, spec: true},
		{name: "empty proto package", cfg: &ResourceTypeConfig{Singular: "Cluster", Plural: "Clusters", ProtoPackage: ""}, spec: true},
		{name: "nil spec descriptor", cfg: &ResourceTypeConfig{Singular: "Cluster", Plural: "Clusters", ProtoPackage: "pkg"}, spec: false},
		{name: "lowercase singular", cfg: &ResourceTypeConfig{Singular: "cluster", Plural: "Clusters", ProtoPackage: "pkg"}, spec: true},
		{name: "lowercase plural", cfg: &ResourceTypeConfig{Singular: "Cluster", Plural: "clusters", ProtoPackage: "pkg"}, spec: true},
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
