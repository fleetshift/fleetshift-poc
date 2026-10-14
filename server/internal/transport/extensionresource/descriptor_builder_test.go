package extensionresource

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

func stubSpecDescriptor() protoreflect.MessageDescriptor {
	return (&timestamppb.Timestamp{}).ProtoReflect().Descriptor()
}

func managedOnlyConfig() *ResourceTypeConfig {
	return &ResourceTypeConfig{
		CollectionConfig: dynamicapi.CollectionConfig{
			Singular:     "Cluster",
			Plural:       "Clusters",
			CollectionID: "clusters",
			Version:      "v1",
		},
		ResourceType: "test.fleetshift.io/Cluster",
		ProtoPackage: "test.fleetshift.v1",
		Capabilities: ResourceCapabilities{
			Management: &ManagementCapabilityConfig{
				SpecMessage:    "google.protobuf.Timestamp",
				SpecDescriptor: stubSpecDescriptor(),
			},
		},
	}
}

func inventoryOnlyConfig() *ResourceTypeConfig {
	return &ResourceTypeConfig{
		CollectionConfig: dynamicapi.CollectionConfig{
			Singular:     "Cluster",
			Plural:       "Clusters",
			CollectionID: "clusters",
			Version:      "v1",
		},
		ResourceType: "test.fleetshift.io/Cluster",
		ProtoPackage: "test.fleetshift.v1",
		Capabilities: ResourceCapabilities{
			Inventory: &InventoryCapabilityConfig{},
		},
	}
}

func managedAndInventoryConfig() *ResourceTypeConfig {
	cfg := managedOnlyConfig()
	cfg.Capabilities.Inventory = &InventoryCapabilityConfig{}
	return cfg
}

func TestBuildExtensionServiceDescriptors_InvalidInputs(t *testing.T) {
	tests := []struct {
		name string
		cfg  *ResourceTypeConfig
		spec bool // false = nil spec descriptor
	}{
		{name: "nil config", cfg: nil, spec: true},
		{name: "empty singular", cfg: &ResourceTypeConfig{CollectionConfig: dynamicapi.CollectionConfig{Singular: "", Plural: "Clusters", CollectionID: "clusters"}, ProtoPackage: "pkg", Capabilities: ResourceCapabilities{Management: &ManagementCapabilityConfig{}}}, spec: true},
		{name: "empty plural", cfg: &ResourceTypeConfig{CollectionConfig: dynamicapi.CollectionConfig{Singular: "Cluster", Plural: "", CollectionID: "clusters"}, ProtoPackage: "pkg", Capabilities: ResourceCapabilities{Management: &ManagementCapabilityConfig{}}}, spec: true},
		{name: "empty proto package", cfg: &ResourceTypeConfig{CollectionConfig: dynamicapi.CollectionConfig{Singular: "Cluster", Plural: "Clusters", CollectionID: "clusters"}, ProtoPackage: "", Capabilities: ResourceCapabilities{Management: &ManagementCapabilityConfig{}}}, spec: true},
		{name: "empty collection ID", cfg: &ResourceTypeConfig{CollectionConfig: dynamicapi.CollectionConfig{Singular: "Cluster", Plural: "Clusters", CollectionID: ""}, ProtoPackage: "pkg", Capabilities: ResourceCapabilities{Management: &ManagementCapabilityConfig{}}}, spec: true},
		{name: "nil spec descriptor with management", cfg: &ResourceTypeConfig{CollectionConfig: dynamicapi.CollectionConfig{Singular: "Cluster", Plural: "Clusters", CollectionID: "clusters"}, ProtoPackage: "pkg", Capabilities: ResourceCapabilities{Management: &ManagementCapabilityConfig{}}}, spec: false},
		{name: "no capabilities", cfg: &ResourceTypeConfig{CollectionConfig: dynamicapi.CollectionConfig{Singular: "Cluster", Plural: "Clusters", CollectionID: "clusters"}, ProtoPackage: "pkg"}, spec: false},
		{name: "lowercase singular", cfg: &ResourceTypeConfig{CollectionConfig: dynamicapi.CollectionConfig{Singular: "cluster", Plural: "Clusters", CollectionID: "clusters"}, ProtoPackage: "pkg", Capabilities: ResourceCapabilities{Management: &ManagementCapabilityConfig{}}}, spec: true},
		{name: "lowercase plural", cfg: &ResourceTypeConfig{CollectionConfig: dynamicapi.CollectionConfig{Singular: "Cluster", Plural: "clusters", CollectionID: "clusters"}, ProtoPackage: "pkg", Capabilities: ResourceCapabilities{Management: &ManagementCapabilityConfig{}}}, spec: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var specDesc protoreflect.MessageDescriptor
			if tt.spec {
				specDesc = stubSpecDescriptor()
			}
			_, err := BuildExtensionServiceDescriptors(tt.cfg, specDesc)
			if err == nil {
				t.Error("expected error for invalid input, got nil")
			}
		})
	}
}

func TestBuildExtensionServiceDescriptors_ManagedOnly(t *testing.T) {
	descs, err := BuildExtensionServiceDescriptors(managedOnlyConfig(), stubSpecDescriptor())
	if err != nil {
		t.Fatalf("BuildExtensionServiceDescriptors: %v", err)
	}

	res := descs.Resource
	assertHasFields(t, res, "name", "uid", "labels", "create_time", "update_time", "etag",
		"spec", "intent_version", "state", "reconciling", "pause_reason", "generation", "provenance", "delete_time")
	assertMissingFields(t, res, "local_labels", "conditions", "observation", "local_update_time", "index_update_time")

	labels := res.Fields().ByName("labels")
	if labels == nil || !labels.IsMap() {
		t.Error("labels should be map<string,string>")
	}

	assertMethodNames(t, descs.Service, "CreateCluster", "GetCluster", "ListClusters", "DeleteCluster", "ResumeCluster")
	if descs.CreateRequest == nil || descs.DeleteRequest == nil || descs.ResumeRequest == nil {
		t.Error("management request descriptors should be present")
	}
	if descs.Spec == nil {
		t.Error("Spec descriptor should be set for managed types")
	}
}

func TestBuildExtensionServiceDescriptors_InventoryOnly(t *testing.T) {
	descs, err := BuildExtensionServiceDescriptors(inventoryOnlyConfig(), nil)
	if err != nil {
		t.Fatalf("BuildExtensionServiceDescriptors: %v", err)
	}

	res := descs.Resource
	assertHasFields(t, res, "name", "uid", "labels", "create_time", "update_time", "etag",
		"local_labels", "conditions", "observation", "local_update_time", "index_update_time")
	assertMissingFields(t, res, "spec", "intent_version", "state", "reconciling", "pause_reason", "generation", "provenance", "delete_time")

	localLabels := res.Fields().ByName("local_labels")
	if localLabels == nil || !localLabels.IsMap() {
		t.Error("local_labels should be a map")
	}
	conditions := res.Fields().ByName("conditions")
	if conditions == nil || !conditions.IsMap() {
		t.Error("conditions should be a map")
	}
	if conditions != nil && conditions.MapValue().Kind() != protoreflect.MessageKind {
		t.Error("conditions map value should be a message")
	}

	assertMethodNames(t, descs.Service, "GetCluster", "ListClusters")
	assertMissingMethods(t, descs.Service, "CreateCluster", "DeleteCluster", "ResumeCluster")
	if descs.CreateRequest != nil || descs.DeleteRequest != nil || descs.ResumeRequest != nil {
		t.Error("management request descriptors should be nil for inventory-only")
	}
	if descs.Spec != nil {
		t.Error("Spec should be nil for inventory-only")
	}
}

func TestBuildExtensionServiceDescriptors_ManagedAndInventory(t *testing.T) {
	descs, err := BuildExtensionServiceDescriptors(managedAndInventoryConfig(), stubSpecDescriptor())
	if err != nil {
		t.Fatalf("BuildExtensionServiceDescriptors: %v", err)
	}

	res := descs.Resource
	assertHasFields(t, res, "name", "uid", "labels", "create_time", "update_time", "etag",
		"spec", "intent_version", "state", "reconciling",
		"local_labels", "conditions", "observation", "local_update_time", "index_update_time")
	assertMethodNames(t, descs.Service, "CreateCluster", "GetCluster", "ListClusters", "DeleteCluster", "ResumeCluster")
}

func TestBuildExtensionServiceDescriptors_CommonLabelsOnAllShapes(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  *ResourceTypeConfig
		spec protoreflect.MessageDescriptor
	}{
		{"managed", managedOnlyConfig(), stubSpecDescriptor()},
		{"inventory", inventoryOnlyConfig(), nil},
		{"both", managedAndInventoryConfig(), stubSpecDescriptor()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			descs, err := BuildExtensionServiceDescriptors(tc.cfg, tc.spec)
			if err != nil {
				t.Fatalf("BuildExtensionServiceDescriptors: %v", err)
			}
			fd := descs.Resource.Fields().ByName("labels")
			if fd == nil || !fd.IsMap() {
				t.Error("common labels map missing")
			}
		})
	}
}

func TestSchemaContentHash_InventoryPresence(t *testing.T) {
	base := domain.ExtensionResourceSchema{
		ResourceType: "test.fleetshift.io/Cluster",
		Singular:     "Cluster",
		Plural:       "Clusters",
		ProtoPackage: "test.fleetshift.v1",
		Version:      "v1",
		CollectionID: "clusters",
		Management:   &domain.ManagementSchema{SpecMessage: "ClusterSpec"},
		ProtoFiles:   map[string]string{"a.proto": "syntax = \"proto3\";"},
	}
	withoutInv := SchemaContentHash(base)

	withInv := base
	withInv.Inventory = &domain.InventorySchema{}
	withInvHash := SchemaContentHash(withInv)

	if withoutInv == withInvHash {
		t.Fatal("expected hash to change when inventory capability appears")
	}

	// Removing inventory again should restore the original hash.
	again := withInv
	again.Inventory = nil
	if SchemaContentHash(again) != withoutInv {
		t.Fatal("expected hash to restore when inventory capability disappears")
	}
}

func assertHasFields(t *testing.T, msg protoreflect.MessageDescriptor, names ...string) {
	t.Helper()
	for _, name := range names {
		if msg.Fields().ByName(protoreflect.Name(name)) == nil {
			t.Errorf("missing field %q", name)
		}
	}
}

func assertMissingFields(t *testing.T, msg protoreflect.MessageDescriptor, names ...string) {
	t.Helper()
	for _, name := range names {
		if msg.Fields().ByName(protoreflect.Name(name)) != nil {
			t.Errorf("unexpected field %q", name)
		}
	}
}

func assertMethodNames(t *testing.T, svc protoreflect.ServiceDescriptor, names ...string) {
	t.Helper()
	got := map[string]bool{}
	for i := 0; i < svc.Methods().Len(); i++ {
		got[string(svc.Methods().Get(i).Name())] = true
	}
	for _, name := range names {
		if !got[name] {
			t.Errorf("missing method %q", name)
		}
	}
	if len(got) != len(names) {
		t.Errorf("method count = %d, want %d (%v)", len(got), len(names), got)
	}
}

func assertMissingMethods(t *testing.T, svc protoreflect.ServiceDescriptor, names ...string) {
	t.Helper()
	for i := 0; i < svc.Methods().Len(); i++ {
		name := string(svc.Methods().Get(i).Name())
		for _, wantMissing := range names {
			if name == wantMissing {
				t.Errorf("unexpected method %q", name)
			}
		}
	}
}
