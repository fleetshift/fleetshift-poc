package kind_test

import (
	"context"
	"testing"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/types/dynamicpb"

	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

// TestKindClusterSpec_NameNotRequired verifies that the KindClusterSpec
// proto (the schema used to validate managed resource creation
// requests) does not require a "name" field. For managed resources,
// the cluster name always comes from the resource's own identity (see
// parseManagedClusterSpec); a "name" in the spec body would be silently
// ignored, so callers must not be forced to supply one.
func TestKindClusterSpec_NameNotRequired(t *testing.T) {
	schema := kindaddon.Schema()
	desc, err := dynamicapi.CompileInline(context.Background(),
		schema.ProtoFiles, schema.EntryFile, schema.Management.SpecMessage)
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}

	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}

	msg := dynamicpb.NewMessage(desc.Message)
	if err := validator.Validate(msg); err != nil {
		t.Errorf("validate KindClusterSpec with no fields set (in particular, no name): %v", err)
	}
}

func TestDescriptor_DeclaresInventoryCapabilities(t *testing.T) {
	d := kindaddon.Descriptor()
	var hasClusterInv, hasNodeInv, hasManaged bool
	for _, c := range d.Capabilities {
		switch c := c.(type) {
		case domain.InventoryResourceCapability:
			switch c.ResourceType {
			case kindaddon.ClusterResourceType:
				hasClusterInv = true
			case kindaddon.NodeResourceType:
				hasNodeInv = true
			}
		case domain.ManagedResourceCapability:
			if c.ResourceType == kindaddon.ClusterResourceType {
				hasManaged = true
			}
		}
	}
	if !hasManaged {
		t.Error("missing ManagedResourceCapability for Cluster")
	}
	if !hasClusterInv {
		t.Error("missing InventoryResourceCapability for Cluster")
	}
	if !hasNodeInv {
		t.Error("missing InventoryResourceCapability for Node")
	}
}

func TestSchema_IncludesInventory(t *testing.T) {
	s := kindaddon.Schema()
	if s.Inventory == nil {
		t.Fatal("Schema().Inventory is nil")
	}
	if s.Management == nil {
		t.Fatal("Schema().Management is nil")
	}
}

func TestSchema_UsesManagedClusterManifestType(t *testing.T) {
	s := kindaddon.Schema()
	if s.Management == nil {
		t.Fatal("Schema().Management is nil")
	}
	rst, ok := s.Management.Relation.(domain.RegisteredSelfTarget)
	if !ok {
		t.Fatalf("Relation type = %T, want RegisteredSelfTarget", s.Management.Relation)
	}
	if rst.ManifestType() != kindaddon.ManagedClusterManifestType {
		t.Errorf("ManifestType() = %q, want %q", rst.ManifestType(), kindaddon.ManagedClusterManifestType)
	}
	if rst.ManifestType() == kindaddon.ClusterManifestType {
		t.Fatal("managed self-target must not reuse the bare ClusterSpec manifest type")
	}
}

func TestNodeSchema_InventoryOnly(t *testing.T) {
	s := kindaddon.NodeSchema()
	if s.ResourceType != kindaddon.NodeResourceType {
		t.Errorf("ResourceType = %q, want %q", s.ResourceType, kindaddon.NodeResourceType)
	}
	if s.CollectionID != "nodes" {
		t.Errorf("CollectionID = %q, want nodes", s.CollectionID)
	}
	if s.Inventory == nil {
		t.Fatal("NodeSchema().Inventory is nil")
	}
	if s.Management != nil {
		t.Fatal("NodeSchema().Management should be nil (inventory-only)")
	}
	if len(s.ProtoFiles) != 0 {
		t.Errorf("ProtoFiles = %v, want empty for inventory-only", s.ProtoFiles)
	}
}
