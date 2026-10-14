package kubernetes_test

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestDescriptor_DeclaresDeliveryAndInventoryCapabilities(t *testing.T) {
	desc := kubernetes.Descriptor()

	if desc.ID != kubernetes.AddonID {
		t.Errorf("ID = %q, want %q", desc.ID, kubernetes.AddonID)
	}
	if len(desc.Capabilities) != 2 {
		t.Fatalf("len(Capabilities) = %d, want 2", len(desc.Capabilities))
	}

	var hasDelivery, hasInventory bool
	for _, cap := range desc.Capabilities {
		switch c := cap.(type) {
		case domain.DeliveryCapability:
			hasDelivery = c.TargetType == kubernetes.TargetType
		case domain.InventoryResourceCapability:
			hasInventory = c.ResourceType == kubernetes.ObjectResourceType
		}
	}
	if !hasDelivery {
		t.Error("expected a DeliveryCapability for kubernetes.TargetType")
	}
	if !hasInventory {
		t.Error("expected an InventoryResourceCapability for kubernetes.ObjectResourceType")
	}
}

// TestInventorySchema_ObjectInventoryShape pins every field of the generic
// object inventory schema. Most of these fields (ProtoPackage,
// Singular, Plural) are never read back through
// [domain.ExtensionResourceType] for an inventory-only schema -- the
// platform persists ResourceType/APIVersion/CollectionID and activates
// inventory-only schemas for QueryResources scoping without a management
// proto surface -- so an AddonManager-level registration test cannot
// catch a typo in the unused-looking identity fields. This is the only
// test that can.
func TestInventorySchema_ObjectInventoryShape(t *testing.T) {
	s := kubernetes.InventorySchema()

	if s.ResourceType != kubernetes.ObjectResourceType {
		t.Errorf("ResourceType = %q, want %q", s.ResourceType, kubernetes.ObjectResourceType)
	}
	if s.ProtoPackage != "kubernetes.fleetshift.v1" {
		t.Errorf("ProtoPackage = %q, want %q", s.ProtoPackage, "kubernetes.fleetshift.v1")
	}
	if s.Version != "v1" {
		t.Errorf("Version = %q, want %q", s.Version, "v1")
	}
	if s.CollectionID != string(kubernetes.ObjectCollectionID) {
		t.Errorf("CollectionID = %q, want %q", s.CollectionID, kubernetes.ObjectCollectionID)
	}
	if s.Singular != "Object" {
		t.Errorf("Singular = %q, want %q", s.Singular, "Object")
	}
	if s.Plural != "Objects" {
		t.Errorf("Plural = %q, want %q", s.Plural, "Objects")
	}
	if s.Inventory == nil {
		t.Error("Inventory is nil, want non-nil")
	}
	if s.Management != nil {
		t.Error("Management is non-nil, want nil (inventory-only schema)")
	}
	if len(s.ProtoFiles) != 0 {
		t.Errorf("ProtoFiles = %v, want empty (no proto to compile for an inventory-only schema)", s.ProtoFiles)
	}
	if s.EntryFile != "" {
		t.Errorf("EntryFile = %q, want empty", s.EntryFile)
	}
}
