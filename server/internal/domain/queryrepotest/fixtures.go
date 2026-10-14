package queryrepotest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// fixedTime anchors every fixture's timestamps so equivalence
// comparisons (see equiv.go) never depend on wall-clock skew.
var fixedTime = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

// Fixture holds the identifiers queryrepotest's core seeded resources
// are addressed by, so individual test cases don't need to re-derive
// names/types/UIDs.
type Fixture struct {
	// PlatformOnlyName is a physical platform resource with no
	// extension representation: clusters/platform-only, env=prod.
	PlatformOnlyName domain.ResourceName

	// ManagedType/ManagedName/ManagedUID identify a managed extension
	// resource: kind.fleetshift.io/Cluster, clusters/managed, spec
	// {"provider":"aws","region":"us-east-1"}, labels team=platform.
	// It has no physical platform_resources row, so it also doubles
	// as the virtual-platform-resource fixture at the same name.
	ManagedType domain.ResourceType
	ManagedName domain.ResourceName
	ManagedUID  domain.ExtensionResourceUID

	// InventoryType/InventoryName/InventoryUID identify an
	// inventory-only extension resource: kubernetes.fleetshift.io/Node,
	// nodes/node-1, inventory labels node-role=worker, condition
	// Ready=True, observation {"capacity":{"cpu":8},"allocatable":{"cpu":6}}.
	// Like ManagedName, it has no physical platform_resources row.
	InventoryType domain.ResourceType
	InventoryName domain.ResourceName
	InventoryUID  domain.ExtensionResourceUID
}

// SeedCoreFixtures seeds four core fixtures (a physical platform
// resource, a managed extension resource, an inventory-only extension
// resource, and -- implicitly, since neither extension resource has a
// physical platform_resources row -- two virtual platform resources)
// into tx, and returns their identifiers.
func SeedCoreFixtures(t *testing.T, tx domain.Tx) Fixture {
	t.Helper()
	ctx := context.Background()

	platformOnly := domain.ResourceName("clusters/platform-only")
	if err := tx.ResourceIdentities().Create(ctx, domain.NewPlatformResource(platformOnly, map[string]string{"env": "prod"}, fixedTime)); err != nil {
		t.Fatalf("seed platform-only resource: %v", err)
	}

	managedType := domain.ResourceType("kind.fleetshift.io/Cluster")
	seedManagedType(t, tx, managedType)
	fID := domain.FulfillmentID("query-fixture-managed")
	seedFulfillment(t, tx, fID, fixedTime)
	managedUID := domain.NewExtensionResourceUID()
	managedName := domain.ResourceName("clusters/managed")
	managed := domain.NewExtensionResource(managedUID, managedType, managedName, fixedTime,
		domain.WithManagedState(fID),
		domain.WithExtensionLabels(map[string]string{"team": "platform"}),
	)
	if _, err := managed.RecordIntent(json.RawMessage(`{"provider":"aws","region":"us-east-1"}`), fixedTime); err != nil {
		t.Fatalf("record intent for managed fixture: %v", err)
	}
	if err := tx.ExtensionResources().Create(ctx, managed); err != nil {
		t.Fatalf("seed managed extension resource: %v", err)
	}

	invType := domain.ResourceType("kubernetes.fleetshift.io/Node")
	seedInventoryType(t, tx, invType)
	invUID := domain.NewExtensionResourceUID()
	invName := domain.ResourceName("nodes/node-1")
	inv := domain.NewExtensionResource(invUID, invType, invName, fixedTime)
	if err := tx.ExtensionResources().Create(ctx, inv); err != nil {
		t.Fatalf("seed inventory-only extension resource: %v", err)
	}
	obs := json.RawMessage(`{"capacity":{"cpu":8},"allocatable":{"cpu":6}}`)
	ready, err := domain.NewCondition("Ready", domain.ConditionTrue, "NodeReady", "node is ready", fixedTime)
	if err != nil {
		t.Fatalf("build Ready condition: %v", err)
	}
	if err := tx.ExtensionResources().ReplaceInventory(ctx, []domain.InventoryReplacement{{
		ResourceType: invType,
		Name:         invName,
		CandidateUID: invUID,
		Labels:       map[string]string{"node-role": "worker"},
		Observation:  &obs,
		Conditions:   []domain.Condition{ready},
		ObservedAt:   fixedTime,
		ReceivedAt:   fixedTime,
	}}); err != nil {
		t.Fatalf("seed inventory for inventory-only fixture: %v", err)
	}

	return Fixture{
		PlatformOnlyName: platformOnly,
		ManagedType:      managedType,
		ManagedName:      managedName,
		ManagedUID:       managedUID,
		InventoryType:    invType,
		InventoryName:    invName,
		InventoryUID:     invUID,
	}
}

// seedManagedType registers a management-capable extension resource
// type, mirroring extensionresourcerepotest's sampleType helper
// (unexported there, so reimplemented minimally here rather than
// exported solely for this one caller).
func seedManagedType(t *testing.T, tx domain.Tx, rt domain.ResourceType) {
	t.Helper()
	typeName := rt.TypeName()
	def := domain.NewExtensionResourceType(
		rt, "v1", domain.CollectionID("clusters"), fixedTime,
		domain.WithManagement(
			domain.NewRegisteredSelfTarget(
				domain.TargetID("addon-"+typeName),
				domain.ManifestType("api.test."+typeName),
			),
			domain.Signature{
				Signer:         domain.FederatedIdentity{Subject: "addon-svc", Issuer: "https://issuer.test"},
				ContentHash:    []byte("hash"),
				SignatureBytes: []byte("sig"),
			},
		),
	)
	if err := tx.ExtensionResources().CreateType(context.Background(), def); err != nil {
		t.Fatalf("seed managed type %s: %v", rt, err)
	}
}

// seedInventoryType registers an inventory-only extension resource
// type (no management metadata).
func seedInventoryType(t *testing.T, tx domain.Tx, rt domain.ResourceType) {
	t.Helper()
	def := domain.NewExtensionResourceType(
		rt, "v1", domain.CollectionID("nodes"), fixedTime,
		domain.WithInventory(),
	)
	if err := tx.ExtensionResources().CreateType(context.Background(), def); err != nil {
		t.Fatalf("seed inventory type %s: %v", rt, err)
	}
}

// seedFulfillment creates a minimal fulfillment for the managed
// fixture, mirroring extensionresourcerepotest's seedFulfillment
// helper (unexported there).
func seedFulfillment(t *testing.T, tx domain.Tx, fID domain.FulfillmentID, at time.Time) {
	t.Helper()
	f := domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
		ID:        fID,
		State:     domain.FulfillmentStateCreating,
		CreatedAt: at,
		UpdatedAt: at,
	})
	f.AdvanceManifestStrategy(domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
	}, at)
	f.AdvancePlacementStrategy(domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"t1"},
	}, at)
	if err := tx.Fulfillments().Create(context.Background(), f); err != nil {
		t.Fatalf("seed fulfillment: %v", err)
	}
}
