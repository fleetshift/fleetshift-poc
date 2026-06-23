package application_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// recordingActivator is a test double for [application.SchemaActivator]
// that records Activate/Deactivate calls. It mirrors the real
// activator's hash-based dedup: unchanged schemas are not recorded
// as new activations.
type recordingActivator struct {
	mu          sync.Mutex
	activated   []domain.ManagedResourceSchema
	deactivated []application.SchemaRegistrationID
	nextErr     error
	hashes      map[string][32]byte
}

func (r *recordingActivator) Activate(_ context.Context, schema domain.ManagedResourceSchema) (application.SchemaRegistrationID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nextErr != nil {
		err := r.nextErr
		r.nextErr = nil
		return "", err
	}

	serviceName := schema.ProtoPackage + "." + schema.Singular + "Service"

	hash := testSchemaHash(schema)
	if r.hashes == nil {
		r.hashes = make(map[string][32]byte)
	}
	if prev, ok := r.hashes[serviceName]; ok && prev == hash {
		return application.SchemaRegistrationID(serviceName), nil
	}

	r.activated = append(r.activated, schema)
	r.hashes[serviceName] = hash
	return application.SchemaRegistrationID(serviceName), nil
}

func (r *recordingActivator) Deactivate(id application.SchemaRegistrationID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deactivated = append(r.deactivated, id)
	delete(r.hashes, string(id))
}

func (r *recordingActivator) activatedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.activated)
}

func (r *recordingActivator) deactivatedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.deactivated)
}

func testSchemaHash(s domain.ManagedResourceSchema) [32]byte {
	h := sha256.New()
	h.Write([]byte(s.APIServiceName))
	h.Write([]byte{0})
	h.Write([]byte(s.ProtoPackage))
	h.Write([]byte{0})
	h.Write([]byte(s.Version))
	h.Write([]byte{0})
	h.Write([]byte(s.CollectionID))
	h.Write([]byte{0})
	h.Write([]byte(s.SpecMessage))
	h.Write([]byte{0})
	h.Write([]byte(s.Singular))
	h.Write([]byte{0})
	h.Write([]byte(s.Plural))
	h.Write([]byte{0})
	keys := make([]string, 0, len(s.ProtoFiles))
	for k := range s.ProtoFiles {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(s.ProtoFiles[k]))
		h.Write([]byte{0})
	}
	return [32]byte(h.Sum(nil))
}

type addonManagerEnv struct {
	mgr       *application.AddonManager
	activator *recordingActivator
	router    *delivery.RoutingDeliveryService
	typeSvc   *application.ManagedResourceTypeService
	targetSvc *application.TargetService
}

func setupAddonManager(t *testing.T) *addonManagerEnv {
	t.Helper()

	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	router := delivery.NewRoutingDeliveryService()

	reg := &memworkflow.Registry{}
	orchSpec := domain.NewOrchestrationWorkflowSpec(
		store, router, domain.StrategyFactory{Store: store}, reg,
		domain.WithAckRetryInterval(5*time.Second),
	)
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	createMRWf, err := reg.RegisterCreateManagedResource(&domain.CreateManagedResourceWorkflowSpec{
		Store: store, Orchestration: orchWf,
	})
	if err != nil {
		t.Fatalf("RegisterCreateManagedResource: %v", err)
	}

	mrCleanupWf, err := reg.RegisterDeleteManagedResourceCleanup(&domain.DeleteManagedResourceCleanupWorkflowSpec{Store: store})
	if err != nil {
		t.Fatalf("RegisterDeleteManagedResourceCleanup: %v", err)
	}

	deleteMRWf, err := reg.RegisterDeleteManagedResource(&domain.DeleteManagedResourceWorkflowSpec{
		Store: store, Orchestration: orchWf, Cleanup: mrCleanupWf,
	})
	if err != nil {
		t.Fatalf("RegisterDeleteManagedResource: %v", err)
	}

	// ManagedResourceService is needed by the type service's underlying
	// store transactions, but is not directly exercised here — the
	// activator is stubbed.
	_ = &application.ManagedResourceService{
		Store: store, CreateWF: createMRWf, DeleteWF: deleteMRWf,
	}

	typeSvc := application.NewManagedResourceTypeService(store)
	targetSvc := &application.TargetService{Store: store}

	activator := &recordingActivator{}

	mgr := application.NewAddonManager(application.AddonManagerDeps{
		Router:    router,
		TypeSvc:   typeSvc,
		Activator: activator,
	})

	return &addonManagerEnv{
		mgr:       mgr,
		activator: activator,
		router:    router,
		typeSvc:   typeSvc,
		targetSvc: targetSvc,
	}
}

// clusterMgmtDescriptor returns the cluster-mgmt addon descriptor
// without importing the clustermgmt package — keeps the test focused
// on the application layer's behavior with value objects only.
func clusterMgmtDescriptor() domain.AddonDescriptor {
	return domain.AddonDescriptor{
		ID:   "cluster-mgmt",
		Name: "Cluster Management",
		Capabilities: []domain.Capability{
			domain.ManagedResourceCapability{ResourceType: "test.fleetshift.io/Cluster"},
		},
	}
}

func clusterSchema() domain.ManagedResourceSchema {
	return domain.ManagedResourceSchema{
		ResourceType:   "test.fleetshift.io/Cluster",
		APIServiceName: "test.fleetshift.io",
		ProtoPackage:   "test.fleetshift.v1",
		Version:        "v1",
		CollectionID:   "clusters",
		Singular:       "Cluster",
		Plural:         "Clusters",
		ProtoFiles:     map[string]string{"fake.proto": "syntax = \"proto3\";"},
		SpecMessage:    "fake.ClusterSpec",
		Relation:       domain.NewRegisteredSelfTarget("kind-local", "api.kind.cluster"),
	}
}

func TestAddonManager_EnableRecordsCapabilities(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	desc := clusterMgmtDescriptor()
	if err := env.mgr.Enable(ctx, desc); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	addon, err := env.mgr.Get("cluster-mgmt")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if addon.State != domain.AddonStateEnabled {
		t.Errorf("state = %d, want %d (enabled)", addon.State, domain.AddonStateEnabled)
	}
	if len(addon.Capabilities) != 1 {
		t.Fatalf("capabilities count = %d, want 1", len(addon.Capabilities))
	}
	if addon.Capabilities[0].CapabilityType() != "managed_resource" {
		t.Errorf("capability type = %q, want managed_resource", addon.Capabilities[0].CapabilityType())
	}
}

func TestAddonManager_EnableDoesNotActivateSchemas(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	if err := env.mgr.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	if env.activator.activatedCount() != 0 {
		t.Errorf("expected no schema activations after Enable, got %d", env.activator.activatedCount())
	}
}

func TestAddonManager_ConnectActivatesSchemas(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	if err := env.mgr.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	if err := env.targetSvc.Register(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "kind-local", Type: "kind", Name: "Local Kind",
		AcceptedManifestTypes: []domain.ManifestType{"clusters"},
	})); err != nil {
		t.Fatalf("register target: %v", err)
	}

	schema := clusterSchema()
	if err := env.mgr.Connect(ctx, "cluster-mgmt", application.ConnectInput{
		Schemas: []domain.ManagedResourceSchema{schema},
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	addon, _ := env.mgr.Get("cluster-mgmt")
	if addon.State != domain.AddonStateConnected {
		t.Errorf("state = %d, want %d (connected)", addon.State, domain.AddonStateConnected)
	}

	if env.activator.activatedCount() != 1 {
		t.Fatalf("activated count = %d, want 1", env.activator.activatedCount())
	}
	if env.activator.activated[0].ResourceType != "test.fleetshift.io/Cluster" {
		t.Errorf("activated resource type = %q, want test.fleetshift.io/Cluster", env.activator.activated[0].ResourceType)
	}

	typeDef, err := env.typeSvc.Get(ctx, "test.fleetshift.io/Cluster")
	if err != nil {
		t.Fatalf("Get type def: %v", err)
	}
	if typeDef.ResourceType != "test.fleetshift.io/Cluster" {
		t.Errorf("type def resource type = %q, want test.fleetshift.io/Cluster", typeDef.ResourceType)
	}
}

func TestAddonManager_ConnectWithDeliveryAgent(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	desc := kindaddon.Descriptor()
	if err := env.mgr.Enable(ctx, desc); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	agent := &stubDeliveryAgent{}
	if err := env.mgr.Connect(ctx, "kind", application.ConnectInput{
		Agent: agent,
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	addon, _ := env.mgr.Get("kind")
	if addon.State != domain.AddonStateConnected {
		t.Errorf("state = %d, want %d (connected)", addon.State, domain.AddonStateConnected)
	}

	// No schemas were provided, so the activator should not have been called.
	if env.activator.activatedCount() != 0 {
		t.Errorf("activated count = %d, want 0 (delivery-only addon)", env.activator.activatedCount())
	}
}

func TestAddonManager_ConnectSchemaMismatchReturnsError(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	if err := env.mgr.Enable(ctx, kindaddon.Descriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	schema := clusterSchema()
	err := env.mgr.Connect(ctx, "kind", application.ConnectInput{
		Schemas: []domain.ManagedResourceSchema{schema},
	})
	if err == nil {
		t.Fatal("expected error when schema resource type doesn't match declared capabilities")
	}

	if env.activator.activatedCount() != 0 {
		t.Error("activator should not have been called on capability mismatch")
	}
}

func TestAddonManager_ConnectActivationFailureReturnsError(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	if err := env.mgr.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	env.activator.nextErr = fmt.Errorf("compilation failed")

	err := env.mgr.Connect(ctx, "cluster-mgmt", application.ConnectInput{
		Schemas: []domain.ManagedResourceSchema{clusterSchema()},
	})
	if err == nil {
		t.Fatal("expected error when activator fails")
	}
}

func TestAddonManager_ConnectWithoutEnableReturnsError(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	err := env.mgr.Connect(ctx, "nonexistent", application.ConnectInput{})
	if err == nil {
		t.Fatal("expected error when connecting unenabled addon")
	}
}

func TestAddonManager_Disconnect(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	desc := kindaddon.Descriptor()
	if err := env.mgr.Enable(ctx, desc); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	agent := &stubDeliveryAgent{}
	if err := env.mgr.Connect(ctx, "kind", application.ConnectInput{
		Agent: agent,
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if err := env.mgr.Disconnect(ctx, "kind"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	addon, _ := env.mgr.Get("kind")
	if addon.State != domain.AddonStateEnabled {
		t.Errorf("state = %d, want %d (enabled)", addon.State, domain.AddonStateEnabled)
	}
}

func TestAddonManager_DisconnectDoesNotDeactivateSchemas(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	if err := env.mgr.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if err := env.targetSvc.Register(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "kind-local", Type: "kind", Name: "Local Kind",
		AcceptedManifestTypes: []domain.ManifestType{"clusters"},
	})); err != nil {
		t.Fatalf("register target: %v", err)
	}
	if err := env.mgr.Connect(ctx, "cluster-mgmt", application.ConnectInput{
		Schemas: []domain.ManagedResourceSchema{clusterSchema()},
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if err := env.mgr.Disconnect(ctx, "cluster-mgmt"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	if env.activator.deactivatedCount() != 0 {
		t.Error("expected no deactivations after Disconnect — API surface should remain live")
	}
}

func TestAddonManager_DisableDeactivatesSchemas(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	if err := env.mgr.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if err := env.targetSvc.Register(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "kind-local", Type: "kind", Name: "Local Kind",
		AcceptedManifestTypes: []domain.ManifestType{"clusters"},
	})); err != nil {
		t.Fatalf("register target: %v", err)
	}
	if err := env.mgr.Connect(ctx, "cluster-mgmt", application.ConnectInput{
		Schemas: []domain.ManagedResourceSchema{clusterSchema()},
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if err := env.mgr.Disable(ctx, "cluster-mgmt"); err != nil {
		t.Fatalf("Disable: %v", err)
	}

	addon, _ := env.mgr.Get("cluster-mgmt")
	if addon.State != domain.AddonStateDefined {
		t.Errorf("state = %d, want %d (defined)", addon.State, domain.AddonStateDefined)
	}

	if env.activator.deactivatedCount() != 1 {
		t.Fatalf("deactivated count = %d, want 1", env.activator.deactivatedCount())
	}
	if env.activator.deactivated[0] != "test.fleetshift.v1.ClusterService" {
		t.Errorf("deactivated service = %q, want test.fleetshift.v1.ClusterService",
			env.activator.deactivated[0])
	}
}

func TestAddonManager_ConnectRegistersTargets(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	desc := kindaddon.Descriptor()
	if err := env.mgr.Enable(ctx, desc); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	agent := &stubDeliveryAgent{}
	err := env.mgr.Connect(ctx, "kind", application.ConnectInput{
		Agent: agent,
		Targets: []domain.TargetInfo{
			domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
				ID:                    "kind-local",
				Type:                  kindaddon.TargetType,
				Name:                  "Local Kind Provider",
				AcceptedManifestTypes: []domain.ManifestType{"clusters", domain.TrustBundleManifestType},
			}),
		},
	})
	if err != nil {
		t.Fatalf("Connect with targets: %v", err)
	}

	addon, _ := env.mgr.Get("kind")
	if addon.State != domain.AddonStateConnected {
		t.Errorf("state = %d, want %d (connected)", addon.State, domain.AddonStateConnected)
	}
}

func TestAddonManager_ConnectDuplicateTargetIsIdempotent(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	if err := env.mgr.Enable(ctx, kindaddon.Descriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:                    "kind-local",
		Type:                  kindaddon.TargetType,
		Name:                  "Local Kind Provider",
		AcceptedManifestTypes: []domain.ManifestType{"clusters"},
	})

	if err := env.targetSvc.Register(ctx, target); err != nil {
		t.Fatalf("pre-register target: %v", err)
	}

	err := env.mgr.Connect(ctx, "kind", application.ConnectInput{
		Agent: &stubDeliveryAgent{},
		Targets: []domain.TargetInfo{
			target,
		},
	})
	if err != nil {
		t.Fatalf("Connect should silently skip existing target: %v", err)
	}
}

func TestAddonManager_ReconnectReconcilesStaleSchemasOnConnect(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	desc := domain.AddonDescriptor{
		ID:   "multi-resource",
		Name: "Multi Resource Addon",
		Capabilities: []domain.Capability{
			domain.ManagedResourceCapability{ResourceType: "test.fleetshift.io/Cluster"},
			domain.ManagedResourceCapability{ResourceType: "test.fleetshift.io/Database"},
		},
	}
	if err := env.mgr.Enable(ctx, desc); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	if err := env.targetSvc.Register(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "kind-local", Type: "kind", Name: "Local Kind",
		AcceptedManifestTypes: []domain.ManifestType{"clusters", "databases"},
	})); err != nil {
		t.Fatalf("register target: %v", err)
	}

	clusterS := clusterSchema()
	databaseS := domain.ManagedResourceSchema{
		ResourceType:   "test.fleetshift.io/Database",
		APIServiceName: "test.fleetshift.io",
		ProtoPackage:   "test.fleetshift.v1",
		Version:        "v1",
		CollectionID:   "databases",
		Singular:       "Database",
		Plural:         "Databases",
		ProtoFiles:     map[string]string{"fake_db.proto": "syntax = \"proto3\";"},
		SpecMessage:    "fake.DatabaseSpec",
		Relation:       domain.NewRegisteredSelfTarget("kind-local", "api.fake.database"),
	}
	if err := env.mgr.Connect(ctx, "multi-resource", application.ConnectInput{
		Schemas: []domain.ManagedResourceSchema{clusterS, databaseS},
	}); err != nil {
		t.Fatalf("first Connect: %v", err)
	}

	if env.activator.activatedCount() != 2 {
		t.Fatalf("activated count = %d, want 2 after first connect", env.activator.activatedCount())
	}

	// Disconnect, then reconnect with only clusters — databases should
	// be deactivated and the clusters schema should be left in place.
	if err := env.mgr.Disconnect(ctx, "multi-resource"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	if err := env.mgr.Connect(ctx, "multi-resource", application.ConnectInput{
		Schemas: []domain.ManagedResourceSchema{clusterS},
	}); err != nil {
		t.Fatalf("second Connect: %v", err)
	}

	if env.activator.deactivatedCount() != 1 {
		t.Fatalf("deactivated count = %d, want 1 (databases)", env.activator.deactivatedCount())
	}
	if env.activator.deactivated[0] != "test.fleetshift.v1.DatabaseService" {
		t.Errorf("deactivated service = %q, want test.fleetshift.v1.DatabaseService",
			env.activator.deactivated[0])
	}

	// Clusters should NOT have been re-activated — still 2 total.
	if env.activator.activatedCount() != 2 {
		t.Errorf("activated count = %d, want 2 (clusters should not be re-activated)",
			env.activator.activatedCount())
	}
}

func TestAddonManager_ReconnectWithSameSchemasIsIdempotent(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	if err := env.mgr.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if err := env.targetSvc.Register(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "kind-local", Type: "kind", Name: "Local Kind",
		AcceptedManifestTypes: []domain.ManifestType{"clusters"},
	})); err != nil {
		t.Fatalf("register target: %v", err)
	}

	schema := clusterSchema()
	if err := env.mgr.Connect(ctx, "cluster-mgmt", application.ConnectInput{
		Schemas: []domain.ManagedResourceSchema{schema},
	}); err != nil {
		t.Fatalf("first Connect: %v", err)
	}

	if err := env.mgr.Disconnect(ctx, "cluster-mgmt"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	if err := env.mgr.Connect(ctx, "cluster-mgmt", application.ConnectInput{
		Schemas: []domain.ManagedResourceSchema{schema},
	}); err != nil {
		t.Fatalf("second Connect: %v", err)
	}

	if env.activator.activatedCount() != 1 {
		t.Errorf("activated count = %d, want 1 (should not re-activate unchanged schema)",
			env.activator.activatedCount())
	}
	if env.activator.deactivatedCount() != 0 {
		t.Errorf("deactivated count = %d, want 0 (no stale schemas)",
			env.activator.deactivatedCount())
	}
}

func TestAddonManager_ReconnectWithUpdatedSchemaReactivates(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	if err := env.mgr.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if err := env.targetSvc.Register(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "kind-local", Type: "kind", Name: "Local Kind",
		AcceptedManifestTypes: []domain.ManifestType{"clusters"},
	})); err != nil {
		t.Fatalf("register target: %v", err)
	}

	v1 := clusterSchema()
	if err := env.mgr.Connect(ctx, "cluster-mgmt", application.ConnectInput{
		Schemas: []domain.ManagedResourceSchema{v1},
	}); err != nil {
		t.Fatalf("first Connect: %v", err)
	}

	if err := env.mgr.Disconnect(ctx, "cluster-mgmt"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	// Reconnect with an updated schema (different proto content).
	v2 := clusterSchema()
	v2.ProtoFiles = map[string]string{"fake.proto": "syntax = \"proto3\";\nmessage ClusterSpecV2 {}"}
	v2.SpecMessage = "fake.ClusterSpecV2"

	if err := env.mgr.Connect(ctx, "cluster-mgmt", application.ConnectInput{
		Schemas: []domain.ManagedResourceSchema{v2},
	}); err != nil {
		t.Fatalf("second Connect: %v", err)
	}

	// No application-level deactivation — same resource type is still in
	// the input. The activator detected the content change via hash and
	// atomically swapped the mux entry.
	if env.activator.deactivatedCount() != 0 {
		t.Fatalf("deactivated count = %d, want 0 (activator swaps atomically)", env.activator.deactivatedCount())
	}
	if env.activator.activatedCount() != 2 {
		t.Fatalf("activated count = %d, want 2 (v1 + v2)", env.activator.activatedCount())
	}

	lastActivated := env.activator.activated[1]
	if lastActivated.SpecMessage != "fake.ClusterSpecV2" {
		t.Errorf("last activated spec = %q, want fake.ClusterSpecV2", lastActivated.SpecMessage)
	}
}

func TestAddonManager_ReconnectDeactivatesOldRegistrationOnIDChange(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	if err := env.mgr.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if err := env.targetSvc.Register(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "kind-local", Type: "kind", Name: "Local Kind",
		AcceptedManifestTypes: []domain.ManifestType{"clusters"},
	})); err != nil {
		t.Fatalf("register target: %v", err)
	}

	v1 := clusterSchema()
	if err := env.mgr.Connect(ctx, "cluster-mgmt", application.ConnectInput{
		Schemas: []domain.ManagedResourceSchema{v1},
	}); err != nil {
		t.Fatalf("first Connect: %v", err)
	}

	if err := env.mgr.Disconnect(ctx, "cluster-mgmt"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	// Reconnect with a schema that yields a different registration ID
	// (different ProtoPackage produces a different gRPC service name).
	v2 := clusterSchema()
	v2.ProtoPackage = "test.fleetshift.v2"
	v2.ProtoFiles = map[string]string{"fake.proto": "syntax = \"proto3\";\nmessage ClusterSpecV2 {}"}
	v2.SpecMessage = "fake.ClusterSpecV2"

	if err := env.mgr.Connect(ctx, "cluster-mgmt", application.ConnectInput{
		Schemas: []domain.ManagedResourceSchema{v2},
	}); err != nil {
		t.Fatalf("second Connect: %v", err)
	}

	if env.activator.deactivatedCount() != 1 {
		t.Fatalf("deactivated count = %d, want 1 (old registration ID)", env.activator.deactivatedCount())
	}
	if env.activator.deactivated[0] != "test.fleetshift.v1.ClusterService" {
		t.Errorf("deactivated service = %q, want test.fleetshift.v1.ClusterService",
			env.activator.deactivated[0])
	}
}

func TestAddonManager_ReEnableAfterDisable(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	desc := clusterMgmtDescriptor()
	if err := env.mgr.Enable(ctx, desc); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if err := env.targetSvc.Register(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "kind-local", Type: "kind", Name: "Local Kind",
		AcceptedManifestTypes: []domain.ManifestType{"clusters"},
	})); err != nil {
		t.Fatalf("register target: %v", err)
	}
	if err := env.mgr.Connect(ctx, "cluster-mgmt", application.ConnectInput{
		Schemas: []domain.ManagedResourceSchema{clusterSchema()},
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := env.mgr.Disconnect(ctx, "cluster-mgmt"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if err := env.mgr.Disable(ctx, "cluster-mgmt"); err != nil {
		t.Fatalf("Disable: %v", err)
	}

	addon, _ := env.mgr.Get("cluster-mgmt")
	if addon.State != domain.AddonStateDefined {
		t.Fatalf("state after Disable = %d, want %d (defined)", addon.State, domain.AddonStateDefined)
	}

	if err := env.mgr.Enable(ctx, desc); err != nil {
		t.Fatalf("re-Enable after Disable: %v", err)
	}

	addon, _ = env.mgr.Get("cluster-mgmt")
	if addon.State != domain.AddonStateEnabled {
		t.Errorf("state after re-Enable = %d, want %d (enabled)", addon.State, domain.AddonStateEnabled)
	}
}

func TestAddonManager_DuplicateEnableReturnsError(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	desc := clusterMgmtDescriptor()
	if err := env.mgr.Enable(ctx, desc); err != nil {
		t.Fatalf("first Enable: %v", err)
	}

	err := env.mgr.Enable(ctx, desc)
	if err == nil {
		t.Fatal("expected error on duplicate Enable")
	}
}

// TestAddonManager_ConnectRejectsConflictingAPIMetadata verifies that
// when an existing type def has non-empty API identity metadata and the
// reconnecting addon provides different values, Connect fails.
func TestAddonManager_ConnectRejectsConflictingAPIMetadata(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	buildManager := func() *addonManagerEnv {
		router := delivery.NewRoutingDeliveryService()
		typeSvc := application.NewManagedResourceTypeService(store)
		targetSvc := &application.TargetService{Store: store}
		activator := &recordingActivator{}
		mgr := application.NewAddonManager(application.AddonManagerDeps{
			Router:    router,
			TypeSvc:   typeSvc,
			Activator: activator,
		})
		return &addonManagerEnv{
			mgr:       mgr,
			activator: activator,
			router:    router,
			typeSvc:   typeSvc,
			targetSvc: targetSvc,
		}
	}

	ctx := context.Background()

	// --- first "pod": creates type def WITH API metadata ---
	env1 := buildManager()
	if err := env1.mgr.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatalf("Enable (pod 1): %v", err)
	}
	if err := env1.targetSvc.Register(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "kind-local", Type: "kind", Name: "Local Kind",
		AcceptedManifestTypes: []domain.ManifestType{"clusters"},
	})); err != nil {
		t.Fatalf("register target: %v", err)
	}
	schema1 := clusterSchema()
	if err := env1.mgr.Connect(ctx, "cluster-mgmt", application.ConnectInput{
		Schemas: []domain.ManagedResourceSchema{schema1},
	}); err != nil {
		t.Fatalf("Connect (pod 1): %v", err)
	}

	// --- second "pod": reconnects with DIFFERENT API metadata ---
	env2 := buildManager()
	if err := env2.mgr.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatalf("Enable (pod 2): %v", err)
	}
	schema2 := clusterSchema()
	schema2.APIServiceName = "different.service.io"
	err := env2.mgr.Connect(ctx, "cluster-mgmt", application.ConnectInput{
		Schemas: []domain.ManagedResourceSchema{schema2},
	})
	if err == nil {
		t.Fatal("expected error when reconnecting with conflicting API metadata")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

// stubDeliveryAgent is a minimal stub for testing delivery agent
// registration through the addon lifecycle. It satisfies the
// domain.DeliveryAgent interface without performing real delivery.
type stubDeliveryAgent struct{}

func (s *stubDeliveryAgent) Deliver(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ domain.Generation) error {
	return nil
}

func (s *stubDeliveryAgent) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ domain.Generation) error {
	return nil
}

var _ domain.DeliveryAgent = (*stubDeliveryAgent)(nil)

// TestAddonManager_ConnectTypeDefAlreadyExistsIsIdempotent simulates a pod
// restart: the first AddonManager creates the type def in the DB, then a
// second AddonManager (empty in-memory state, same DB) connects the same
// addon. Before the fix, the second Connect would fail with
// "already exists: resource type ...".
func TestAddonManager_ConnectTypeDefAlreadyExistsIsIdempotent(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	buildManager := func() *addonManagerEnv {
		router := delivery.NewRoutingDeliveryService()
		reg := &memworkflow.Registry{}
		orchSpec := domain.NewOrchestrationWorkflowSpec(
			store, router, domain.StrategyFactory{Store: store}, reg,
			domain.WithAckRetryInterval(5*time.Second),
		)
		orchWf, err := reg.RegisterOrchestration(orchSpec)
		if err != nil {
			t.Fatalf("RegisterOrchestration: %v", err)
		}
		createMRWf, err := reg.RegisterCreateManagedResource(&domain.CreateManagedResourceWorkflowSpec{
			Store: store, Orchestration: orchWf,
		})
		if err != nil {
			t.Fatalf("RegisterCreateManagedResource: %v", err)
		}
		mrCleanupWf, err := reg.RegisterDeleteManagedResourceCleanup(&domain.DeleteManagedResourceCleanupWorkflowSpec{Store: store})
		if err != nil {
			t.Fatalf("RegisterDeleteManagedResourceCleanup: %v", err)
		}
		_, err = reg.RegisterDeleteManagedResource(&domain.DeleteManagedResourceWorkflowSpec{
			Store: store, Orchestration: orchWf, Cleanup: mrCleanupWf,
		})
		if err != nil {
			t.Fatalf("RegisterDeleteManagedResource: %v", err)
		}
		_ = createMRWf

		typeSvc := application.NewManagedResourceTypeService(store)
		targetSvc := &application.TargetService{Store: store}
		activator := &recordingActivator{}

		mgr := application.NewAddonManager(application.AddonManagerDeps{
			Router:    router,
			TypeSvc:   typeSvc,
			Activator: activator,
		})
		return &addonManagerEnv{
			mgr:       mgr,
			activator: activator,
			router:    router,
			typeSvc:   typeSvc,
			targetSvc: targetSvc,
		}
	}

	ctx := context.Background()
	schema := clusterSchema()

	// --- first "pod" ---
	env1 := buildManager()
	if err := env1.mgr.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatalf("Enable (pod 1): %v", err)
	}
	if err := env1.targetSvc.Register(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "kind-local", Type: "kind", Name: "Local Kind",
		AcceptedManifestTypes: []domain.ManifestType{"clusters"},
	})); err != nil {
		t.Fatalf("register target (pod 1): %v", err)
	}
	if err := env1.mgr.Connect(ctx, "cluster-mgmt", application.ConnectInput{
		Schemas: []domain.ManagedResourceSchema{schema},
	}); err != nil {
		t.Fatalf("Connect (pod 1): %v", err)
	}

	// --- second "pod" (fresh AddonManager, same DB) ---
	env2 := buildManager()
	if err := env2.mgr.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatalf("Enable (pod 2): %v", err)
	}
	if err := env2.mgr.Connect(ctx, "cluster-mgmt", application.ConnectInput{
		Schemas: []domain.ManagedResourceSchema{schema},
	}); err != nil {
		t.Fatalf("Connect (pod 2) should succeed when type def already exists: %v", err)
	}

	// Verify the type def is still intact in the DB.
	typeDef, err := env2.typeSvc.Get(ctx, "test.fleetshift.io/Cluster")
	if err != nil {
		t.Fatalf("Get type def: %v", err)
	}
	if typeDef.ResourceType != "test.fleetshift.io/Cluster" {
		t.Errorf("type def resource type = %q, want test.fleetshift.io/Cluster", typeDef.ResourceType)
	}
}
