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
	kubernetesaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// recordingActivator is a test double for [application.SchemaActivator]
// that records Activate/Deactivate calls. It mirrors the real
// activator's hash-based dedup: unchanged schemas are not recorded
// as new activations.
type recordingActivator struct {
	mu          sync.Mutex
	activated   []domain.ExtensionResourceSchema
	deactivated []application.SchemaActivationID
	nextErr     error
	hashes      map[string][32]byte
}

func (r *recordingActivator) Activate(_ context.Context, schema domain.ExtensionResourceSchema) (application.SchemaActivationID, error) {
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
		return application.SchemaActivationID(serviceName), nil
	}

	r.activated = append(r.activated, schema)
	r.hashes[serviceName] = hash
	return application.SchemaActivationID(serviceName), nil
}

func (r *recordingActivator) Deactivate(id application.SchemaActivationID) {
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

func testSchemaHash(s domain.ExtensionResourceSchema) [32]byte {
	h := sha256.New()
	h.Write([]byte(s.ResourceType.ServiceName()))
	h.Write([]byte{0})
	h.Write([]byte(s.ProtoPackage))
	h.Write([]byte{0})
	h.Write([]byte(s.Version))
	h.Write([]byte{0})
	h.Write([]byte(s.CollectionID))
	h.Write([]byte{0})
	if s.Management != nil {
		h.Write([]byte(s.Management.SpecMessage))
	}
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
	typeSvc   *application.ExtensionResourceTypeService
	targetSvc *application.TargetService
}

func setupAddonManager(t *testing.T) *addonManagerEnv {
	t.Helper()

	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	router := delivery.NewRoutingDeliveryService()

	typeSvc := application.NewExtensionResourceTypeService(store)
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

// clusterMgmtDescriptor returns a test addon descriptor for managed
// cluster resources. The ID matches the service name prefix of the
// declared resource type, enforcing the AddonID-as-ServiceName
// invariant.
func clusterMgmtDescriptor() domain.AddonDescriptor {
	return domain.AddonDescriptor{
		ID:   "test.fleetshift.io",
		Name: "Cluster Management",
		Capabilities: []domain.Capability{
			domain.ManagedResourceCapability{ResourceType: "test.fleetshift.io/Cluster"},
		},
	}
}

func clusterSchema() domain.ExtensionResourceSchema {
	return domain.ExtensionResourceSchema{
		ResourceType: "test.fleetshift.io/Cluster",
		ProtoPackage: "test.fleetshift.v1",
		Version:      "v1",
		CollectionID: "clusters",
		Singular:     "Cluster",
		Plural:       "Clusters",
		ProtoFiles:   map[string]string{"fake.proto": "syntax = \"proto3\";"},
		Management: &domain.ManagementSchema{
			SpecMessage: "fake.ClusterSpec",
			Relation:    domain.NewRegisteredSelfTarget("kind-local", "api.kind.cluster"),
		},
	}
}

func TestAddonManager_EnableRecordsCapabilities(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	desc := clusterMgmtDescriptor()
	if err := env.mgr.Enable(ctx, desc); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	addon, err := env.mgr.Get("test.fleetshift.io")
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
	if err := env.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{schema},
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	addon, _ := env.mgr.Get("test.fleetshift.io")
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
	if typeDef.ResourceType() != "test.fleetshift.io/Cluster" {
		t.Errorf("type def resource type = %q, want test.fleetshift.io/Cluster", typeDef.ResourceType())
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
	if err := env.mgr.Connect(ctx, kindaddon.Descriptor().ID, application.ConnectInput{
		Agent: agent,
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	addon, _ := env.mgr.Get(kindaddon.Descriptor().ID)
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
	err := env.mgr.Connect(ctx, kindaddon.Descriptor().ID, application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{schema},
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

	err := env.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{clusterSchema()},
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
	if err := env.mgr.Connect(ctx, kindaddon.Descriptor().ID, application.ConnectInput{
		Agent: agent,
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if err := env.mgr.Disconnect(ctx, kindaddon.Descriptor().ID); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	addon, _ := env.mgr.Get(kindaddon.Descriptor().ID)
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
	if err := env.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{clusterSchema()},
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if err := env.mgr.Disconnect(ctx, "test.fleetshift.io"); err != nil {
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
	if err := env.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{clusterSchema()},
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if err := env.mgr.Disable(ctx, "test.fleetshift.io"); err != nil {
		t.Fatalf("Disable: %v", err)
	}

	addon, _ := env.mgr.Get("test.fleetshift.io")
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

func TestAddonManager_ReconnectReconcilesStaleSchemasOnConnect(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	desc := domain.AddonDescriptor{
		ID:   "test.fleetshift.io",
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
	databaseS := domain.ExtensionResourceSchema{
		ResourceType: "test.fleetshift.io/Database",
		ProtoPackage: "test.fleetshift.v1",
		Version:      "v1",
		CollectionID: "databases",
		Singular:     "Database",
		Plural:       "Databases",
		ProtoFiles:   map[string]string{"fake_db.proto": "syntax = \"proto3\";"},
		Management: &domain.ManagementSchema{
			SpecMessage: "fake.DatabaseSpec",
			Relation:    domain.NewRegisteredSelfTarget("kind-local", "api.fake.database"),
		},
	}
	if err := env.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{clusterS, databaseS},
	}); err != nil {
		t.Fatalf("first Connect: %v", err)
	}

	if env.activator.activatedCount() != 2 {
		t.Fatalf("activated count = %d, want 2 after first connect", env.activator.activatedCount())
	}

	// Disconnect, then reconnect with only clusters — databases should
	// be deactivated and the clusters schema should be left in place.
	if err := env.mgr.Disconnect(ctx, "test.fleetshift.io"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	if err := env.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{clusterS},
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
	if err := env.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{schema},
	}); err != nil {
		t.Fatalf("first Connect: %v", err)
	}

	if err := env.mgr.Disconnect(ctx, "test.fleetshift.io"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	if err := env.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{schema},
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
	if err := env.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{v1},
	}); err != nil {
		t.Fatalf("first Connect: %v", err)
	}

	if err := env.mgr.Disconnect(ctx, "test.fleetshift.io"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	// Reconnect with an updated schema (different proto content).
	v2 := clusterSchema()
	v2.ProtoFiles = map[string]string{"fake.proto": "syntax = \"proto3\";\nmessage ClusterSpecV2 {}"}
	v2.Management = &domain.ManagementSchema{
		SpecMessage: "fake.ClusterSpecV2",
		Relation:    domain.NewRegisteredSelfTarget("kind-local", "api.kind.cluster"),
	}

	if err := env.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{v2},
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
	if lastActivated.Management.SpecMessage != "fake.ClusterSpecV2" {
		t.Errorf("last activated spec = %q, want fake.ClusterSpecV2", lastActivated.Management.SpecMessage)
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
	if err := env.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{v1},
	}); err != nil {
		t.Fatalf("first Connect: %v", err)
	}

	if err := env.mgr.Disconnect(ctx, "test.fleetshift.io"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	// Reconnect with a schema that yields a different registration ID
	// (different ProtoPackage produces a different gRPC service name).
	v2 := clusterSchema()
	v2.ProtoPackage = "test.fleetshift.v2"
	v2.ProtoFiles = map[string]string{"fake.proto": "syntax = \"proto3\";\nmessage ClusterSpecV2 {}"}
	v2.Management = &domain.ManagementSchema{
		SpecMessage: "fake.ClusterSpecV2",
		Relation:    domain.NewRegisteredSelfTarget("kind-local", "api.kind.cluster"),
	}

	if err := env.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{v2},
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
	if err := env.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{clusterSchema()},
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := env.mgr.Disconnect(ctx, "test.fleetshift.io"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if err := env.mgr.Disable(ctx, "test.fleetshift.io"); err != nil {
		t.Fatalf("Disable: %v", err)
	}

	addon, _ := env.mgr.Get("test.fleetshift.io")
	if addon.State != domain.AddonStateDefined {
		t.Fatalf("state after Disable = %d, want %d (defined)", addon.State, domain.AddonStateDefined)
	}

	if err := env.mgr.Enable(ctx, desc); err != nil {
		t.Fatalf("re-Enable after Disable: %v", err)
	}

	addon, _ = env.mgr.Get("test.fleetshift.io")
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
		typeSvc := application.NewExtensionResourceTypeService(store)
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
	if err := env1.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{schema1},
	}); err != nil {
		t.Fatalf("Connect (pod 1): %v", err)
	}

	// --- second "pod": reconnects with DIFFERENT API metadata ---
	env2 := buildManager()
	if err := env2.mgr.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatalf("Enable (pod 2): %v", err)
	}
	schema2 := clusterSchema()
	schema2.Version = "v2"
	err := env2.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{schema2},
	})
	if err == nil {
		t.Fatal("expected error when reconnecting with conflicting API metadata")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

// TestAddonManager_ConnectRejectsConflictingManagementRelation verifies that
// when an existing type def has a management relation and the reconnecting
// addon provides a different relation, Connect fails. This prevents stale
// management metadata from persisting across addon reconnections.
func TestAddonManager_ConnectRejectsConflictingManagementRelation(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	buildManager := func() *addonManagerEnv {
		router := delivery.NewRoutingDeliveryService()
		typeSvc := application.NewExtensionResourceTypeService(store)
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

	// --- first "pod": creates type def with relation targeting kind-local ---
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
	if err := env1.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{schema1},
	}); err != nil {
		t.Fatalf("Connect (pod 1): %v", err)
	}

	// --- second "pod": reconnects with DIFFERENT management relation ---
	env2 := buildManager()
	if err := env2.mgr.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatalf("Enable (pod 2): %v", err)
	}
	schema2 := clusterSchema()
	schema2.Management = &domain.ManagementSchema{
		SpecMessage: schema2.Management.SpecMessage,
		Relation:    domain.NewRegisteredSelfTarget("kind-remote", "api.kind.cluster"),
	}
	err := env2.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{schema2},
	})
	if err == nil {
		t.Fatal("expected error when reconnecting with conflicting management relation")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

// TestAddonManager_ConnectAllowsBackfillWhenManagementNil verifies that
// reconnecting an addon whose persisted type def has no management
// metadata backfills Management onto the catalog so Create can succeed.
func TestAddonManager_ConnectAllowsBackfillWhenManagementNil(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	// Pre-create a type def WITHOUT management metadata so that
	// Management() returns nil after hydration.
	typeSvc := application.NewExtensionResourceTypeService(store)
	_, err := typeSvc.Create(context.Background(), application.CreateExtensionTypeInput{
		ResourceType: "test.fleetshift.io/Cluster",
		APIVersion:   "v1",
		CollectionID: "clusters",
		Management:   nil, // no management metadata
	})
	if err != nil {
		t.Fatalf("pre-create type def: %v", err)
	}

	// Build a fresh manager sharing the same DB.
	router := delivery.NewRoutingDeliveryService()
	activator := &recordingActivator{}
	mgr := application.NewAddonManager(application.AddonManagerDeps{
		Router:    router,
		TypeSvc:   typeSvc,
		Activator: activator,
	})

	ctx := context.Background()

	if err := mgr.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	targetSvc := &application.TargetService{Store: store}
	if err := targetSvc.Register(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "kind-local", Type: "kind", Name: "Local Kind",
		AcceptedManifestTypes: []domain.ManifestType{"clusters"},
	})); err != nil {
		t.Fatalf("register target: %v", err)
	}

	schema := clusterSchema()
	if err := mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{schema},
	}); err != nil {
		t.Fatalf("Connect should backfill when existing management is nil, got: %v", err)
	}

	got, err := typeSvc.Get(ctx, "test.fleetshift.io/Cluster")
	if err != nil {
		t.Fatalf("Get after Connect: %v", err)
	}
	if got.Management() == nil {
		t.Fatal("Management() still nil after Connect; Create would reject the catalog type")
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

		typeSvc := application.NewExtensionResourceTypeService(store)
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
	if err := env1.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{schema},
	}); err != nil {
		t.Fatalf("Connect (pod 1): %v", err)
	}

	// --- second "pod" (fresh AddonManager, same DB) ---
	env2 := buildManager()
	if err := env2.mgr.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatalf("Enable (pod 2): %v", err)
	}
	if err := env2.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{schema},
	}); err != nil {
		t.Fatalf("Connect (pod 2) should succeed when type def already exists: %v", err)
	}

	// Verify the type def is still intact in the DB.
	typeDef, err := env2.typeSvc.Get(ctx, "test.fleetshift.io/Cluster")
	if err != nil {
		t.Fatalf("Get type def: %v", err)
	}
	if typeDef.ResourceType() != "test.fleetshift.io/Cluster" {
		t.Errorf("type def resource type = %q, want test.fleetshift.io/Cluster", typeDef.ResourceType())
	}
}

// --- Inventory capability tests ---

func inventoryOnlyDescriptor() domain.AddonDescriptor {
	return domain.AddonDescriptor{
		ID:   "test.fleetshift.io",
		Name: "Inventory Provider",
		Capabilities: []domain.Capability{
			domain.InventoryResourceCapability{ResourceType: "test.fleetshift.io/Node"},
		},
	}
}

func inventoryOnlySchema() domain.ExtensionResourceSchema {
	return domain.ExtensionResourceSchema{
		ResourceType: "test.fleetshift.io/Node",
		ProtoPackage: "test.fleetshift.v1",
		Version:      "v1",
		CollectionID: "nodes",
		Singular:     "Node",
		Plural:       "Nodes",
		Inventory:    &domain.InventorySchema{},
	}
}

func managedAndInventoryDescriptor() domain.AddonDescriptor {
	return domain.AddonDescriptor{
		ID:   "test.fleetshift.io",
		Name: "Full Provider",
		Capabilities: []domain.Capability{
			domain.ManagedResourceCapability{ResourceType: "test.fleetshift.io/Cluster"},
			domain.InventoryResourceCapability{ResourceType: "test.fleetshift.io/Cluster"},
		},
	}
}

func managedAndInventorySchema() domain.ExtensionResourceSchema {
	s := clusterSchema()
	s.Inventory = &domain.InventorySchema{}
	return s
}

func TestAddonManager_ConnectInventoryOnlyActivatesAndRegistersTypeDef(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	if err := env.mgr.Enable(ctx, inventoryOnlyDescriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	schema := inventoryOnlySchema()
	if err := env.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{schema},
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	addon, _ := env.mgr.Get("test.fleetshift.io")
	if addon.State != domain.AddonStateConnected {
		t.Errorf("state = %d, want %d (connected)", addon.State, domain.AddonStateConnected)
	}

	// Inventory-only schemas must trigger schema activation (read-only
	// dynamic API + platform surface).
	if env.activator.activatedCount() != 1 {
		t.Errorf("activated count = %d, want 1 (inventory-only addon)", env.activator.activatedCount())
	}
	if env.activator.activatedCount() == 1 {
		got := env.activator.activated[0]
		if got.ResourceType != "test.fleetshift.io/Node" {
			t.Errorf("activated resource type = %q, want test.fleetshift.io/Node", got.ResourceType)
		}
		if got.Inventory == nil {
			t.Error("activated schema Inventory is nil, want non-nil")
		}
		if got.Management != nil {
			t.Error("activated schema Management is non-nil, want nil")
		}
	}

	// The type def should exist with Inventory set and Management nil.
	typeDef, err := env.typeSvc.Get(ctx, "test.fleetshift.io/Node")
	if err != nil {
		t.Fatalf("Get type def: %v", err)
	}
	if typeDef.Inventory() == nil {
		t.Error("type def Inventory() is nil, want non-nil")
	}
	if typeDef.Management() != nil {
		t.Error("type def Management() is non-nil, want nil for inventory-only type")
	}
}

func TestAddonManager_ConnectManagedAndInventoryActivatesSchemaAndSetsInventory(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	if err := env.mgr.Enable(ctx, managedAndInventoryDescriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	if err := env.targetSvc.Register(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "kind-local", Type: "kind", Name: "Local Kind",
		AcceptedManifestTypes: []domain.ManifestType{"clusters"},
	})); err != nil {
		t.Fatalf("register target: %v", err)
	}

	schema := managedAndInventorySchema()
	if err := env.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{schema},
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	addon, _ := env.mgr.Get("test.fleetshift.io")
	if addon.State != domain.AddonStateConnected {
		t.Errorf("state = %d, want %d (connected)", addon.State, domain.AddonStateConnected)
	}

	// Managed+inventory schemas activate the transport (management part).
	if env.activator.activatedCount() != 1 {
		t.Fatalf("activated count = %d, want 1", env.activator.activatedCount())
	}

	// Type def should have both Management and Inventory set.
	typeDef, err := env.typeSvc.Get(ctx, "test.fleetshift.io/Cluster")
	if err != nil {
		t.Fatalf("Get type def: %v", err)
	}
	if typeDef.Management() == nil {
		t.Error("type def Management() is nil, want non-nil")
	}
	if typeDef.Inventory() == nil {
		t.Error("type def Inventory() is nil, want non-nil")
	}
}

func TestAddonManager_ConnectRejectsInventoryWithoutCapability(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	// Addon only declares ManagedResourceCapability, not inventory.
	if err := env.mgr.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	schema := clusterSchema()
	schema.Inventory = &domain.InventorySchema{}
	err := env.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{schema},
	})
	if err == nil {
		t.Fatal("expected error when schema has Inventory but addon lacks InventoryResourceCapability")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}

	if env.activator.activatedCount() != 0 {
		t.Error("activator should not have been called when capability validation fails")
	}
}

func TestAddonManager_ConnectRejectsManagementWithoutCapability(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	// Addon only declares InventoryResourceCapability, not managed.
	if err := env.mgr.Enable(ctx, inventoryOnlyDescriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	schema := inventoryOnlySchema()
	schema.Management = &domain.ManagementSchema{
		SpecMessage: "fake.NodeSpec",
		Relation:    domain.NewRegisteredSelfTarget("kind-local", "api.fake.node"),
	}
	err := env.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{schema},
	})
	if err == nil {
		t.Fatal("expected error when schema has Management but addon lacks ManagedResourceCapability")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestAddonManager_EnableKubernetesDescriptorRecordsCapabilities(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	if err := env.mgr.Enable(ctx, kubernetesaddon.Descriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	addon, err := env.mgr.Get(kubernetesaddon.AddonID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if addon.State != domain.AddonStateEnabled {
		t.Errorf("state = %d, want %d (enabled)", addon.State, domain.AddonStateEnabled)
	}

	var hasDelivery, hasInventory bool
	for _, cap := range addon.Capabilities {
		switch c := cap.(type) {
		case domain.DeliveryCapability:
			hasDelivery = c.TargetType == kubernetesaddon.TargetType
		case domain.InventoryResourceCapability:
			hasInventory = c.ResourceType == kubernetesaddon.ObjectResourceType
		}
	}
	if !hasDelivery {
		t.Error("expected a DeliveryCapability for kubernetes.TargetType")
	}
	if !hasInventory {
		t.Error("expected an InventoryResourceCapability for kubernetes.ObjectResourceType")
	}
}

func TestAddonManager_ConnectKubernetesSchemaRegistersAndActivatesInventoryType(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	if err := env.mgr.Enable(ctx, kubernetesaddon.Descriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	if err := env.mgr.Connect(ctx, kubernetesaddon.AddonID, application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{kubernetesaddon.InventorySchema()},
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	addon, _ := env.mgr.Get(kubernetesaddon.AddonID)
	if addon.State != domain.AddonStateConnected {
		t.Errorf("state = %d, want %d (connected)", addon.State, domain.AddonStateConnected)
	}

	// Inventory-only schemas still activate so QueryResources can scope
	// to activated types. Management remains nil.
	if env.activator.activatedCount() != 1 {
		t.Errorf("activated count = %d, want 1 (inventory-only schema)", env.activator.activatedCount())
	}
	if env.activator.activatedCount() == 1 {
		got := env.activator.activated[0]
		if got.ResourceType != kubernetesaddon.ObjectResourceType {
			t.Errorf("activated resource type = %q, want %q", got.ResourceType, kubernetesaddon.ObjectResourceType)
		}
		if got.Inventory == nil {
			t.Error("activated schema Inventory is nil, want non-nil")
		}
		if got.Management != nil {
			t.Error("activated schema Management is non-nil, want nil")
		}
	}

	typeDef, err := env.typeSvc.Get(ctx, kubernetesaddon.ObjectResourceType)
	if err != nil {
		t.Fatalf("Get type def: %v", err)
	}
	if typeDef.Inventory() == nil {
		t.Error("type def Inventory() is nil, want non-nil")
	}
	if typeDef.Management() != nil {
		t.Error("type def Management() is non-nil, want nil for inventory-only type")
	}
	if typeDef.APIVersion() != "v1" {
		t.Errorf("APIVersion() = %q, want %q", typeDef.APIVersion(), "v1")
	}
	if typeDef.CollectionID() != kubernetesaddon.ObjectCollectionID {
		t.Errorf("CollectionID() = %q, want %q", typeDef.CollectionID(), kubernetesaddon.ObjectCollectionID)
	}
}

func TestAddonManager_EnableRejectsServiceNameMismatch(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	// Addon ID is "wrong.fleetshift.io" but capability declares
	// resource type "test.fleetshift.io/Cluster" — service name
	// mismatch must be rejected.
	desc := domain.AddonDescriptor{
		ID:   "wrong.fleetshift.io",
		Name: "Mismatched Addon",
		Capabilities: []domain.Capability{
			domain.ManagedResourceCapability{ResourceType: "test.fleetshift.io/Cluster"},
		},
	}
	err := env.mgr.Enable(ctx, desc)
	if err == nil {
		t.Fatal("expected error when capability resource type service name does not match addon ID")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestAddonManager_ConnectRejectsServiceNameMismatch(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	// Enable with a valid addon whose ID matches its capability's
	// service name.
	desc := domain.AddonDescriptor{
		ID:   "test.fleetshift.io",
		Name: "Test Addon",
		Capabilities: []domain.Capability{
			domain.ManagedResourceCapability{ResourceType: "test.fleetshift.io/Cluster"},
		},
	}
	if err := env.mgr.Enable(ctx, desc); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	// Connect with a schema whose service name doesn't match.
	schema := domain.ExtensionResourceSchema{
		ResourceType: "other.fleetshift.io/Cluster",
		ProtoPackage: "other.fleetshift.v1",
		Version:      "v1",
		CollectionID: "clusters",
		Singular:     "Cluster",
		Plural:       "Clusters",
		ProtoFiles:   map[string]string{"fake.proto": "syntax = \"proto3\";"},
		Management: &domain.ManagementSchema{
			SpecMessage: "fake.ClusterSpec",
			Relation:    domain.NewRegisteredSelfTarget("kind-local", "api.kind.cluster"),
		},
	}
	err := env.mgr.Connect(ctx, desc.ID, application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{schema},
	})
	if err == nil {
		t.Fatal("expected error when schema resource type service name does not match addon ID")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

// --- Create-or-verify target tests (OME-291) ---

// scriptedLocalTarget returns the canonical scripted-local target shape
// used in production bootstrap.
func scriptedLocalTarget() domain.TargetInfo {
	return domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:                    "scripted-local",
		Type:                  "scripted",
		Name:                  "Local Scripted Provider",
		State:                 domain.TargetStateReady,
		AcceptedManifestTypes: []domain.ManifestType{"managed.api.scripted.resource"},
	})
}

func TestAddonManager_ConnectTargetFreshAtomicCreation(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	desc := kindaddon.Descriptor()
	if err := env.mgr.Enable(ctx, desc); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	target := domain.NewTargetInfo(
		"kind-local",
		kindaddon.TargetType,
		"Local Kind Provider",
		domain.TargetStateReady,
		nil,
		nil,
		[]domain.ManifestType{"clusters", domain.TrustBundleManifestType},
	)
	err := env.mgr.Connect(ctx, kindaddon.Descriptor().ID, application.ConnectInput{
		Agent:   &stubDeliveryAgent{},
		Targets: []domain.TargetInfo{target},
	})
	if err != nil {
		t.Fatalf("Connect with fresh target: %v", err)
	}

	addon, _ := env.mgr.Get(kindaddon.Descriptor().ID)
	if addon.State != domain.AddonStateConnected {
		t.Errorf("state = %d, want %d (connected)", addon.State, domain.AddonStateConnected)
	}

	// Verify target + inventory item exist.
	got, err := env.targetSvc.Get(ctx, "kind-local")
	if err != nil {
		t.Fatalf("Get target: %v", err)
	}
	if got.Name() != "Local Kind Provider" {
		t.Errorf("target name = %q, want %q", got.Name(), "Local Kind Provider")
	}
}

func TestAddonManager_ConnectTargetExactRestartIdempotent(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	buildManager := func() *addonManagerEnv {
		router := delivery.NewRoutingDeliveryService()
		typeSvc := application.NewExtensionResourceTypeService(store)
		targetSvc := &application.TargetService{Store: store}
		activator := &recordingActivator{}
		mgr := application.NewAddonManager(application.AddonManagerDeps{
			Router:    router,
			TypeSvc:   typeSvc,
			Activator: activator,
		})
		return &addonManagerEnv{
			mgr: mgr, activator: activator, router: router,
			typeSvc: typeSvc, targetSvc: targetSvc,
		}
	}
	ctx := context.Background()

	target := scriptedLocalTarget()

	// --- first pod: creates target + inventory ---
	env1 := buildManager()
	if err := env1.targetSvc.Register(ctx, target); err != nil {
		t.Fatalf("pre-register target: %v", err)
	}

	// --- second pod: reconnects with same target ---
	env2 := buildManager()
	if err := env2.mgr.Enable(ctx, kindaddon.Descriptor()); err != nil {
		t.Fatalf("Enable (pod 2): %v", err)
	}
	err := env2.mgr.Connect(ctx, kindaddon.Descriptor().ID, application.ConnectInput{
		Agent:   &stubDeliveryAgent{},
		Targets: []domain.TargetInfo{target},
	})
	if err != nil {
		t.Fatalf("Connect should succeed on exact restart: %v", err)
	}
}

func TestAddonManager_ConnectTargetMissingInventoryItemFatal(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	// Create target row directly WITHOUT inventory item.
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	target := domain.NewTargetInfo("orphan-target", "kind", "Orphan", domain.TargetStateReady, nil, nil, nil)
	if err := tx.Targets().Create(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	router := delivery.NewRoutingDeliveryService()
	typeSvc := application.NewExtensionResourceTypeService(store)
	activator := &recordingActivator{}
	mgr := application.NewAddonManager(application.AddonManagerDeps{
		Router: router, TypeSvc: typeSvc, Activator: activator,
	})
	ctx := context.Background()
	if err := mgr.Enable(ctx, kindaddon.Descriptor()); err != nil {
		t.Fatal(err)
	}

	err = mgr.Connect(ctx, kindaddon.Descriptor().ID, application.ConnectInput{
		Agent: &stubDeliveryAgent{},
		Targets: []domain.TargetInfo{
			domain.NewTargetInfo("orphan-target", "kind", "Orphan", domain.TargetStateReady, nil, nil, nil),
		},
	})
	if err == nil {
		t.Fatal("expected fatal error when inventory item is missing")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestAddonManager_ConnectTargetMissingTargetRowFatal(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	// Create inventory item directly WITHOUT target row.
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	invID := domain.InventoryItemID("target:orphan-inv")
	if err := tx.Inventory().Create(context.Background(), domain.NewInventoryItem(
		invID, "kind", "Orphan", nil, nil, nil, syncedTime,
	)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	router := delivery.NewRoutingDeliveryService()
	typeSvc := application.NewExtensionResourceTypeService(store)
	activator := &recordingActivator{}
	mgr := application.NewAddonManager(application.AddonManagerDeps{
		Router: router, TypeSvc: typeSvc, Activator: activator,
	})
	ctx := context.Background()
	if err := mgr.Enable(ctx, kindaddon.Descriptor()); err != nil {
		t.Fatal(err)
	}

	err = mgr.Connect(ctx, kindaddon.Descriptor().ID, application.ConnectInput{
		Agent: &stubDeliveryAgent{},
		Targets: []domain.TargetInfo{
			domain.NewTargetInfo("orphan-inv", "kind", "Orphan", domain.TargetStateReady, nil, nil, nil),
		},
	})
	if err == nil {
		t.Fatal("expected fatal error when target row is missing")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

// syncedTime is a fixed time used in test setup helpers.
var syncedTime = func() time.Time {
	t, _ := time.Parse(time.RFC3339, "2026-01-01T00:00:00Z")
	return t
}()

func TestAddonManager_ConnectTargetMismatchedFieldFatal(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	// Pre-register target with one name.
	original := domain.NewTargetInfo(
		"kind-local", kindaddon.TargetType, "Original Name",
		domain.TargetStateReady, nil, nil,
		[]domain.ManifestType{"clusters"},
	)
	if err := env.targetSvc.Register(ctx, original); err != nil {
		t.Fatalf("pre-register: %v", err)
	}

	if err := env.mgr.Enable(ctx, kindaddon.Descriptor()); err != nil {
		t.Fatal(err)
	}

	// Connect with different name → should fail.
	err := env.mgr.Connect(ctx, kindaddon.Descriptor().ID, application.ConnectInput{
		Agent: &stubDeliveryAgent{},
		Targets: []domain.TargetInfo{
			domain.NewTargetInfo("kind-local", kindaddon.TargetType, "Different Name",
				domain.TargetStateReady, nil, nil,
				[]domain.ManifestType{"clusters"}),
		},
	})
	if err == nil {
		t.Fatal("expected error on target field mismatch")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestAddonManager_ConnectTargetNameCollisionFatal(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	// Pre-register target with certain properties.
	original := domain.NewTargetInfo(
		"kind-local", kindaddon.TargetType, "Local Kind Provider",
		domain.TargetStateReady, nil,
		map[string]string{"region": "us-east"},
		[]domain.ManifestType{"clusters"},
	)
	if err := env.targetSvc.Register(ctx, original); err != nil {
		t.Fatalf("pre-register: %v", err)
	}

	if err := env.mgr.Enable(ctx, kindaddon.Descriptor()); err != nil {
		t.Fatal(err)
	}

	// Connect with same ID and name but different properties → drift.
	err := env.mgr.Connect(ctx, kindaddon.Descriptor().ID, application.ConnectInput{
		Agent: &stubDeliveryAgent{},
		Targets: []domain.TargetInfo{
			domain.NewTargetInfo("kind-local", kindaddon.TargetType, "Local Kind Provider",
				domain.TargetStateReady, nil,
				map[string]string{"region": "eu-west"},
				[]domain.ManifestType{"clusters"}),
		},
	})
	if err == nil {
		t.Fatal("expected error on target properties drift")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

// --- Connect compensation tests (OME-291) ---

func TestAddonManager_ConnectCompensatesOnTargetFailure(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	// Pre-create an orphan target row (without inventory) so the
	// second target in the Connect input will fail verification.
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	orphan := domain.NewTargetInfo("orphan", "kind", "Orphan", domain.TargetStateReady, nil, nil, nil)
	if err := tx.Targets().Create(context.Background(), orphan); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	router := delivery.NewRoutingDeliveryService()
	typeSvc := application.NewExtensionResourceTypeService(store)
	targetSvc := &application.TargetService{Store: store}
	activator := &recordingActivator{}
	mgr := application.NewAddonManager(application.AddonManagerDeps{
		Router: router, TypeSvc: typeSvc, Activator: activator,
	})
	ctx := context.Background()

	if err := mgr.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatal(err)
	}

	schema := clusterSchema()
	err = mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{schema},
		Targets: []domain.TargetInfo{
			// First target: fresh, will succeed.
			domain.NewTargetInfo("good-target", "kind", "Good",
				domain.TargetStateReady, nil, nil, []domain.ManifestType{"clusters"}),
			// Second target: orphan (no inventory), will fail.
			domain.NewTargetInfo("orphan", "kind", "Orphan",
				domain.TargetStateReady, nil, nil, nil),
		},
	})
	if err == nil {
		t.Fatal("expected Connect to fail on orphan target")
	}

	// Compensation should have removed the schema activation and type
	// def created during this call.
	if activator.deactivatedCount() != 1 {
		t.Errorf("deactivated count = %d, want 1 (compensation)", activator.deactivatedCount())
	}
	_, typeErr := typeSvc.Get(ctx, "test.fleetshift.io/Cluster")
	if typeErr == nil {
		t.Error("type def should have been deleted by compensation")
	}

	// The good target that was created before the failure should have
	// been deregistered by compensation.
	_, goodErr := targetSvc.Get(ctx, "good-target")
	if goodErr == nil {
		t.Error("good-target should have been deregistered by compensation")
	}

	// Addon should still be in enabled state, not connected.
	addon, _ := mgr.Get("test.fleetshift.io")
	if addon.State != domain.AddonStateEnabled {
		t.Errorf("state = %d, want %d (enabled, not connected)", addon.State, domain.AddonStateEnabled)
	}
}

func TestAddonManager_ConnectCompensatesOnSchemaFailure(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	// Addon with two managed resource types.
	desc := domain.AddonDescriptor{
		ID:   "test.fleetshift.io",
		Name: "Multi Resource Addon",
		Capabilities: []domain.Capability{
			domain.ManagedResourceCapability{ResourceType: "test.fleetshift.io/Cluster"},
			domain.ManagedResourceCapability{ResourceType: "test.fleetshift.io/Database"},
		},
	}
	if err := env.mgr.Enable(ctx, desc); err != nil {
		t.Fatal(err)
	}

	// Make the activator fail on the second schema.
	clusterS := clusterSchema()
	databaseS := domain.ExtensionResourceSchema{
		ResourceType: "test.fleetshift.io/Database",
		ProtoPackage: "test.fleetshift.v1",
		Version:      "v1",
		CollectionID: "databases",
		Singular:     "Database",
		Plural:       "Databases",
		ProtoFiles:   map[string]string{"fake_db.proto": "syntax = \"proto3\";"},
		Management: &domain.ManagementSchema{
			SpecMessage: "fake.DatabaseSpec",
			Relation:    domain.NewRegisteredSelfTarget("kind-local", "api.fake.database"),
		},
	}

	// Use a custom activator that fails on the second activation.
	failActivator := &failOnNthActivator{
		inner: env.activator,
		failN: 2,
	}
	mgr := application.NewAddonManager(application.AddonManagerDeps{
		Router:    env.router,
		TypeSvc:   env.typeSvc,
		Activator: failActivator,
	})
	if err := mgr.Enable(ctx, desc); err != nil {
		t.Fatal(err)
	}
	err := mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{clusterS, databaseS},
	})
	if err == nil {
		t.Fatal("expected Connect to fail on second schema activation")
	}

	// Compensation should have removed the first schema's type def
	// and deactivated its activation.
	_, typeErr := env.typeSvc.Get(ctx, "test.fleetshift.io/Cluster")
	if typeErr == nil {
		t.Error("Cluster type def should have been deleted by compensation")
	}

	// Database type def was created but activation failed, so it
	// should also be cleaned up.
	_, dbTypeErr := env.typeSvc.Get(ctx, "test.fleetshift.io/Database")
	if dbTypeErr == nil {
		t.Error("Database type def should have been deleted by compensation")
	}

	// Addon should still be enabled, not connected.
	addon, _ := mgr.Get("test.fleetshift.io")
	if addon.State != domain.AddonStateEnabled {
		t.Errorf("state = %d, want %d (enabled)", addon.State, domain.AddonStateEnabled)
	}
}

func TestAddonManager_ConnectCompensationPreservesPreexistingRows(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	ctx := context.Background()

	// --- first pod: creates target + schema successfully ---
	router1 := delivery.NewRoutingDeliveryService()
	typeSvc1 := application.NewExtensionResourceTypeService(store)
	act1 := &recordingActivator{}
	mgr1 := application.NewAddonManager(application.AddonManagerDeps{
		Router: router1, TypeSvc: typeSvc1, Activator: act1,
	})
	if err := mgr1.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatal(err)
	}
	target := domain.NewTargetInfo("kind-local", kindaddon.TargetType, "Local Kind",
		domain.TargetStateReady, nil, nil, []domain.ManifestType{"clusters"})
	if err := mgr1.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{clusterSchema()},
		Targets: []domain.TargetInfo{target},
	}); err != nil {
		t.Fatalf("first Connect: %v", err)
	}

	// Create an orphan target row (no inventory) so Connect fails
	// during the second target's verification.
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	orphan := domain.NewTargetInfo("orphan", "kind", "Orphan", domain.TargetStateReady, nil, nil, nil)
	if err := tx.Targets().Create(ctx, orphan); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// --- second pod: reconnects but second target verification
	//     fails because orphan has no inventory item ---
	router2 := delivery.NewRoutingDeliveryService()
	typeSvc2 := application.NewExtensionResourceTypeService(store)
	targetSvc2 := &application.TargetService{Store: store}
	act2 := &recordingActivator{}
	mgr2 := application.NewAddonManager(application.AddonManagerDeps{
		Router: router2, TypeSvc: typeSvc2, Activator: act2,
	})
	if err := mgr2.Enable(ctx, clusterMgmtDescriptor()); err != nil {
		t.Fatal(err)
	}

	err = mgr2.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{clusterSchema()},
		Targets: []domain.TargetInfo{
			target,
			// Orphan target (no inventory) → fatal.
			domain.NewTargetInfo("orphan", "kind", "Orphan", domain.TargetStateReady, nil, nil, nil),
		},
	})
	if err == nil {
		t.Fatal("expected failure")
	}

	// Preexisting target from pod 1 should still be intact.
	_, goodErr := targetSvc2.Get(ctx, "kind-local")
	if goodErr != nil {
		t.Fatalf("preexisting target should be preserved: %v", goodErr)
	}

	// Preexisting type def from pod 1 should still be intact (schema
	// was reconciled, not freshly created, so compensation doesn't
	// touch it).
	_, typeErr := typeSvc2.Get(ctx, "test.fleetshift.io/Cluster")
	if typeErr != nil {
		t.Fatalf("preexisting type def should be preserved: %v", typeErr)
	}
}

// TestAddonManager_ConnectDisconnectReconnectPreservesSchema verifies
// that a successful Connect → Disconnect → failed reconnect does not
// deactivate schema activations that were live before the reconnect.
func TestAddonManager_ConnectDisconnectReconnectPreservesSchema(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	ctx := context.Background()

	router := delivery.NewRoutingDeliveryService()
	typeSvc := application.NewExtensionResourceTypeService(store)
	act := &recordingActivator{}
	mgr := application.NewAddonManager(application.AddonManagerDeps{
		Router: router, TypeSvc: typeSvc, Activator: act,
	})

	desc := clusterMgmtDescriptor()
	if err := mgr.Enable(ctx, desc); err != nil {
		t.Fatal(err)
	}

	target := domain.NewTargetInfo("kind-local", kindaddon.TargetType, "Local Kind",
		domain.TargetStateReady, nil, nil, []domain.ManifestType{"clusters"})

	// Step 1: successful Connect — schema activated + target created.
	if err := mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{clusterSchema()},
		Targets: []domain.TargetInfo{target},
	}); err != nil {
		t.Fatalf("first Connect: %v", err)
	}

	if act.deactivatedCount() != 0 {
		t.Fatalf("expected 0 deactivations after first Connect, got %d", act.deactivatedCount())
	}

	// Step 2: Disconnect — agent removed, schemas remain registered.
	if err := mgr.Disconnect(ctx, "test.fleetshift.io"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	// Step 3: reconnect with same schema but a drifted target (name
	// changed) so connectTargets fails with drift error.
	driftedTarget := domain.NewTargetInfo("kind-local", kindaddon.TargetType, "CHANGED NAME",
		domain.TargetStateReady, nil, nil, []domain.ManifestType{"clusters"})
	err := mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{clusterSchema()},
		Targets: []domain.TargetInfo{driftedTarget},
	})
	if err == nil {
		t.Fatal("expected reconnect to fail due to target drift")
	}

	// The preexisting schema activation must NOT have been
	// deactivated by rollback — it belonged to the first Connect,
	// not this failed one.
	if act.deactivatedCount() != 0 {
		t.Fatalf("expected 0 deactivations (preexisting schema must survive rollback), got %d", act.deactivatedCount())
	}

	// Type def should also survive.
	if _, typeErr := typeSvc.Get(ctx, "test.fleetshift.io/Cluster"); typeErr != nil {
		t.Fatalf("preexisting type def should be preserved: %v", typeErr)
	}
}

// failOnNthActivator delegates to an inner activator but returns an
// error on the Nth Activate call (1-indexed).
type failOnNthActivator struct {
	inner *recordingActivator
	failN int
	count int
}

func (f *failOnNthActivator) Activate(ctx context.Context, schema domain.ExtensionResourceSchema) (application.SchemaActivationID, error) {
	f.count++
	if f.count == f.failN {
		return "", fmt.Errorf("injected activation failure")
	}
	return f.inner.Activate(ctx, schema)
}

func (f *failOnNthActivator) Deactivate(id application.SchemaActivationID) {
	f.inner.Deactivate(id)
}

func TestAddonManager_DisableDeactivatesInventoryOnlyTypeDef(t *testing.T) {
	env := setupAddonManager(t)
	ctx := context.Background()

	if err := env.mgr.Enable(ctx, inventoryOnlyDescriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if err := env.mgr.Connect(ctx, "test.fleetshift.io", application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{inventoryOnlySchema()},
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if err := env.mgr.Disable(ctx, "test.fleetshift.io"); err != nil {
		t.Fatalf("Disable: %v", err)
	}

	addon, _ := env.mgr.Get("test.fleetshift.io")
	if addon.State != domain.AddonStateDefined {
		t.Errorf("state = %d, want %d (defined)", addon.State, domain.AddonStateDefined)
	}

	// Type def should be deleted.
	_, err := env.typeSvc.Get(ctx, "test.fleetshift.io/Node")
	if err == nil {
		t.Error("expected type def to be deleted after Disable")
	}
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
}
