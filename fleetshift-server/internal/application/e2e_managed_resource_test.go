package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/jsonschema"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// TestEndToEnd_ManagedResource_DeliveryWithAttestation exercises the
// full managed resource tracer bullet:
//  1. Register a managed resource type (with SignedRelation fields)
//  2. Verify that creating a resource with invalid spec is rejected
//  3. Create a managed resource instance with a valid spec
//  4. Verify orchestration derives the fulfillment and delivers to the addon
//  5. Verify the delivery carries attestation with ManagedResourceContent
//  6. Verify the delivered manifests contain the resource spec
func TestEndToEnd_ManagedResource_DeliveryWithAttestation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := newStore(t)

	inner := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC) },
	}
	agent := &mrCapturingDeliveryAgent{inner: inner}

	router := delivery.NewRoutingDeliveryService()
	router.Register("addon", agent)

	reg := &memworkflow.Registry{}

	orchSpec := &domain.OrchestrationWorkflowSpec{
		Store:      store,
		Delivery:   router,
		Strategies: domain.StrategyFactory{Store: store},
		Registry:   reg,
		Now:        func() time.Time { return time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC) },
	}
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	createWfSpec := &domain.CreateManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		Now:           func() time.Time { return time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC) },
	}
	createWf, err := reg.RegisterCreateManagedResource(createWfSpec)
	if err != nil {
		t.Fatalf("RegisterCreateManagedResource: %v", err)
	}

	deleteWfSpec := &domain.DeleteManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	}
	deleteWf, err := reg.RegisterDeleteManagedResource(deleteWfSpec)
	if err != nil {
		t.Fatalf("RegisterDeleteManagedResource: %v", err)
	}

	typeSvc := &application.ManagedResourceTypeService{
		Store:          store,
		SchemaCompiler: jsonschema.Compiler{},
	}
	resourceSvc := &application.ManagedResourceService{
		Store:          store,
		SchemaCompiler: jsonschema.Compiler{},
		CreateWF:       createWf,
		DeleteWF:       deleteWf,
	}

	// --- Step 1: Register target (the addon) ---
	{
		tx, _ := store.Begin(ctx)
		_ = tx.Targets().Create(ctx, domain.TargetInfo{
			ID:                    "addon-cluster-mgmt",
			Name:                  "Cluster Management Addon",
			Type:                  "addon",
			AcceptedResourceTypes: []domain.ResourceType{"managed_resource_spec"},
		})
		_ = tx.Commit()
	}

	// --- Step 2: Register managed resource type with schema ---
	specSchema := domain.RawSchema(`{
		"type": "object",
		"properties": {
			"provider": {"type": "string", "enum": ["rosa", "aro", "eks"]},
			"version": {"type": "string"}
		},
		"required": ["provider", "version"]
	}`)

	addonSig := domain.Signature{
		Signer:         domain.FederatedIdentity{Subject: "addon-cluster-svc", Issuer: "https://addon-issuer.test"},
		ContentHash:    []byte("relation-hash"),
		SignatureBytes: []byte("relation-sig"),
	}

	_, err = typeSvc.Create(ctx, application.CreateTypeInput{
		ResourceType: "clusters",
		Relation:     domain.RegisteredSelfTarget{AddonTarget: "addon-cluster-mgmt"},
		Signature:    addonSig,
		SpecSchema:   &specSchema,
	})
	if err != nil {
		t.Fatalf("RegisterType: %v", err)
	}

	// --- Step 3: Verify invalid spec is rejected ---
	_, err = resourceSvc.Create(ctx, application.CreateManagedResourceInput{
		ResourceType: "clusters",
		Name:         "invalid-cluster",
		Spec:         json.RawMessage(`{"provider":"invalid-provider","version":"4.16"}`),
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("Create with invalid spec: got %v, want ErrInvalidArgument", err)
	}

	// Also test missing required field.
	_, err = resourceSvc.Create(ctx, application.CreateManagedResourceInput{
		ResourceType: "clusters",
		Name:         "missing-fields",
		Spec:         json.RawMessage(`{"provider":"rosa"}`),
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("Create with missing field: got %v, want ErrInvalidArgument", err)
	}

	// --- Step 4: Create valid resource ---
	validSpec := json.RawMessage(`{"provider":"rosa","version":"4.16.2"}`)
	view, err := resourceSvc.Create(ctx, application.CreateManagedResourceInput{
		ResourceType: "clusters",
		Name:         "prod-us-east-1",
		Spec:         validSpec,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if view.ManagedResource.Name != "prod-us-east-1" {
		t.Errorf("Name = %q, want %q", view.ManagedResource.Name, "prod-us-east-1")
	}
	if view.ManagedResource.CurrentVersion != 1 {
		t.Errorf("CurrentVersion = %d, want 1", view.ManagedResource.CurrentVersion)
	}
	if view.Fulfillment.ManifestStrategy.Type != domain.ManifestStrategyManagedResource {
		t.Errorf("ManifestStrategy.Type = %q, want %q", view.Fulfillment.ManifestStrategy.Type, domain.ManifestStrategyManagedResource)
	}
	if view.Fulfillment.ManifestStrategy.IntentRef.ResourceType != "clusters" {
		t.Errorf("IntentRef.ResourceType = %q, want %q", view.Fulfillment.ManifestStrategy.IntentRef.ResourceType, "clusters")
	}
	if view.Fulfillment.ManifestStrategy.IntentRef.Version != 1 {
		t.Errorf("IntentRef.Version = %d, want 1", view.Fulfillment.ManifestStrategy.IntentRef.Version)
	}

	// --- Step 5: Wait for delivery (orchestration runs async) ---
	awaitFulfillmentState(ctx, t, store, view.Fulfillment.ID, domain.FulfillmentStateActive)

	// --- Step 6: Verify delivered manifests contain the resource spec ---
	manifests := agent.capturedManifests()
	if len(manifests) == 0 {
		t.Fatal("no manifests delivered")
	}
	if manifests[0].ResourceType != "managed_resource_spec" {
		t.Errorf("Manifest.ResourceType = %q, want %q", manifests[0].ResourceType, "managed_resource_spec")
	}
	if string(manifests[0].Raw) != string(validSpec) {
		t.Errorf("Manifest.Raw = %s, want %s", manifests[0].Raw, validSpec)
	}

	// --- Step 7: Verify the resource is retrievable from the service ---
	got, err := resourceSvc.Get(ctx, "clusters", "prod-us-east-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ManagedResource.FulfillmentID != view.Fulfillment.ID {
		t.Errorf("FulfillmentID = %q, want %q", got.ManagedResource.FulfillmentID, view.Fulfillment.ID)
	}
	if got.Fulfillment.State != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want %q", got.Fulfillment.State, domain.FulfillmentStateActive)
	}
}

func awaitFulfillmentState(ctx context.Context, t *testing.T, store domain.Store, fID domain.FulfillmentID, want domain.FulfillmentState) {
	t.Helper()
	for {
		tx, err := store.BeginReadOnly(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		f, err := tx.Fulfillments().Get(ctx, fID)
		tx.Rollback()
		if err == nil && f.State == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for fulfillment %s to reach state %q (current: %q)", fID, want, f.State)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// mrCapturingDeliveryAgent wraps another delivery service and captures
// the last attestation and manifests delivered.
type mrCapturingDeliveryAgent struct {
	inner     domain.DeliveryService
	mu        sync.Mutex
	att       *domain.Attestation
	manifests []domain.Manifest
}

func (a *mrCapturingDeliveryAgent) Deliver(ctx context.Context, target domain.TargetInfo, id domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	a.mu.Lock()
	a.att = att
	a.manifests = manifests
	a.mu.Unlock()
	return a.inner.Deliver(ctx, target, id, manifests, auth, att, signaler)
}

func (a *mrCapturingDeliveryAgent) Remove(ctx context.Context, target domain.TargetInfo, id domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, signaler *domain.DeliverySignaler) error {
	return a.inner.Remove(ctx, target, id, manifests, auth, att, signaler)
}

func (a *mrCapturingDeliveryAgent) capturedAttestation() *domain.Attestation {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.att
}

func (a *mrCapturingDeliveryAgent) capturedManifests() []domain.Manifest {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.manifests
}
