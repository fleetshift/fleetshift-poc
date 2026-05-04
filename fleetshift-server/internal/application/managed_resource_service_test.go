package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/jsonschema"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

type mrTestHarness struct {
	typeSvc     *application.ManagedResourceTypeService
	resourceSvc *application.ManagedResourceService
	store       domain.Store
}

func setupManagedResources(t *testing.T) mrTestHarness {
	t.Helper()
	store := newStore(t)
	reg := &memworkflow.Registry{}

	agent := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC) },
	}

	orchSpec := &domain.OrchestrationWorkflowSpec{
		Store:      store,
		Delivery:   agent,
		Strategies: domain.StrategyFactory{Store: store},
		Registry:   reg,
	}
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	createWfSpec := &domain.CreateManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	}
	createWf, err := reg.RegisterCreateManagedResource(createWfSpec)
	if err != nil {
		t.Fatalf("RegisterCreateManagedResource: %v", err)
	}

	cleanupSpec := &domain.DeleteManagedResourceCleanupWorkflowSpec{
		Store: store,
	}
	cleanupWf, err := reg.RegisterDeleteManagedResourceCleanup(cleanupSpec)
	if err != nil {
		t.Fatalf("RegisterDeleteManagedResourceCleanup: %v", err)
	}

	deleteWfSpec := &domain.DeleteManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		Cleanup:       cleanupWf,
	}
	deleteWf, err := reg.RegisterDeleteManagedResource(deleteWfSpec)
	if err != nil {
		t.Fatalf("RegisterDeleteManagedResource: %v", err)
	}

	return mrTestHarness{
		typeSvc: &application.ManagedResourceTypeService{
			Store:          store,
			SchemaCompiler: jsonschema.Compiler{},
		},
		resourceSvc: &application.ManagedResourceService{
			Store:          store,
			SchemaCompiler: jsonschema.Compiler{},
			CreateWF:       createWf,
			DeleteWF:       deleteWf,
		},
		store: store,
	}
}

func TestManagedResourceService_CreateReadDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h := setupManagedResources(t)

	// First register a target so placement can resolve.
	{
		tx, _ := h.store.Begin(ctx)
		_ = tx.Targets().Create(ctx, domain.TargetInfo{
			ID:   "addon-cluster-mgmt",
			Name: "Cluster Addon",
			Type: "test",
			Properties: map[string]string{
				"foo": "bar",
			},
			AcceptedResourceTypes: []domain.ResourceType{"clusters"},
		})
		_ = tx.Commit()
	}

	schema := domain.RawSchema(`{"type":"object","properties":{"provider":{"type":"string"},"version":{"type":"string"}},"required":["provider"]}`)

	// Register a type.
	_, err := h.typeSvc.Create(ctx, application.CreateTypeInput{
		ResourceType: "clusters",
		Relation:     domain.RegisteredSelfTarget{AddonTarget: "addon-cluster-mgmt"},
		Signature: domain.Signature{
			Signer:         domain.FederatedIdentity{Subject: "addon-svc", Issuer: "https://issuer.test"},
			ContentHash:    []byte("hash"),
			SignatureBytes: []byte("sig"),
		},
		SpecSchema: &schema,
	})
	if err != nil {
		t.Fatalf("RegisterType: %v", err)
	}

	// Create with valid spec.
	view, err := h.resourceSvc.Create(ctx, application.CreateManagedResourceInput{
		ResourceType: "clusters",
		Name:         "prod-us-east-1",
		Spec:         json.RawMessage(`{"provider":"rosa","version":"4.16.2"}`),
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
	if view.Fulfillment.State != domain.FulfillmentStateCreating {
		t.Errorf("Fulfillment.State = %q, want %q", view.Fulfillment.State, domain.FulfillmentStateCreating)
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
	if view.Fulfillment.PlacementStrategy.Targets[0] != "addon-cluster-mgmt" {
		t.Errorf("PlacementStrategy.Targets[0] = %q, want %q", view.Fulfillment.PlacementStrategy.Targets[0], "addon-cluster-mgmt")
	}

	awaitFulfillmentState(ctx, t, h.store, view.Fulfillment.ID, domain.FulfillmentStateActive)

	// Get the resource.
	got, err := h.resourceSvc.Get(ctx, "clusters", "prod-us-east-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ManagedResource.UID != view.ManagedResource.UID {
		t.Errorf("Get UID = %q, want %q", got.ManagedResource.UID, view.ManagedResource.UID)
	}

	// List resources.
	views, err := h.resourceSvc.List(ctx, "clusters")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(views) != 1 {
		t.Fatalf("List len = %d, want 1", len(views))
	}

	// Delete.
	deleted, err := h.resourceSvc.Delete(ctx, "clusters", "prod-us-east-1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify gone.
	_, err = h.resourceSvc.Get(ctx, "clusters", "prod-us-east-1")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Get after delete: got %v, want ErrNotFound", err)
	}

	awaitFulfillmentGone(ctx, t, h.store, deleted.Fulfillment.ID)
}

func TestManagedResourceService_CreateInvalidSpec(t *testing.T) {
	ctx := context.Background()
	h := setupManagedResources(t)

	schema := domain.RawSchema(`{"type":"object","required":["provider"]}`)

	_, err := h.typeSvc.Create(ctx, application.CreateTypeInput{
		ResourceType: "clusters",
		Relation:     domain.RegisteredSelfTarget{AddonTarget: "addon"},
		Signature: domain.Signature{
			Signer:         domain.FederatedIdentity{Subject: "s", Issuer: "i"},
			ContentHash:    []byte("h"),
			SignatureBytes: []byte("s"),
		},
		SpecSchema: &schema,
	})
	if err != nil {
		t.Fatalf("RegisterType: %v", err)
	}

	// Invalid spec (missing required "provider" field).
	_, err = h.resourceSvc.Create(ctx, application.CreateManagedResourceInput{
		ResourceType: "clusters",
		Name:         "bad-spec",
		Spec:         json.RawMessage(`{"version":"4.16.2"}`),
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("Create with invalid spec: got %v, want ErrInvalidArgument", err)
	}
}

func TestManagedResourceService_CreateTypeNotFound(t *testing.T) {
	ctx := context.Background()
	h := setupManagedResources(t)

	_, err := h.resourceSvc.Create(ctx, application.CreateManagedResourceInput{
		ResourceType: "nonexistent",
		Name:         "x",
		Spec:         json.RawMessage(`{}`),
	})
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Create with unknown type: got %v, want ErrNotFound", err)
	}
}

func awaitFulfillmentGone(ctx context.Context, t *testing.T, store domain.Store, id domain.FulfillmentID) {
	t.Helper()
	for {
		tx, err := store.BeginReadOnly(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		_, err = tx.Fulfillments().Get(ctx, id)
		tx.Rollback()
		if errors.Is(err, domain.ErrNotFound) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for fulfillment %s to be deleted", id)
		case <-time.After(5 * time.Millisecond):
		}
	}
}
