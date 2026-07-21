package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

const (
	deletePreflightResourceType domain.ResourceType = "test.fleetshift.io/Widget"
	deletePreflightManifestType domain.ManifestType = "api.test.widget"
	deletePreflightTargetType   domain.TargetType   = "test"
	deletePreflightTargetID     domain.TargetID     = "addon-local"
)

type extensionResourceHarness struct {
	store domain.Store
	svc   *application.ExtensionResourceService
}

func setupExtensionResourceService(t *testing.T) extensionResourceHarness {
	t.Helper()
	store := newStore(t)
	fixed := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)

	agent := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return fixed },
	}
	router := delivery.NewRoutingDeliveryService()
	router.Register(deletePreflightTargetType, agent)

	reg := &memworkflow.Registry{}
	agent.Reporter = application.NewDeliveryReportService(store, reg)

	orchWf, err := reg.RegisterOrchestration(domain.NewOrchestrationWorkflowSpec(
		store, router, domain.StrategyFactory{Store: store}, reg,
		domain.WithAckRetryInterval(5*time.Second),
	))
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}
	createMRWf, err := reg.RegisterCreateManagedResource(&domain.CreateManagedResourceWorkflowSpec{
		Store: store, Orchestration: orchWf, Now: func() time.Time { return fixed },
	})
	if err != nil {
		t.Fatalf("RegisterCreateManagedResource: %v", err)
	}
	cleanupWf, err := reg.RegisterDeleteManagedResourceCleanup(&domain.DeleteManagedResourceCleanupWorkflowSpec{
		Store: store,
	})
	if err != nil {
		t.Fatalf("RegisterDeleteManagedResourceCleanup: %v", err)
	}
	deleteMRWf, err := reg.RegisterDeleteManagedResource(&domain.DeleteManagedResourceWorkflowSpec{
		Store: store, Orchestration: orchWf, Cleanup: cleanupWf, Now: func() time.Time { return fixed },
	})
	if err != nil {
		t.Fatalf("RegisterDeleteManagedResource: %v", err)
	}

	ctx := context.Background()
	if err := (&application.TargetService{Store: store}).Register(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:                    deletePreflightTargetID,
		Type:                  deletePreflightTargetType,
		Name:                  "Test Addon",
		AcceptedManifestTypes: []domain.ManifestType{deletePreflightManifestType},
	})); err != nil {
		t.Fatalf("Register target: %v", err)
	}

	return extensionResourceHarness{
		store: store,
		svc:   application.NewExtensionResourceService(store, createMRWf, deleteMRWf, nil, nil),
	}
}

func seedManagedPlusInventoryTypeForDelete(t *testing.T, store domain.Store, now time.Time) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()

	typeDef := domain.NewExtensionResourceType(
		deletePreflightResourceType, "v1", "widgets", now,
		domain.WithManagement(
			domain.NewRegisteredSelfTarget(deletePreflightTargetID, deletePreflightManifestType),
			domain.Signature{},
		),
		domain.WithInventory(),
	)
	if err := tx.ExtensionResources().CreateType(ctx, typeDef); err != nil {
		t.Fatalf("CreateType: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// TestDeleteExtensionResource_RejectsInventoryOnlyInstance ensures Delete
// fails before starting the delete workflow when an instance of a
// managed+inventory type exists without managed state. That happens when
// inventory reporting resolve-or-creates a row for a resource that was
// never created through the managed-resource API (for example, a custom
// Deployment that materializes the same type the addon also manages).
func TestDeleteExtensionResource_RejectsInventoryOnlyInstance(t *testing.T) {
	h := setupExtensionResourceService(t)
	ctx := context.Background()
	fixed := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)

	name := domain.ResourceName("widgets/w1")
	seedManagedPlusInventoryTypeForDelete(t, h.store, fixed)

	ready, err := domain.NewCondition(
		"Ready", domain.ConditionTrue, "Available", "ready", fixed)
	if err != nil {
		t.Fatalf("NewCondition: %v", err)
	}
	obs := json.RawMessage(`{"status":"ok"}`)
	reporter := application.NewInventoryReporterAdapter(application.NewInventoryReportService(h.store))
	if err := reporter.ApplyDeltaBatch(ctx, domain.InventoryDeltaBatch{
		Reports: []domain.InventoryDeltaReport{{
			ResourceType:      deletePreflightResourceType,
			Name:              name,
			ReplaceConditions: []domain.Condition{ready},
			Observation:       &obs,
			ObservedAt:        fixed,
		}},
	}); err != nil {
		t.Fatalf("ApplyDeltaBatch: %v", err)
	}

	_, err = h.svc.Delete(ctx, deletePreflightResourceType, name)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("Delete err = %v, want ErrInvalidArgument", err)
	}
	// Workflow path wraps as "mutate to deleting: …"; preflight must not.
	if strings.Contains(err.Error(), "mutate to deleting") {
		t.Fatalf("Delete reached workflow: %v", err)
	}
}

func TestDeleteExtensionResource_StartsWorkflowWhenManaged(t *testing.T) {
	h := setupExtensionResourceService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fixed := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)

	name := domain.ResourceName("widgets/w1")
	seedManagedPlusInventoryTypeForDelete(t, h.store, fixed)

	if _, err := h.svc.Create(ctx, application.CreateExtensionResourceInput{
		ResourceType: deletePreflightResourceType,
		Name:         name,
		Spec:         json.RawMessage(`{"name":"w1"}`),
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	view, err := h.svc.Delete(ctx, deletePreflightResourceType, name)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if view.Fulfillment == nil {
		t.Fatal("expected fulfillment on delete snapshot")
	}
	if view.Fulfillment.State() != domain.FulfillmentStateDeleting {
		t.Fatalf("Fulfillment.State = %q, want deleting", view.Fulfillment.State())
	}
}
