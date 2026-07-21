package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type stubDeleteManagedResourceWorkflow struct {
	started *bool
	result  domain.ExtensionResourceView
}

func (w *stubDeleteManagedResourceWorkflow) Start(
	_ context.Context,
	_ domain.DeleteManagedResourceInput,
) (domain.Execution[domain.ExtensionResourceView], error) {
	*w.started = true
	return &immediateERExecution{val: w.result}, nil
}

type immediateERExecution struct {
	val domain.ExtensionResourceView
}

func (e *immediateERExecution) WorkflowID() string { return "stub-delete-mr" }
func (e *immediateERExecution) AwaitResult(_ context.Context) (domain.ExtensionResourceView, error) {
	return e.val, nil
}

func seedClusterTypeForDelete(t *testing.T, store domain.Store, rt domain.ResourceType, now time.Time) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()

	typeDef := domain.NewExtensionResourceType(
		rt, "v1", "clusters", now,
		domain.WithManagement(
			domain.NewRegisteredSelfTarget("kind-local", "api.kind.cluster"),
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
// fails before starting the delete workflow when the extension resource
// exists but has no managed state (e.g. seeded only by inventory
// reporting from a deployment-created kind cluster).
func TestDeleteExtensionResource_RejectsInventoryOnlyInstance(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	fixed := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)

	rt := domain.ResourceType("kind.fleetshift.io/Cluster")
	name := domain.ResourceName("clusters/c1")
	seedClusterTypeForDelete(t, store, rt, fixed)

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Inventory-only instance: same shape inventory reporting creates
	// when a deployment delivers a kind cluster.
	er := domain.NewExtensionResource(domain.NewExtensionResourceUID(), rt, name, fixed)
	if err := tx.ExtensionResources().Create(ctx, er); err != nil {
		t.Fatalf("Create extension resource: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	started := false
	svc := application.NewExtensionResourceService(
		store,
		nil,
		&stubDeleteManagedResourceWorkflow{started: &started},
		nil,
		nil,
	)

	_, err = svc.Delete(ctx, rt, name)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("Delete err = %v, want ErrInvalidArgument", err)
	}
	if started {
		t.Fatal("delete workflow should not start for inventory-only instance")
	}
}

func TestDeleteExtensionResource_StartsWorkflowWhenManaged(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	fixed := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)

	rt := domain.ResourceType("kind.fleetshift.io/Cluster")
	name := domain.ResourceName("clusters/c1")
	seedClusterTypeForDelete(t, store, rt, fixed)

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	fID := domain.FulfillmentID("f-managed")
	if err := tx.Fulfillments().Create(ctx, domain.NewFulfillment(fID, domain.DeliveryAuth{}, nil, nil, fixed)); err != nil {
		t.Fatalf("Create fulfillment: %v", err)
	}
	er := domain.NewExtensionResource(
		domain.NewExtensionResourceUID(), rt, name, fixed,
		domain.WithManagedState(fID),
	)
	if _, err := er.RecordIntent([]byte(`{"name":"c1"}`), fixed); err != nil {
		t.Fatalf("RecordIntent: %v", err)
	}
	if err := tx.ExtensionResources().Create(ctx, er); err != nil {
		t.Fatalf("Create extension resource: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	started := false
	svc := application.NewExtensionResourceService(
		store,
		nil,
		&stubDeleteManagedResourceWorkflow{started: &started},
		nil,
		nil,
	)

	if _, err := svc.Delete(ctx, rt, name); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !started {
		t.Fatal("delete workflow should start for managed instance")
	}
}
