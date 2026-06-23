package goworkflows_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	wfsqlite "github.com/cschleiden/go-workflows/backend/sqlite"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/workflow"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/goworkflows"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

type deleteCapturingDelivery struct {
	mu           sync.Mutex
	reporter     domain.DeliveryReporter
	removeAuth   domain.DeliveryAuth
	removeEvents []domain.DeliveryEvent
	removeDone   chan struct{}
	allowRemove  chan struct{}
}

func newDeleteCapturingDelivery() *deleteCapturingDelivery {
	return &deleteCapturingDelivery{
		removeDone:  make(chan struct{}),
		allowRemove: make(chan struct{}),
	}
}

func (d *deleteCapturingDelivery) Deliver(
	ctx context.Context,
	_ domain.TargetInfo,
	deliveryID domain.DeliveryID,
	_ []domain.Manifest,
	_ domain.DeliveryAuth,
	_ *domain.Attestation,
	gen domain.Generation,
) error {
	if d.reporter != nil {
		_ = d.reporter.ReportResult(ctx, deliveryID, gen, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
	}
	return nil
}

func (d *deleteCapturingDelivery) Remove(
	ctx context.Context,
	_ domain.TargetInfo,
	deliveryID domain.DeliveryID,
	_ []domain.Manifest,
	auth domain.DeliveryAuth,
	_ *domain.Attestation,
	gen domain.Generation,
) error {
	event := domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   "removing managed resource",
	}

	d.mu.Lock()
	d.removeAuth = auth
	d.removeEvents = append(d.removeEvents, event)
	d.mu.Unlock()

	select {
	case <-d.removeDone:
	default:
		close(d.removeDone)
	}
	select {
	case <-d.allowRemove:
		if d.reporter != nil {
			_ = d.reporter.ReportResult(ctx, deliveryID, gen, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *deleteCapturingDelivery) snapshot() (domain.DeliveryAuth, []domain.DeliveryEvent) {
	d.mu.Lock()
	defer d.mu.Unlock()
	events := make([]domain.DeliveryEvent, len(d.removeEvents))
	copy(events, d.removeEvents)
	return d.removeAuth, events
}

func awaitGoFulfillmentState(
	ctx context.Context,
	t *testing.T,
	store domain.Store,
	id domain.FulfillmentID,
	want domain.FulfillmentState,
) {
	t.Helper()
	for {
		tx, err := store.BeginReadOnly(ctx)
		if err != nil {
			t.Fatalf("begin read tx: %v", err)
		}
		f, err := tx.Fulfillments().Get(ctx, id)
		_ = tx.Rollback()
		if err == nil && f.State() == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for fulfillment %s to reach %q", id, want)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func awaitGoFulfillmentGone(
	ctx context.Context,
	t *testing.T,
	store domain.Store,
	id domain.FulfillmentID,
) {
	t.Helper()
	for {
		tx, err := store.BeginReadOnly(ctx)
		if err != nil {
			t.Fatalf("begin read tx: %v", err)
		}
		_, err = tx.Fulfillments().Get(ctx, id)
		_ = tx.Rollback()
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

func TestManagedResourceDelete_GoWorkflows_UsesDeleteAuthAndEmitsRemoveEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	deliveryAgent := newDeleteCapturingDelivery()
	router := delivery.NewRoutingDeliveryService()
	router.Register("addon", deliveryAgent)

	backend := wfsqlite.NewInMemoryBackend()
	worker := startWorker(t, backend)
	reg := &goworkflows.Registry{
		Worker:  worker,
		Client:  client.New(backend),
		Timeout: 10 * time.Second,
		ActivityOptions: &workflow.ActivityOptions{
			RetryOptions: workflow.RetryOptions{
				MaxAttempts:        3,
				FirstRetryInterval: 1 * time.Millisecond,
				MaxRetryInterval:   5 * time.Millisecond,
				BackoffCoefficient: 2,
			},
		},
	}

	deliveryReporter := application.NewDeliveryReportService(store, reg)
	deliveryAgent.reporter = deliveryReporter
	orchSpec := domain.NewOrchestrationWorkflowSpec(
		store, router, domain.StrategyFactory{Store: store}, reg,
		domain.WithAckRetryInterval(5*time.Second),
	)
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	createWf, err := reg.RegisterCreateManagedResource(&domain.CreateManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
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
	deleteWf, err := reg.RegisterDeleteManagedResource(&domain.DeleteManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		Cleanup:       cleanupWf,
	})
	if err != nil {
		t.Fatalf("RegisterDeleteManagedResource: %v", err)
	}

	typeSvc := application.NewManagedResourceTypeService(store)
	resourceSvc := &application.ManagedResourceService{
		Store:    store,
		CreateWF: createWf,
		DeleteWF: deleteWf,
	}

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := tx.Targets().Create(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:                    "addon-cluster-mgmt",
		Name:                  "Cluster Management Addon",
		Type:                  "addon",
		AcceptedManifestTypes: []domain.ManifestType{"clusters"},
	})); err != nil {
		t.Fatalf("create target: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit target: %v", err)
	}

	if _, err := typeSvc.Create(ctx, application.CreateTypeInput{
		ResourceType:   "test.fleetshift.io/Cluster",
		Relation:       domain.NewRegisteredSelfTarget("addon-cluster-mgmt", "clusters"),
		Signature:      domain.Signature{},
		APIServiceName: "kind.fleetshift.io",
		APIVersion:     "v1",
		CollectionID:   "clusters",
	}); err != nil {
		t.Fatalf("CreateType: %v", err)
	}

	createCtx := application.ContextWithAuth(ctx, &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "creator",
				Issuer:  "https://issuer.example/create",
			},
		},
		Token: "create-token",
	})
	view, err := resourceSvc.Create(createCtx, application.CreateManagedResourceInput{
		ResourceType: "test.fleetshift.io/Cluster",
		Name:         "clusters/prod-us-east-1",
		Spec:         json.RawMessage(`{"provider":"rosa","version":"4.16.2"}`),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	awaitGoFulfillmentState(ctx, t, store, view.Fulfillment.ID(), domain.FulfillmentStateActive)

	deleteCtx := application.ContextWithAuth(ctx, &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "deleter",
				Issuer:  "https://issuer.example/delete",
			},
		},
		Token: "delete-token",
	})
	if _, err := resourceSvc.Delete(deleteCtx, "test.fleetshift.io/Cluster", "clusters/prod-us-east-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	select {
	case <-deliveryAgent.removeDone:
	case <-ctx.Done():
		t.Fatal("timed out waiting for remove to run")
	}
	defer func() {
		select {
		case <-deliveryAgent.allowRemove:
		default:
			close(deliveryAgent.allowRemove)
		}
	}()

	gotAuth, gotRemoveEvents := deliveryAgent.snapshot()
	if gotAuth.Token != "delete-token" {
		t.Fatalf("Remove auth token = %q, want %q", gotAuth.Token, "delete-token")
	}
	if gotAuth.Caller == nil || gotAuth.Caller.Subject != "deleter" {
		t.Fatalf("Remove caller = %+v, want subject deleter", gotAuth.Caller)
	}
	if len(gotRemoveEvents) == 0 {
		t.Fatal("expected remove path to emit at least one event")
	}

	viewDuringDelete, err := resourceSvc.Get(ctx, "test.fleetshift.io/Cluster", "clusters/prod-us-east-1")
	if err != nil {
		t.Fatalf("Get during delete: %v", err)
	}
	if viewDuringDelete.Fulfillment.State() != domain.FulfillmentStateDeleting {
		t.Fatalf("Get during delete state = %q, want deleting", viewDuringDelete.Fulfillment.State())
	}

	close(deliveryAgent.allowRemove)
	awaitGoFulfillmentGone(ctx, t, store, view.Fulfillment.ID())
}
