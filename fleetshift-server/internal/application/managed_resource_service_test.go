package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/testutil"
)

type mrTestHarness struct {
	resourceSvc *application.ManagedResourceService
	store       domain.Store
}

func setupManagedResources(t *testing.T) mrTestHarness {
	return setupManagedResourcesWithDelivery(t, nil)
}

// registerTestAddon seeds a delivery target and managed resource type
// definition for the given resource type. This mirrors what
// [AddonManager.Connect] does in production (target + type def) without
// needing proto compilation or schema activation.
func registerTestAddon(t *testing.T, store domain.Store, resourceType domain.ResourceType) domain.TargetID {
	t.Helper()
	ctx := context.Background()

	targetID := domain.TargetID("test-addon-" + string(resourceType))

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("registerTestAddon: begin tx: %v", err)
	}
	defer tx.Rollback()

	if err := tx.Targets().Create(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:                    targetID,
		Name:                  "Test Addon (" + string(resourceType) + ")",
		Type:                  "test",
		AcceptedResourceTypes: []domain.ResourceType{resourceType},
	})); err != nil {
		t.Fatalf("registerTestAddon: create target: %v", err)
	}

	if err := tx.ManagedResources().CreateType(ctx, domain.ManagedResourceTypeDef{
		ResourceType: resourceType,
		Relation:     domain.RegisteredSelfTarget{AddonTarget: targetID},
		Signature: domain.Signature{
			Signer:         domain.FederatedIdentity{Subject: "test-addon-svc", Issuer: "https://test-addon.internal"},
			ContentHash:    []byte("test-hash"),
			SignatureBytes: []byte("test-sig"),
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("registerTestAddon: create type def: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("registerTestAddon: commit: %v", err)
	}
	return targetID
}

type blockingRemoveDeliveryService struct {
	inner    *sqlite.RecordingDeliveryService
	reporter domain.DeliveryReporter
	started  chan struct{}
	release  chan struct{}
}

func newBlockingRemoveDeliveryService(store domain.Store) *blockingRemoveDeliveryService {
	return &blockingRemoveDeliveryService{
		inner: &sqlite.RecordingDeliveryService{
			Store: store,
			Now:   func() time.Time { return time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC) },
		},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *blockingRemoveDeliveryService) Deliver(
	ctx context.Context,
	target domain.TargetInfo,
	deliveryID domain.DeliveryID,
	manifests []domain.Manifest,
	auth domain.DeliveryAuth,
	att *domain.Attestation,
	generation domain.Generation,
) error {
	if err := s.inner.Deliver(ctx, target, deliveryID, manifests, auth, att, generation); err != nil {
		return err
	}
	if s.reporter != nil {
		go func() {
			_ = s.reporter.ReportResult(context.Background(), deliveryID, generation, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
		}()
	}
	return nil
}

func (s *blockingRemoveDeliveryService) Remove(
	ctx context.Context,
	target domain.TargetInfo,
	deliveryID domain.DeliveryID,
	manifests []domain.Manifest,
	auth domain.DeliveryAuth,
	att *domain.Attestation,
	generation domain.Generation,
) error {
	if err := s.inner.Remove(ctx, target, deliveryID, manifests, auth, att, generation); err != nil {
		return err
	}
	select {
	case <-s.started:
	default:
		close(s.started)
	}
	select {
	case <-s.release:
		if s.reporter != nil {
			go func() {
				_ = s.reporter.ReportResult(context.Background(), deliveryID, generation, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
			}()
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func setupManagedResourcesWithDelivery(
	t *testing.T,
	buildDelivery func(store domain.Store, reporter domain.DeliveryReporter) domain.DeliveryAgent,
) mrTestHarness {
	t.Helper()
	store := newStore(t)
	reg := &memworkflow.Registry{}

	reporter := application.NewDeliveryReportService(store, reg)
	agent := domain.DeliveryAgent(&sqlite.RecordingDeliveryService{
		Store:    store,
		Reporter: reporter,
		Now:      func() time.Time { return time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC) },
	})
	if buildDelivery != nil {
		agent = buildDelivery(store, reporter)
	}

	orchSpec := domain.NewOrchestrationWorkflowSpec(
		store, agent, domain.StrategyFactory{Store: store}, reg,
		domain.WithAckRetryInterval(5*time.Second),
	)
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

	resumeWfSpec := &domain.ResumeManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		ProvenanceSvc: &domain.ProvenanceService{},
	}
	resumeWf, err := reg.RegisterResumeManagedResource(resumeWfSpec)
	if err != nil {
		t.Fatalf("RegisterResumeManagedResource: %v", err)
	}

	return mrTestHarness{
		resourceSvc: &application.ManagedResourceService{
			Store:    store,
			CreateWF: createWf,
			DeleteWF: deleteWf,
			ResumeWF: resumeWf,
		},
		store: store,
	}
}

func TestManagedResourceService_CreateReadDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testutil.ServiceTimeout)
	defer cancel()
	h := setupManagedResources(t)

	targetID := registerTestAddon(t, h.store, "clusters")

	// Create with valid spec.
	view, err := h.resourceSvc.Create(ctx, application.CreateManagedResourceInput{
		ResourceType: "clusters",
		Name:         "prod-us-east-1",
		Spec:         json.RawMessage(`{"provider":"rosa","version":"4.16.2"}`),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if view.ManagedResource.Name() != "prod-us-east-1" {
		t.Errorf("Name = %q, want %q", view.ManagedResource.Name(), "prod-us-east-1")
	}
	if view.ManagedResource.CurrentVersion() != 1 {
		t.Errorf("CurrentVersion = %d, want 1", view.ManagedResource.CurrentVersion())
	}
	if view.Fulfillment.State() != domain.FulfillmentStateCreating {
		t.Errorf("Fulfillment.State = %q, want %q", view.Fulfillment.State(), domain.FulfillmentStateCreating)
	}
	if view.Fulfillment.ManifestStrategy().Type != domain.ManifestStrategyManagedResource {
		t.Errorf("ManifestStrategy.Type = %q, want %q", view.Fulfillment.ManifestStrategy().Type, domain.ManifestStrategyManagedResource)
	}
	if view.Fulfillment.ManifestStrategy().IntentRef.ResourceType != "clusters" {
		t.Errorf("IntentRef.ResourceType = %q, want %q", view.Fulfillment.ManifestStrategy().IntentRef.ResourceType, "clusters")
	}
	if view.Fulfillment.ManifestStrategy().IntentRef.Version != 1 {
		t.Errorf("IntentRef.Version = %d, want 1", view.Fulfillment.ManifestStrategy().IntentRef.Version)
	}
	if view.Fulfillment.PlacementStrategy().Targets[0] != targetID {
		t.Errorf("PlacementStrategy.Targets[0] = %q, want %q", view.Fulfillment.PlacementStrategy().Targets[0], targetID)
	}

	awaitFulfillmentState(ctx, t, h.store, view.Fulfillment.ID(), domain.FulfillmentStateActive)

	// Get the resource.
	got, err := h.resourceSvc.Get(ctx, "clusters", "prod-us-east-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ManagedResource.UID() != view.ManagedResource.UID() {
		t.Errorf("Get UID = %q, want %q", got.ManagedResource.UID(), view.ManagedResource.UID())
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
	if deleted.Fulfillment.State() != domain.FulfillmentStateDeleting {
		t.Fatalf("Delete state = %q, want deleting", deleted.Fulfillment.State())
	}

	awaitFulfillmentGone(ctx, t, h.store, deleted.Fulfillment.ID())

	// Verify gone after cleanup completes.
	_, err = h.resourceSvc.Get(ctx, "clusters", "prod-us-east-1")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Get after delete: got %v, want ErrNotFound", err)
	}
}

func TestManagedResourceService_DeleteKeepsResourceVisibleDuringCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testutil.ServiceTimeout)
	defer cancel()

	var blocker *blockingRemoveDeliveryService
	h := setupManagedResourcesWithDelivery(t, func(store domain.Store, reporter domain.DeliveryReporter) domain.DeliveryAgent {
		blocker = newBlockingRemoveDeliveryService(store)
		blocker.reporter = reporter
		return blocker
	})

	registerTestAddon(t, h.store, "clusters")

	view, err := h.resourceSvc.Create(ctx, application.CreateManagedResourceInput{
		ResourceType: "clusters",
		Name:         "prod-us-east-1",
		Spec:         json.RawMessage(`{"provider":"rosa","version":"4.16.2"}`),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	awaitFulfillmentState(ctx, t, h.store, view.Fulfillment.ID(), domain.FulfillmentStateActive)

	deleted, err := h.resourceSvc.Delete(ctx, "clusters", "prod-us-east-1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleted.Fulfillment.State() != domain.FulfillmentStateDeleting {
		t.Fatalf("Delete state = %q, want deleting", deleted.Fulfillment.State())
	}

	select {
	case <-blocker.started:
	case <-ctx.Done():
		t.Fatal("timed out waiting for remove to start")
	}

	got, err := h.resourceSvc.Get(ctx, "clusters", "prod-us-east-1")
	if err != nil {
		t.Fatalf("Get during delete: %v", err)
	}
	if got.Fulfillment.State() != domain.FulfillmentStateDeleting {
		t.Fatalf("Get state during delete = %q, want deleting", got.Fulfillment.State())
	}

	views, err := h.resourceSvc.List(ctx, "clusters")
	if err != nil {
		t.Fatalf("List during delete: %v", err)
	}
	if len(views) != 1 {
		t.Fatalf("List during delete len = %d, want 1", len(views))
	}
	if views[0].Fulfillment.State() != domain.FulfillmentStateDeleting {
		t.Fatalf("List state during delete = %q, want deleting", views[0].Fulfillment.State())
	}

	close(blocker.release)
	awaitFulfillmentGone(ctx, t, h.store, deleted.Fulfillment.ID())
}

func TestManagedResourceService_DeleteAllowsRecreateSameName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testutil.ServiceTimeout)
	defer cancel()
	h := setupManagedResources(t)

	registerTestAddon(t, h.store, "clusters")

	first, err := h.resourceSvc.Create(ctx, application.CreateManagedResourceInput{
		ResourceType: "clusters",
		Name:         "prod-us-east-1",
		Spec:         json.RawMessage(`{"provider":"rosa","version":"4.16.2"}`),
	})
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}

	awaitFulfillmentState(ctx, t, h.store, first.Fulfillment.ID(), domain.FulfillmentStateActive)

	deleted, err := h.resourceSvc.Delete(ctx, "clusters", "prod-us-east-1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	awaitFulfillmentGone(ctx, t, h.store, deleted.Fulfillment.ID())

	recreated, err := h.resourceSvc.Create(ctx, application.CreateManagedResourceInput{
		ResourceType: "clusters",
		Name:         "prod-us-east-1",
		Spec:         json.RawMessage(`{"provider":"rosa","version":"4.17.0"}`),
	})
	if err != nil {
		t.Fatalf("recreate after delete: %v", err)
	}
	if recreated.ManagedResource.CurrentVersion() != 1 {
		t.Fatalf("recreated CurrentVersion = %d, want 1", recreated.ManagedResource.CurrentVersion())
	}
}

func TestManagedResourceService_Resume_PausedAuth_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := newStore(t)
	reg := &memworkflow.Registry{}
	reporter := application.NewDeliveryReportService(store, reg)

	agent := &authFailThenSucceedAgent{reporter: reporter}

	orchSpec := domain.NewOrchestrationWorkflowSpec(
		store, agent, domain.StrategyFactory{Store: store}, reg,
		domain.WithAckRetryInterval(5*time.Second),
	)
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

	resumeWfSpec := &domain.ResumeManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		ProvenanceSvc: &domain.ProvenanceService{},
	}
	resumeWf, err := reg.RegisterResumeManagedResource(resumeWfSpec)
	if err != nil {
		t.Fatalf("RegisterResumeManagedResource: %v", err)
	}

	resourceSvc := &application.ManagedResourceService{
		Store:    store,
		CreateWF: createWf,
		ResumeWF: resumeWf,
	}

	registerTestAddon(t, store, "clusters")

	// Create a managed resource with auth context (delivery will fail with auth error).
	createCtx := application.ContextWithAuth(ctx, &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-1", Issuer: "https://issuer.example.com"}},
		Token:   "expired-token",
	})

	view, err := resourceSvc.Create(createCtx, application.CreateManagedResourceInput{
		ResourceType: "clusters",
		Name:         "prod-us-east-1",
		Spec:         json.RawMessage(`{"provider":"rosa","version":"4.16.2"}`),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Wait for the fulfillment to pause due to auth failure.
	awaitFulfillmentPaused(ctx, t, store, view.Fulfillment.ID())

	// Resume with fresh credentials.
	resumeCtx := application.ContextWithAuth(ctx, &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-1", Issuer: "https://issuer.example.com"}},
		Token:   "fresh-token",
	})

	resumed, err := resourceSvc.Resume(resumeCtx, application.ResumeManagedResourceInput{
		ResourceType: "clusters",
		Name:         "prod-us-east-1",
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}

	// The returned view is a snapshot from mutation time (before
	// orchestration converges), so verify auth was updated.
	if resumed.Fulfillment.Auth().Token != "fresh-token" {
		t.Errorf("Auth.Token = %q, want %q", resumed.Fulfillment.Auth().Token, "fresh-token")
	}
	if resumed.ManagedResource.Name() != "prod-us-east-1" {
		t.Errorf("Name = %q, want %q", resumed.ManagedResource.Name(), "prod-us-east-1")
	}

	// After the workflow completes (convergence loop returned), the
	// fulfillment in the store should be active.
	awaitFulfillmentState(ctx, t, store, view.Fulfillment.ID(), domain.FulfillmentStateActive)
}

func TestManagedResourceService_Resume_NotPaused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h := setupManagedResources(t)

	registerTestAddon(t, h.store, "clusters")

	// Create a resource that succeeds (becomes active, not paused).
	view, err := h.resourceSvc.Create(ctx, application.CreateManagedResourceInput{
		ResourceType: "clusters",
		Name:         "prod-us-east-1",
		Spec:         json.RawMessage(`{"provider":"rosa","version":"4.16.2"}`),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	awaitFulfillmentState(ctx, t, h.store, view.Fulfillment.ID(), domain.FulfillmentStateActive)

	// Attempt to resume an active resource — should fail.
	resumeCtx := application.ContextWithAuth(ctx, &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-1", Issuer: "https://issuer.example.com"}},
		Token:   "token",
	})
	_, err = h.resourceSvc.Resume(resumeCtx, application.ResumeManagedResourceInput{
		ResourceType: "clusters",
		Name:         "prod-us-east-1",
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument for resume on active resource, got: %v", err)
	}
}

func TestManagedResourceService_Resume_NoAuth(t *testing.T) {
	h := setupManagedResources(t)

	_, err := h.resourceSvc.Resume(context.Background(), application.ResumeManagedResourceInput{
		ResourceType: "clusters",
		Name:         "prod-us-east-1",
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument for unauthenticated resume, got: %v", err)
	}
}

func TestManagedResourceService_Resume_NotFound(t *testing.T) {
	h := setupManagedResources(t)
	ctx := application.ContextWithAuth(context.Background(), &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-1", Issuer: "https://issuer.example.com"}},
		Token:   "token",
	})

	_, err := h.resourceSvc.Resume(ctx, application.ResumeManagedResourceInput{
		ResourceType: "clusters",
		Name:         "nonexistent",
	})
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for nonexistent resource, got: %v", err)
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

func TestManagedResourceService_DeletePausedAuth_RetryDeleteClearsPause(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h := setupManagedResourcesWithDelivery(t, func(store domain.Store, reporter domain.DeliveryReporter) domain.DeliveryAgent {
		return &removeAuthFailThenSucceedAgent{reporter: reporter}
	})
	registerTestAddon(t, h.store, "clusters")

	// Create with auth context (Deliver reports success via agent).
	authCtx := application.ContextWithAuth(ctx, &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-1", Issuer: "https://issuer.example.com"}},
		Token:   "expired-token",
	})

	view, err := h.resourceSvc.Create(authCtx, application.CreateManagedResourceInput{
		ResourceType: "clusters",
		Name:         "prod-1",
		Spec:         json.RawMessage(`{"provider":"rosa"}`),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	fID := view.Fulfillment.ID()

	awaitFulfillmentState(ctx, t, h.store, fID, domain.FulfillmentStateActive)

	// First delete — Remove reports auth failure → paused.
	_, err = h.resourceSvc.Delete(authCtx, "clusters", "prod-1")
	if err != nil {
		t.Fatalf("first Delete: %v", err)
	}

	awaitFulfillmentPaused(ctx, t, h.store, fID)

	// Retry delete with fresh credentials — should skip the early
	// return (paused), clear the pause, and re-run the workflow.
	freshCtx := application.ContextWithAuth(ctx, &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-1", Issuer: "https://issuer.example.com"}},
		Token:   "fresh-token",
	})

	retryView, err := h.resourceSvc.Delete(freshCtx, "clusters", "prod-1")
	if err != nil {
		t.Fatalf("retry Delete: %v", err)
	}
	if retryView.Fulfillment.State() != domain.FulfillmentStateDeleting {
		t.Fatalf("retry Delete state = %q, want deleting", retryView.Fulfillment.State())
	}
	if retryView.Fulfillment.Paused() {
		t.Error("fulfillment should not be paused after retry delete")
	}
	if retryView.Fulfillment.Auth().Token != "fresh-token" {
		t.Errorf("Auth.Token = %q, want %q", retryView.Fulfillment.Auth().Token, "fresh-token")
	}
}

func TestManagedResourceService_DeleteIdempotentWhileDeleting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testutil.ServiceTimeout)
	defer cancel()

	h := setupManagedResources(t)
	registerTestAddon(t, h.store, "clusters")

	authCtx := application.ContextWithAuth(ctx, &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-1", Issuer: "https://issuer.example.com"}},
		Token:   "token",
	})

	view, err := h.resourceSvc.Create(authCtx, application.CreateManagedResourceInput{
		ResourceType: "clusters",
		Name:         "prod-1",
		Spec:         json.RawMessage(`{"provider":"rosa"}`),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	awaitFulfillmentState(ctx, t, h.store, view.Fulfillment.ID(), domain.FulfillmentStateActive)

	dep1, err := h.resourceSvc.Delete(authCtx, "clusters", "prod-1")
	if err != nil {
		t.Fatalf("first Delete: %v", err)
	}
	if dep1.Fulfillment.State() != domain.FulfillmentStateDeleting {
		t.Fatalf("first Delete state = %q, want deleting", dep1.Fulfillment.State())
	}

	// Second delete while not paused should be idempotent (early return).
	dep2, err := h.resourceSvc.Delete(authCtx, "clusters", "prod-1")
	if err != nil {
		t.Fatalf("second Delete (idempotent): %v", err)
	}
	if dep2.Fulfillment.State() != domain.FulfillmentStateDeleting {
		t.Fatalf("second Delete state = %q, want deleting", dep2.Fulfillment.State())
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
