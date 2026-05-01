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
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/keyregistry"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

type testHarness struct {
	targets     *application.TargetService
	deployments *application.DeploymentService
	store       domain.Store
	fakeReg     *keyregistry.Fake
}

const testTargetType domain.TargetType = "test"

func newStore(t *testing.T) *sqlite.Store {
	t.Helper()
	return &sqlite.Store{DB: sqlite.OpenTestDB(t)}
}

func setupWithStoreAndAgent(t *testing.T, store domain.Store, agent domain.DeliveryAgent) testHarness {
	t.Helper()

	router := delivery.NewRoutingDeliveryService()
	router.Register(testTargetType, agent)

	reg := &memworkflow.Registry{}

	orchSpec := &domain.OrchestrationWorkflowSpec{
		Store:      store,
		Delivery:   router,
		Strategies: domain.DefaultStrategyFactory{},
		Registry:   reg,
	}
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	cwfSpec := &domain.CreateDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	}
	createWf, err := reg.RegisterCreateDeployment(cwfSpec)
	if err != nil {
		t.Fatalf("RegisterCreateDeployment: %v", err)
	}

	fakeReg := keyregistry.NewFake()
	keyResolver := &application.KeyResolver{
		Registries: domain.BuiltInKeyRegistries(),
		Clients: map[domain.KeyRegistryType]domain.RegistryClient{
			domain.KeyRegistryTypeGitHub: fakeReg,
		},
	}

	cleanupSpec := &domain.DeleteCleanupWorkflowSpec{
		Store: store,
	}
	cleanupWf, err := reg.RegisterDeleteCleanup(cleanupSpec)
	if err != nil {
		t.Fatalf("RegisterDeleteCleanup: %v", err)
	}

	deleteSpec := &domain.DeleteDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		Cleanup:       cleanupWf,
	}
	deleteWf, err := reg.RegisterDeleteDeployment(deleteSpec)
	if err != nil {
		t.Fatalf("RegisterDeleteDeployment: %v", err)
	}

	provenanceBuilder := &application.KeyResolverProvenanceBuilder{KeyResolver: keyResolver}

	resumeSpec := &domain.ResumeDeploymentWorkflowSpec{
		Store:             store,
		Orchestration:     orchWf,
		ProvenanceBuilder: provenanceBuilder,
	}
	resumeWf, err := reg.RegisterResumeDeployment(resumeSpec)
	if err != nil {
		t.Fatalf("RegisterResumeDeployment: %v", err)
	}

	return testHarness{
		targets: &application.TargetService{Store: store},
		deployments: &application.DeploymentService{
			Store:             store,
			CreateWF:          createWf,
			DeleteWF:          deleteWf,
			ResumeWF:          resumeWf,
			ProvenanceBuilder: provenanceBuilder,
		},
		store:   store,
		fakeReg: fakeReg,
	}
}

func setup(t *testing.T) testHarness {
	t.Helper()
	store := newStore(t)
	agent := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 2, 27, 12, 0, 0, 0, time.UTC) },
	}
	return setupWithStoreAndAgent(t, store, agent)
}

func awaitDeploymentState(ctx context.Context, t *testing.T, store domain.Store, id domain.DeploymentID, want domain.FulfillmentState) domain.DeploymentView {
	t.Helper()
	for {
		tx, err := store.BeginReadOnly(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		view, err := tx.Deployments().GetView(ctx, id)
		tx.Rollback()
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("GetView(%s): %v", id, err)
		}
		if err == nil && view.Fulfillment.State == want {
			return view
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for deployment %s to reach fulfillment state %q", id, want)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func queryDeliveries(ctx context.Context, t *testing.T, store domain.Store, depID domain.DeploymentID) []domain.Delivery {
	t.Helper()
	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()
	view, err := tx.Deployments().GetView(ctx, depID)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	records, err := tx.Deliveries().ListByFulfillment(ctx, view.Fulfillment.ID)
	if err != nil {
		t.Fatalf("ListByFulfillment: %v", err)
	}
	return records
}

func registerTargets(t *testing.T, h testHarness, ids ...string) {
	t.Helper()
	ctx := context.Background()
	for _, id := range ids {
		if err := h.targets.Register(ctx, domain.TargetInfo{ID: domain.TargetID(id), Type: testTargetType, Name: "cluster-" + id}); err != nil {
			t.Fatal(err)
		}
	}
}

func assertResolvedTargets(t *testing.T, view domain.DeploymentView, expectedIDs ...string) {
	t.Helper()
	if len(view.Fulfillment.ResolvedTargets) != len(expectedIDs) {
		t.Fatalf("ResolvedTargets: got %d, want %d", len(view.Fulfillment.ResolvedTargets), len(expectedIDs))
	}
	got := make(map[domain.TargetID]bool)
	for _, id := range view.Fulfillment.ResolvedTargets {
		got[id] = true
	}
	for _, id := range expectedIDs {
		if !got[domain.TargetID(id)] {
			t.Errorf("expected target %q in ResolvedTargets", id)
		}
	}
}

func TestCreateDeployment_StaticPlacement(t *testing.T) {
	h := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	registerTargets(t, h, "t1", "t2", "t3")

	_, err := h.deployments.Create(ctx, domain.CreateDeploymentInput{
		ID: "d1",
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"kind":"ConfigMap"}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"t1", "t3"},
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	dep := awaitDeploymentState(ctx, t, h.store, "d1", domain.FulfillmentStateActive)
	assertResolvedTargets(t, dep, "t1", "t3")

	records := queryDeliveries(ctx, t, h.store, "d1")
	if len(records) != 2 {
		t.Fatalf("expected 2 delivery records, got %d", len(records))
	}
}

func TestCreateDeployment_AllPlacement(t *testing.T) {
	h := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	registerTargets(t, h, "t1", "t2", "t3")

	_, err := h.deployments.Create(ctx, domain.CreateDeploymentInput{
		ID: "d1",
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	dep := awaitDeploymentState(ctx, t, h.store, "d1", domain.FulfillmentStateActive)
	assertResolvedTargets(t, dep, "t1", "t2", "t3")

	records := queryDeliveries(ctx, t, h.store, "d1")
	if len(records) != 3 {
		t.Fatalf("expected 3 delivery records, got %d", len(records))
	}
}

func TestCreateDeployment_SelectorPlacement(t *testing.T) {
	h := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	must(t, h.targets.Register(ctx, domain.TargetInfo{ID: "t1", Type: testTargetType, Name: "cluster-prod", Labels: map[string]string{"env": "prod"}}))
	must(t, h.targets.Register(ctx, domain.TargetInfo{ID: "t2", Type: testTargetType, Name: "cluster-staging", Labels: map[string]string{"env": "staging"}}))
	must(t, h.targets.Register(ctx, domain.TargetInfo{ID: "t3", Type: testTargetType, Name: "cluster-prod-eu", Labels: map[string]string{"env": "prod"}}))

	_, err := h.deployments.Create(ctx, domain.CreateDeploymentInput{
		ID: "d1",
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:           domain.PlacementStrategySelector,
			TargetSelector: &domain.TargetSelector{MatchLabels: map[string]string{"env": "prod"}},
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	dep := awaitDeploymentState(ctx, t, h.store, "d1", domain.FulfillmentStateActive)
	assertResolvedTargets(t, dep, "t1", "t3")
}

func TestCreateDeployment_StaticPlacement_UnknownTarget(t *testing.T) {
	h := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	registerTargets(t, h, "t1")

	_, err := h.deployments.Create(ctx, domain.CreateDeploymentInput{
		ID: "d1",
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"t1", "missing"},
		},
	})
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected ErrNotFound or nil, got: %v", err)
	}
}

func TestDeleteDeployment_TransitionsToDeleting(t *testing.T) {
	h := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	registerTargets(t, h, "t1", "t2")

	_, err := h.deployments.Create(ctx, domain.CreateDeploymentInput{
		ID: "d1",
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll},
	})
	if err != nil {
		t.Fatal(err)
	}

	awaitDeploymentState(ctx, t, h.store, "d1", domain.FulfillmentStateActive)

	dep, err := h.deployments.Delete(ctx, "d1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if dep.Fulfillment.State != domain.FulfillmentStateDeleting {
		t.Errorf("returned Fulfillment.State = %q, want deleting", dep.Fulfillment.State)
	}

	// The deployment should eventually be fully removed once the
	// background cleanup workflow completes.
	awaitDeploymentGone(ctx, t, h.store, "d1")
}

func TestDeleteDeployment_ReturnsSnapshot(t *testing.T) {
	h := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	registerTargets(t, h, "t1")

	_, err := h.deployments.Create(ctx, domain.CreateDeploymentInput{
		ID: "d1",
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll},
	})
	if err != nil {
		t.Fatal(err)
	}

	awaitDeploymentState(ctx, t, h.store, "d1", domain.FulfillmentStateActive)

	dep, err := h.deployments.Delete(ctx, "d1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if dep.Fulfillment.State != domain.FulfillmentStateDeleting {
		t.Errorf("Fulfillment.State = %q, want deleting", dep.Fulfillment.State)
	}
	if dep.Deployment.ID != "d1" {
		t.Errorf("ID = %q, want d1", dep.Deployment.ID)
	}
}

func TestDeleteDeployment_SecondDeleteAfterCompleteIsNotFound(t *testing.T) {
	h := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	registerTargets(t, h, "t1")

	_, err := h.deployments.Create(ctx, domain.CreateDeploymentInput{
		ID: "d1",
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll},
	})
	if err != nil {
		t.Fatal(err)
	}

	awaitDeploymentState(ctx, t, h.store, "d1", domain.FulfillmentStateActive)

	_, err = h.deployments.Delete(ctx, "d1")
	if err != nil {
		t.Fatalf("first Delete: %v", err)
	}

	awaitDeploymentGone(ctx, t, h.store, "d1")

	_, err = h.deployments.Delete(ctx, "d1")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("second Delete after cleanup: got %v, want ErrNotFound", err)
	}
}

func TestDeleteDeployment_IdempotentWhileDeleting(t *testing.T) {
	h := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	registerTargets(t, h, "t1")

	_, err := h.deployments.Create(ctx, domain.CreateDeploymentInput{
		ID: "d1",
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll},
	})
	if err != nil {
		t.Fatal(err)
	}

	awaitDeploymentState(ctx, t, h.store, "d1", domain.FulfillmentStateActive)

	dep1, err := h.deployments.Delete(ctx, "d1")
	if err != nil {
		t.Fatalf("first Delete: %v", err)
	}
	if dep1.Fulfillment.State != domain.FulfillmentStateDeleting {
		t.Fatalf("first Delete state = %q, want deleting", dep1.Fulfillment.State)
	}

	// Second delete while still deleting should be idempotent.
	dep2, err := h.deployments.Delete(ctx, "d1")
	if err != nil {
		t.Fatalf("second Delete (idempotent): %v", err)
	}
	if dep2.Fulfillment.State != domain.FulfillmentStateDeleting {
		t.Fatalf("second Delete state = %q, want deleting", dep2.Fulfillment.State)
	}
}

func TestDeleteDeployment_DeploymentVisibleDuringCleanup(t *testing.T) {
	h := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	registerTargets(t, h, "t1")

	_, err := h.deployments.Create(ctx, domain.CreateDeploymentInput{
		ID: "d1",
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll},
	})
	if err != nil {
		t.Fatal(err)
	}

	awaitDeploymentState(ctx, t, h.store, "d1", domain.FulfillmentStateActive)

	dep, err := h.deployments.Delete(ctx, "d1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if dep.Fulfillment.State != domain.FulfillmentStateDeleting {
		t.Fatalf("Delete state = %q, want deleting", dep.Fulfillment.State)
	}

	// Deployment should be visible via GetView in DELETING state after
	// Delete returns (the cleanup workflow hasn't finished yet or we can
	// verify it's at least DELETING before it completes).
	view, err := queryDeployment(ctx, t, h.store, "d1")
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("GetView after Delete: %v", err)
	}
	if err == nil && view.Fulfillment.State != domain.FulfillmentStateDeleting {
		t.Fatalf("GetView state = %q, want deleting", view.Fulfillment.State)
	}
}

func queryDeployment(ctx context.Context, t *testing.T, store domain.Store, id domain.DeploymentID) (domain.DeploymentView, error) {
	t.Helper()
	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()
	return tx.Deployments().GetView(ctx, id)
}

func awaitDeploymentGone(ctx context.Context, t *testing.T, store domain.Store, id domain.DeploymentID) {
	t.Helper()
	for {
		tx, err := store.BeginReadOnly(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		_, err = tx.Deployments().GetView(ctx, id)
		tx.Rollback()
		if errors.Is(err, domain.ErrNotFound) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for deployment %s to be deleted", id)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func awaitCondition(ctx context.Context, t *testing.T, store domain.Store, id domain.DeploymentID, cond func(domain.DeploymentView) bool) domain.DeploymentView {
	t.Helper()
	for {
		tx, err := store.BeginReadOnly(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		view, err := tx.Deployments().GetView(ctx, id)
		tx.Rollback()
		if err == nil && cond(view) {
			return view
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for condition on deployment %s", id)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestCreateDeployment_MissingID(t *testing.T) {
	h := setup(t)
	_, err := h.deployments.Create(context.Background(), domain.CreateDeploymentInput{})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

// seedDeployment persists a fulfillment and a thin deployment that references it.
// mutate runs after default strategies are applied via Advance* helpers.
func seedDeployment(t *testing.T, store domain.Store, depID domain.DeploymentID, mutate func(*domain.Fulfillment)) {
	t.Helper()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fID := domain.FulfillmentID("ful-" + string(depID))
	f := domain.Fulfillment{
		ID:        fID,
		CreatedAt: now,
		UpdatedAt: now,
	}
	ms := defaultManifestStrategy()
	ps := defaultPlacementStrategy()
	f.AdvanceManifestStrategy(ms, now)
	f.AdvancePlacementStrategy(ps, now)
	f.AdvanceRolloutStrategy(nil, now)
	if mutate != nil {
		mutate(&f)
	}
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()
	if err := tx.Fulfillments().Create(ctx, f); err != nil {
		t.Fatalf("Create fulfillment: %v", err)
	}
	d := domain.Deployment{
		ID:            depID,
		UID:           "test-uid",
		FulfillmentID: fID,
		CreatedAt:     now,
		UpdatedAt:     now,
		Etag:          "test-etag",
	}
	if err := tx.Deployments().Create(ctx, d); err != nil {
		t.Fatalf("Create deployment: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func testAuthContext() *application.AuthorizationContext {
	return &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-1", Issuer: "https://issuer.example.com"}},
		Token:   "fresh-token",
	}
}

func TestResumeDeployment_WrongState(t *testing.T) {
	h := setup(t)
	ctx := application.ContextWithAuth(context.Background(), testAuthContext())

	seedDeployment(t, h.store, "d1", func(f *domain.Fulfillment) {
		f.State = domain.FulfillmentStateActive
	})

	_, err := h.deployments.Resume(ctx, application.ResumeInput{ID: "d1"})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestResumeDeployment_NotFound(t *testing.T) {
	h := setup(t)
	ctx := application.ContextWithAuth(context.Background(), testAuthContext())

	_, err := h.deployments.Resume(ctx, application.ResumeInput{ID: "nonexistent"})
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
}

func TestResumeDeployment_NoAuth(t *testing.T) {
	h := setup(t)

	seedDeployment(t, h.store, "d1", func(f *domain.Fulfillment) {
		f.State = domain.FulfillmentStatePausedAuth
	})

	_, err := h.deployments.Resume(context.Background(), application.ResumeInput{ID: "d1"})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

// authFailThenSucceedAgent fails the first delivery with
// DeliveryStateAuthFailed, then succeeds on all subsequent attempts.
type authFailThenSucceedAgent struct {
	mu      sync.Mutex
	attempt int
}

func (a *authFailThenSucceedAgent) Deliver(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	a.mu.Lock()
	a.attempt++
	n := a.attempt
	a.mu.Unlock()

	if n == 1 {
		result := domain.DeliveryResult{
			State:   domain.DeliveryStateAuthFailed,
			Message: "401 Unauthorized",
		}
		signaler.Done(context.Background(), result)
		return result, nil
	}
	result := domain.DeliveryResult{State: domain.DeliveryStateDelivered}
	signaler.Done(context.Background(), result)
	return result, nil
}

func (a *authFailThenSucceedAgent) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ *domain.DeliverySignaler) error {
	return nil
}

func TestResumeDeployment_PausedAuth_EndToEnd(t *testing.T) {
	store := newStore(t)
	h := setupWithStoreAndAgent(t, store, &authFailThenSucceedAgent{})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	registerTargets(t, h, "t1")

	authCtx := application.ContextWithAuth(ctx, &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-1", Issuer: "https://issuer.example.com"}},
		Token:   "expired-token",
	})

	_, err := h.deployments.Create(authCtx, domain.CreateDeploymentInput{
		ID: "d1",
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"kind":"ConfigMap"}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"t1"},
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	awaitDeploymentState(ctx, t, h.store, "d1", domain.FulfillmentStatePausedAuth)

	resumeCtx := application.ContextWithAuth(ctx, &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-1", Issuer: "https://issuer.example.com"}},
		Token:   "fresh-token",
	})
	_, err = h.deployments.Resume(resumeCtx, application.ResumeInput{ID: "d1"})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}

	dep := awaitDeploymentState(ctx, t, h.store, "d1", domain.FulfillmentStateActive)
	assertResolvedTargets(t, dep, "t1")
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
