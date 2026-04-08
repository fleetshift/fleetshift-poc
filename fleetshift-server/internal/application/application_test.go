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

	return testHarness{
		targets: &application.TargetService{Store: store},
		deployments: &application.DeploymentService{
			Store:         store,
			CreateWF:      createWf,
			Orchestration: orchWf,
			KeyResolver:   keyResolver,
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

func awaitDeploymentState(ctx context.Context, t *testing.T, store domain.Store, id domain.DeploymentID, want domain.DeploymentState) domain.Deployment {
	t.Helper()
	for {
		tx, err := store.BeginReadOnly(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		dep, err := tx.Deployments().Get(ctx, id)
		tx.Rollback()
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get(%s): %v", id, err)
		}
		if err == nil && dep.State == want {
			return dep
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for deployment %s to reach state %q", id, want)
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
	records, err := tx.Deliveries().ListByDeployment(ctx, depID)
	if err != nil {
		t.Fatalf("ListByDeployment: %v", err)
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

func assertResolvedTargets(t *testing.T, dep domain.Deployment, expectedIDs ...string) {
	t.Helper()
	if len(dep.ResolvedTargets) != len(expectedIDs) {
		t.Fatalf("ResolvedTargets: got %d, want %d", len(dep.ResolvedTargets), len(expectedIDs))
	}
	got := make(map[domain.TargetID]bool)
	for _, id := range dep.ResolvedTargets {
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

	dep := awaitDeploymentState(ctx, t, h.store, "d1", domain.DeploymentStateActive)
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

	dep := awaitDeploymentState(ctx, t, h.store, "d1", domain.DeploymentStateActive)
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

	dep := awaitDeploymentState(ctx, t, h.store, "d1", domain.DeploymentStateActive)
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

	awaitDeploymentState(ctx, t, h.store, "d1", domain.DeploymentStateActive)

	dep, err := h.deployments.Delete(ctx, "d1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if dep.State != domain.DeploymentStateDeleting {
		t.Errorf("returned State = %q, want deleting", dep.State)
	}

	// Verify the deployment is persisted in Deleting state.
	persisted, err := queryDeployment(ctx, t, h.store, "d1")
	if err != nil {
		t.Fatalf("query deployment after delete: %v", err)
	}
	if persisted.State != domain.DeploymentStateDeleting {
		t.Errorf("persisted State = %q, want deleting", persisted.State)
	}
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

	awaitDeploymentState(ctx, t, h.store, "d1", domain.DeploymentStateActive)

	dep, err := h.deployments.Delete(ctx, "d1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if dep.State != domain.DeploymentStateDeleting {
		t.Errorf("State = %q, want deleting", dep.State)
	}
	if dep.ID != "d1" {
		t.Errorf("ID = %q, want d1", dep.ID)
	}
}

func TestDeleteDeployment_Idempotent(t *testing.T) {
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

	awaitDeploymentState(ctx, t, h.store, "d1", domain.DeploymentStateActive)

	_, err = h.deployments.Delete(ctx, "d1")
	if err != nil {
		t.Fatalf("first Delete: %v", err)
	}

	// Second delete on same (now Deleting) deployment should not error.
	_, err = h.deployments.Delete(ctx, "d1")
	if err != nil {
		t.Fatalf("second Delete (idempotent): %v", err)
	}
}

func queryDeployment(ctx context.Context, t *testing.T, store domain.Store, id domain.DeploymentID) (domain.Deployment, error) {
	t.Helper()
	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()
	return tx.Deployments().Get(ctx, id)
}

func awaitCondition(ctx context.Context, t *testing.T, store domain.Store, id domain.DeploymentID, cond func(domain.Deployment) bool) domain.Deployment {
	t.Helper()
	for {
		tx, err := store.BeginReadOnly(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		dep, err := tx.Deployments().Get(ctx, id)
		tx.Rollback()
		if err == nil && cond(dep) {
			return dep
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

func seedDeployment(t *testing.T, store domain.Store, dep domain.Deployment) {
	t.Helper()
	if dep.UID == "" {
		dep.UID = "test-uid"
	}
	if dep.Etag == "" {
		dep.Etag = "test-etag"
	}
	if dep.CreatedAt.IsZero() {
		dep.CreatedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	if dep.UpdatedAt.IsZero() {
		dep.UpdatedAt = dep.CreatedAt
	}
	if dep.Generation == 0 {
		dep.Generation = 1
	}
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()
	if err := tx.Deployments().Create(ctx, dep); err != nil {
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

	seedDeployment(t, h.store, domain.Deployment{
		ID:    "d1",
		State: domain.DeploymentStateActive,
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

	seedDeployment(t, h.store, domain.Deployment{
		ID:    "d1",
		State: domain.DeploymentStatePausedAuth,
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

	awaitDeploymentState(ctx, t, h.store, "d1", domain.DeploymentStatePausedAuth)

	resumeCtx := application.ContextWithAuth(ctx, &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-1", Issuer: "https://issuer.example.com"}},
		Token:   "fresh-token",
	})
	_, err = h.deployments.Resume(resumeCtx, application.ResumeInput{ID: "d1"})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}

	dep := awaitDeploymentState(ctx, t, h.store, "d1", domain.DeploymentStateActive)
	assertResolvedTargets(t, dep, "t1")
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
