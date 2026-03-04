package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/syncworkflow"
)

type testHarness struct {
	targets     *application.TargetService
	deployments *application.DeploymentService
	store       domain.Store
}

const testTargetType domain.TargetType = "test"

func setup(t *testing.T) testHarness {
	t.Helper()
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	recordingAgent := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 2, 27, 12, 0, 0, 0, time.UTC) },
	}
	router := delivery.NewRoutingDeliveryService()
	router.Register(testTargetType, recordingAgent)

	owf := &domain.OrchestrationWorkflow{
		Store:      store,
		Delivery:   router,
		Strategies: domain.DefaultStrategyFactory{},
	}

	cwf := &domain.CreateDeploymentWorkflow{
		Store: store,
	}

	engine := &syncworkflow.Engine{}
	runners, err := engine.Register(owf, cwf)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	return testHarness{
		targets: &application.TargetService{Store: store},
		deployments: &application.DeploymentService{
			Store:    store,
			CreateWF: runners.CreateDeployment,
		},
		store: store,
	}
}

func awaitDeploymentState(ctx context.Context, t *testing.T, store domain.Store, id domain.DeploymentID, want domain.DeploymentState) domain.Deployment {
	t.Helper()
	for {
		tx, err := store.Begin(ctx)
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
	tx, err := store.Begin(ctx)
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

func TestDeleteDeployment_RemovesRecords(t *testing.T) {
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

	if err := h.deployments.Delete(ctx, "d1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	records := queryDeliveries(ctx, t, h.store, "d1")
	if len(records) != 0 {
		t.Fatalf("expected 0 delivery records after delete, got %d", len(records))
	}

	_, err = h.deployments.Get(ctx, "d1")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got: %v", err)
	}
}

func TestCreateDeployment_MissingID(t *testing.T) {
	h := setup(t)
	_, err := h.deployments.Create(context.Background(), domain.CreateDeploymentInput{})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
