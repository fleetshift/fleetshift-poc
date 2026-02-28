package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

type testHarness struct {
	targets     *application.TargetService
	deployments *application.DeploymentService
	records     *sqlite.DeliveryRecordRepo
}

func setup(t *testing.T) testHarness {
	t.Helper()
	db := sqlite.OpenTestDB(t)

	targetRepo := &sqlite.TargetRepo{DB: db}
	deploymentRepo := &sqlite.DeploymentRepo{DB: db}
	recordRepo := &sqlite.DeliveryRecordRepo{DB: db}

	deliverySvc := &sqlite.RecordingDeliveryService{
		Records: recordRepo,
		Now:     func() time.Time { return time.Date(2026, 2, 27, 12, 0, 0, 0, time.UTC) },
	}

	orchestration := &application.OrchestrationService{
		Targets:     targetRepo,
		Deployments: deploymentRepo,
		Delivery:    deliverySvc,
		Strategies:  domain.DefaultStrategyFactory{},
	}

	return testHarness{
		targets: &application.TargetService{Targets: targetRepo},
		deployments: &application.DeploymentService{
			Deployments:   deploymentRepo,
			Records:       recordRepo,
			Orchestration: orchestration,
		},
		records: recordRepo,
	}
}

func TestCreateDeployment_StaticPlacement(t *testing.T) {
	h := setup(t)
	ctx := context.Background()

	registerTargets(t, h, "t1", "t2", "t3")

	dep, err := h.deployments.Create(ctx, application.CreateDeploymentInput{
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

	if dep.State != domain.DeploymentStateActive {
		t.Errorf("State = %q, want %q", dep.State, domain.DeploymentStateActive)
	}
	assertResolvedTargets(t, dep, "t1", "t3")

	records, err := h.records.ListByDeployment(ctx, "d1")
	if err != nil {
		t.Fatalf("ListByDeployment: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 delivery records, got %d", len(records))
	}
	for _, rec := range records {
		if rec.State != domain.DeliveryStateDelivered {
			t.Errorf("record for %s: State = %q, want %q", rec.TargetID, rec.State, domain.DeliveryStateDelivered)
		}
		if len(rec.Manifests) != 1 {
			t.Errorf("record for %s: Manifests len = %d, want 1", rec.TargetID, len(rec.Manifests))
		}
	}
}

func TestCreateDeployment_AllPlacement(t *testing.T) {
	h := setup(t)
	ctx := context.Background()

	registerTargets(t, h, "t1", "t2", "t3")

	dep, err := h.deployments.Create(ctx, application.CreateDeploymentInput{
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

	assertResolvedTargets(t, dep, "t1", "t2", "t3")

	records, _ := h.records.ListByDeployment(ctx, "d1")
	if len(records) != 3 {
		t.Fatalf("expected 3 delivery records, got %d", len(records))
	}
}

func TestCreateDeployment_SelectorPlacement(t *testing.T) {
	h := setup(t)
	ctx := context.Background()

	must(t, h.targets.Register(ctx, domain.TargetInfo{
		ID: "t1", Name: "cluster-prod", Labels: map[string]string{"env": "prod"},
	}))
	must(t, h.targets.Register(ctx, domain.TargetInfo{
		ID: "t2", Name: "cluster-staging", Labels: map[string]string{"env": "staging"},
	}))
	must(t, h.targets.Register(ctx, domain.TargetInfo{
		ID: "t3", Name: "cluster-prod-eu", Labels: map[string]string{"env": "prod"},
	}))

	dep, err := h.deployments.Create(ctx, application.CreateDeploymentInput{
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

	assertResolvedTargets(t, dep, "t1", "t3")
}

func TestCreateDeployment_StaticPlacement_UnknownTarget(t *testing.T) {
	h := setup(t)
	ctx := context.Background()

	registerTargets(t, h, "t1")

	_, err := h.deployments.Create(ctx, application.CreateDeploymentInput{
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
	if err == nil {
		t.Fatal("expected error for unknown target")
	}
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
}

func TestDeleteDeployment_RemovesRecords(t *testing.T) {
	h := setup(t)
	ctx := context.Background()

	registerTargets(t, h, "t1", "t2")

	_, err := h.deployments.Create(ctx, application.CreateDeploymentInput{
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

	if err := h.deployments.Delete(ctx, "d1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	records, _ := h.records.ListByDeployment(ctx, "d1")
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
	_, err := h.deployments.Create(context.Background(), application.CreateDeploymentInput{})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

// --- helpers ---

func registerTargets(t *testing.T, h testHarness, ids ...string) {
	t.Helper()
	for _, id := range ids {
		must(t, h.targets.Register(context.Background(), domain.TargetInfo{
			ID:   domain.TargetID(id),
			Name: "cluster-" + id,
		}))
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

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
