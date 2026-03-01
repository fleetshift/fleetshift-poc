package goworkflows_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	wfsqlite "github.com/cschleiden/go-workflows/backend/sqlite"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/worker"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/goworkflows"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func startWorker(t *testing.T, b backend.Backend) *worker.Worker {
	t.Helper()
	w := worker.New(b, nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = w.WaitForCompletion()
	})
	if err := w.Start(ctx); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	return w
}

func TestOrchestration_GoWorkflows(t *testing.T) {
	b := wfsqlite.NewInMemoryBackend()
	w := startWorker(t, b)
	c := client.New(b)

	db := sqlite.OpenTestDB(t)
	targetRepo := &sqlite.TargetRepo{DB: db}
	deploymentRepo := &sqlite.DeploymentRepo{DB: db}
	recordRepo := &sqlite.DeliveryRecordRepo{DB: db}

	deliverySvc := &sqlite.RecordingDeliveryService{
		Records: recordRepo,
		Now:     func() time.Time { return time.Date(2026, 2, 28, 12, 0, 0, 0, time.UTC) },
	}

	wf := &domain.OrchestrationWorkflow{
		Deployments: deploymentRepo,
		Targets:     targetRepo,
		Delivery:    deliverySvc,
		Strategies:  domain.DefaultStrategyFactory{},
	}

	engine := &goworkflows.Engine{Worker: w, Client: c, Timeout: 10 * time.Second}
	runner, err := engine.OrchestrationRunner(wf)
	if err != nil {
		t.Fatalf("OrchestrationRunner: %v", err)
	}

	orchestration := &application.OrchestrationService{Workflow: runner}

	depSvc := &application.DeploymentService{
		Deployments:   deploymentRepo,
		Records:       recordRepo,
		Orchestration: orchestration,
	}
	targetSvc := &application.TargetService{Targets: targetRepo}

	ctx := context.Background()

	for _, id := range []string{"t1", "t2", "t3"} {
		if err := targetSvc.Register(ctx, domain.TargetInfo{
			ID:   domain.TargetID(id),
			Name: "cluster-" + id,
		}); err != nil {
			t.Fatalf("register target %s: %v", id, err)
		}
	}

	dep, err := depSvc.Create(ctx, application.CreateDeploymentInput{
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
		t.Fatalf("Create deployment: %v", err)
	}

	if dep.State != domain.DeploymentStateActive {
		t.Errorf("State = %q, want %q", dep.State, domain.DeploymentStateActive)
	}
	if len(dep.ResolvedTargets) != 2 {
		t.Fatalf("ResolvedTargets: got %d, want 2", len(dep.ResolvedTargets))
	}

	records, err := recordRepo.ListByDeployment(ctx, "d1")
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
	}
}
