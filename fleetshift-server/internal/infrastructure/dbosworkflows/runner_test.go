package dbosworkflows_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/dbosworkflows"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func startPostgres(t *testing.T) string {
	t.Helper()

	// Ryuk (the reaper) requires a Docker bridge network that does not
	// exist on Podman. We handle cleanup via t.Cleanup instead.
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	ctx := context.Background()

	ctr, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("dbos_test"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}

	connStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("get postgres connection string: %v", err)
	}
	return connStr
}

func TestOrchestration_DBOS(t *testing.T) {
	connStr := startPostgres(t)

	ctx := context.Background()

	dbosCtx, err := dbos.NewDBOSContext(ctx, dbos.Config{
		AppName:     "fleetshift-dbos-test",
		DatabaseURL: connStr,
	})
	if err != nil {
		t.Fatalf("NewDBOSContext: %v", err)
	}

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

	engine := &dbosworkflows.Engine{DBOSCtx: dbosCtx}
	runner, err := engine.OrchestrationRunner(wf)
	if err != nil {
		t.Fatalf("OrchestrationRunner: %v", err)
	}

	if err := dbos.Launch(dbosCtx); err != nil {
		t.Fatalf("dbos.Launch: %v", err)
	}
	t.Cleanup(func() { dbos.Shutdown(dbosCtx, 5*time.Second) })

	orchestration := &application.OrchestrationService{Workflow: runner}

	depSvc := &application.DeploymentService{
		Deployments:   deploymentRepo,
		Records:       recordRepo,
		Orchestration: orchestration,
	}
	targetSvc := &application.TargetService{Targets: targetRepo}

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
