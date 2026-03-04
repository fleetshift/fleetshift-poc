package dbosworkflows_test

import (
	"context"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/workflowenginetest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/dbosworkflows"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func startPostgres(t *testing.T) string {
	t.Helper()

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

func dbosInfra(t *testing.T) workflowenginetest.Infra {
	t.Helper()
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	recordingAgent := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 2, 28, 12, 0, 0, 0, time.UTC) },
	}
	router := delivery.NewRoutingDeliveryService()
	router.Register(workflowenginetest.TestTargetType, recordingAgent)
	return workflowenginetest.Infra{
		Store:    store,
		Delivery: router,
	}
}

// TestWorkflowEngine_DBOS runs the workflow engine contract against the DBOS engine.
// The engine only provides [domain.WorkflowEngine]; setup (Postgres, Launch) and
// teardown are implementation-specific.
func TestWorkflowEngine_DBOS(t *testing.T) {
	workflowenginetest.Run(t, dbosInfra, func(t *testing.T) domain.WorkflowEngine {
		connStr := startPostgres(t)

		dbosCtx, err := dbos.NewDBOSContext(context.Background(), dbos.Config{
			AppName:     "fleetshift-dbos-test",
			DatabaseURL: connStr,
		})
		if err != nil {
			t.Fatalf("NewDBOSContext: %v", err)
		}

		t.Cleanup(func() { dbos.Shutdown(dbosCtx, 5*time.Second) })

		return &dbosworkflows.Engine{DBOSCtx: dbosCtx}
	})
}
