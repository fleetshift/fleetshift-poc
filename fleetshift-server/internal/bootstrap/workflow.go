package bootstrap

import (
	"fmt"
	"log/slog"
	"time"

	wfbackend "github.com/cschleiden/go-workflows/backend"
	wfpostgres "github.com/cschleiden/go-workflows/backend/postgres"
	wfsqlite "github.com/cschleiden/go-workflows/backend/sqlite"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/worker"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/goworkflows"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
)

// NewGoWorkflowRegistry builds the production go-workflows registry for database.
// Lifecycle (Start/Wait/Close) is implemented on [goworkflows.Registry].
func NewGoWorkflowRegistry(database Database, logger *slog.Logger) (*goworkflows.Registry, error) {
	var wfBackend wfbackend.Backend
	switch db := database.(type) {
	case Postgres:
		wfBackend = wfpostgres.NewPostgresBackend(db.Host, db.Port, db.User, db.Password, db.Name,
			wfpostgres.WithBackendOptions(wfbackend.WithLogger(logger.With("component", "workflows"))),
		)
	case SQLite:
		wfBackend = wfsqlite.NewSqliteBackend(db.Path,
			wfsqlite.WithBackendOptions(wfbackend.WithLogger(logger.With("component", "workflows"))),
		)
	default:
		return nil, fmt.Errorf("unsupported database config %T", database)
	}
	wfWorker := worker.New(wfBackend, nil)
	wfClient := client.New(wfBackend)
	return &goworkflows.Registry{
		Worker:  wfWorker,
		Client:  wfClient,
		Timeout: 30 * time.Second,
	}, nil
}

// NewMemWorkflowRegistry returns a non-durable in-memory [domain.Registry].
func NewMemWorkflowRegistry() domain.Registry {
	return &memworkflow.Registry{}
}
