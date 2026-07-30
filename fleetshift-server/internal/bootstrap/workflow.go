package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
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

// WorkflowRuntime couples a domain.Registry to its owned start, wait, and
// bounded-close lifecycle. All workflows must be registered before Start.
type WorkflowRuntime interface {
	// Registry returns the workflow registry used to register and start work.
	Registry() domain.Registry
	// Start begins background workflow processing. It must be called only
	// after all workflows have been registered.
	Start(ctx context.Context) error
	// Wait blocks until the runtime stops or ctx is cancelled. It surfaces
	// unexpected termination; normal Close completion is not an error here
	// for runtimes that join cleanly.
	Wait(ctx context.Context) error
	// Close performs bounded shutdown of the runtime. Callers supply the
	// deadline via ctx; implementations should not ignore it.
	Close(ctx context.Context) error
}

// GoWorkflowRuntime is the production go-workflows worker/backend bundle.
type GoWorkflowRuntime struct {
	reg    *goworkflows.Registry
	worker *worker.Worker

	cancel   context.CancelFunc
	waitOnce sync.Once
	waitErr  error
}

// NewGoWorkflowRuntime builds a production workflow runtime for database.
func NewGoWorkflowRuntime(database Database, logger *slog.Logger) (*GoWorkflowRuntime, error) {
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
	return &GoWorkflowRuntime{
		reg: &goworkflows.Registry{
			Worker:  wfWorker,
			Client:  wfClient,
			Timeout: 30 * time.Second,
		},
		worker: wfWorker,
	}, nil
}

// Registry implements WorkflowRuntime.
func (r *GoWorkflowRuntime) Registry() domain.Registry { return r.reg }

// Start implements WorkflowRuntime. The worker is stopped by canceling the
// derived run context from Close (or when the parent ctx is cancelled).
func (r *GoWorkflowRuntime) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	if err := r.worker.Start(runCtx); err != nil {
		cancel()
		return fmt.Errorf("start workflow worker: %w", err)
	}
	r.cancel = cancel
	return nil
}

// waitCompletion joins the worker exactly once. go-workflows'
// WaitForCompletion closes an internal channel and is not safe to call twice.
func (r *GoWorkflowRuntime) waitCompletion() error {
	r.waitOnce.Do(func() {
		r.waitErr = r.worker.WaitForCompletion()
	})
	return r.waitErr
}

// Wait implements WorkflowRuntime. The go-workflows worker exposes
// cancellation/join only; poll/task failures are not claimed here.
func (r *GoWorkflowRuntime) Wait(ctx context.Context) error {
	done := make(chan error, 1)
	go func() { done <- r.waitCompletion() }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

// Close implements WorkflowRuntime. It cancels the Start run context, then
// joins worker completion within ctx.
func (r *GoWorkflowRuntime) Close(ctx context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	return r.Wait(ctx)
}

// MemWorkflowRuntime wraps memworkflow.Registry for test substitutions.
type MemWorkflowRuntime struct {
	reg *memworkflow.Registry
}

// NewMemWorkflowRuntime returns an in-memory workflow runtime.
func NewMemWorkflowRuntime() *MemWorkflowRuntime {
	return &MemWorkflowRuntime{reg: &memworkflow.Registry{}}
}

// Registry implements WorkflowRuntime.
func (r *MemWorkflowRuntime) Registry() domain.Registry { return r.reg }

// Start implements WorkflowRuntime.
func (r *MemWorkflowRuntime) Start(context.Context) error { return nil }

// Wait implements WorkflowRuntime.
func (r *MemWorkflowRuntime) Wait(context.Context) error { return nil }

// Close implements WorkflowRuntime.
func (r *MemWorkflowRuntime) Close(context.Context) error { return nil }
