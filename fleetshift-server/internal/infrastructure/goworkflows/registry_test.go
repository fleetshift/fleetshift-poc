package goworkflows_test

import (
	"context"
	"testing"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	wfsqlite "github.com/cschleiden/go-workflows/backend/sqlite"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/worker"
	"github.com/cschleiden/go-workflows/workflow"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/workflowenginetest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/goworkflows"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func startWorker(t *testing.T, b backend.Backend) *worker.Worker {
	t.Helper()
	w := worker.New(b, &worker.Options{
		WorkflowWorkerOptions: worker.WorkflowWorkerOptions{
			WorkflowPollers:         2,
			WorkflowPollingInterval: 5 * time.Millisecond,
		},
		ActivityWorkerOptions: worker.ActivityWorkerOptions{
			ActivityPollers:         2,
			ActivityPollingInterval: 5 * time.Millisecond,
		},
		SingleWorkerMode: true,
	})
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

func goInfra(t *testing.T) workflowenginetest.Infra {
	t.Helper()
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	vault := &sqlite.VaultStore{DB: db}
	recordingAgent := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 2, 28, 12, 0, 0, 0, time.UTC) },
	}
	router := delivery.NewRoutingDeliveryService()
	router.Register(workflowenginetest.TestTargetType, recordingAgent)
	return workflowenginetest.Infra{
		Store:          store,
		Delivery:       router,
		Vault:          vault,
		AgentRegistrar: router,
	}
}

// TestWorkflowEngine_GoWorkflows runs the workflow engine contract against
// the go-workflows registry. The registry only provides [domain.Registry];
// worker and client setup are implementation-specific.
func TestWorkflowEngine_GoWorkflows(t *testing.T) {
	// Use fast retry options for tests so that transient-failure and
	// terminal-failure tests don't spend minutes in exponential backoff.
	testActivityOpts := &workflow.ActivityOptions{
		RetryOptions: workflow.RetryOptions{
			MaxAttempts:        3,
			FirstRetryInterval: 1 * time.Millisecond,
			MaxRetryInterval:   5 * time.Millisecond,
			BackoffCoefficient: 2,
		},
	}

	workflowenginetest.Run(t, goInfra, func(t *testing.T) domain.Registry {
		b := wfsqlite.NewInMemoryBackend()
		w := startWorker(t, b)
		c := client.New(b)
		return &goworkflows.Registry{
			Worker:          w,
			Client:          c,
			Timeout:         10 * time.Second,
			ActivityOptions: testActivityOpts,
		}
	})
}
