package goworkflows_test

import (
	"context"
	"testing"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	wfsqlite "github.com/cschleiden/go-workflows/backend/sqlite"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/worker"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/workflowenginetest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
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

func goInfra(t *testing.T) workflowenginetest.Infra {
	t.Helper()
	db := sqlite.OpenTestDB(t)
	targetRepo := &sqlite.TargetRepo{DB: db}
	deploymentRepo := &sqlite.DeploymentRepo{DB: db}
	deliveryRepo := &sqlite.DeliveryRepo{DB: db}
	recordingAgent := &sqlite.RecordingDeliveryService{
		Deliveries: deliveryRepo,
		Now:        func() time.Time { return time.Date(2026, 2, 28, 12, 0, 0, 0, time.UTC) },
	}
	router := delivery.NewRoutingDeliveryService()
	router.Register(workflowenginetest.TestTargetType, recordingAgent)
	return workflowenginetest.Infra{
		Targets:     targetRepo,
		Deployments: deploymentRepo,
		Deliveries:  deliveryRepo,
		Delivery:    router,
	}
}

// TestWorkflowEngine_GoWorkflows runs the workflow engine contract against
// the go-workflows engine. The engine only provides [domain.WorkflowEngine];
// worker and client setup are implementation-specific.
func TestWorkflowEngine_GoWorkflows(t *testing.T) {
	workflowenginetest.Run(t, goInfra, func(t *testing.T) domain.WorkflowEngine {
		b := wfsqlite.NewInMemoryBackend()
		w := startWorker(t, b)
		c := client.New(b)
		return &goworkflows.Engine{Worker: w, Client: c, Timeout: 10 * time.Second}
	})
}
