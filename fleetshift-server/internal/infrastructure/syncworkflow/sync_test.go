package syncworkflow_test

import (
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/workflowenginetest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/syncworkflow"
)

func syncInfra(t *testing.T) workflowenginetest.Infra {
	t.Helper()
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	recordingAgent := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 2, 27, 12, 0, 0, 0, time.UTC) },
	}
	router := delivery.NewRoutingDeliveryService()
	router.Register(workflowenginetest.TestTargetType, recordingAgent)
	return workflowenginetest.Infra{
		Store:    store,
		Delivery: router,
	}
}

// TestWorkflowEngine runs the workflow engine contract against the sync engine.
func TestWorkflowEngine(t *testing.T) {
	workflowenginetest.Run(t, syncInfra, func(t *testing.T) domain.WorkflowEngine {
		return &syncworkflow.Engine{}
	})
}
