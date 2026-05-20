package memworkflow_test

import (
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/workflowenginetest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func memInfra(t *testing.T) workflowenginetest.Infra {
	t.Helper()
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	vault := &sqlite.VaultStore{DB: db}
	recordingAgent := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 2, 27, 12, 0, 0, 0, time.UTC) },
	}
	router := delivery.NewRoutingDeliveryService()
	router.Register(workflowenginetest.TestTargetType, recordingAgent)
	return workflowenginetest.Infra{
		Store:    store,
		Delivery: router,
		Vault:    vault,
		DeliveryReporterFactory: func(reg domain.Registry) domain.DeliveryReporter {
			reporter := application.NewDeliveryReportService(store, reg)
			recordingAgent.Reporter = reporter
			return reporter
		},
		AgentRegistrar: router,
	}
}

func TestWorkflowEngine(t *testing.T) {
	workflowenginetest.Run(t, memInfra, func(t *testing.T) domain.Registry {
		return &memworkflow.Registry{}
	})
}
