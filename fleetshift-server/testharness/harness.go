// Package testharness provides shared application-layer wiring for
// integration tests. It creates an in-memory SQLite store, workflow
// engine, and all platform services, eliminating duplication across
// testserver, kind integration tests, and kubernetes e2e tests.
package testharness

import (
	"context"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// Harness holds the wired platform services for integration tests.
type Harness struct {
	Store           domain.Store
	Router          *delivery.RoutingDeliveryService
	Reporter        *application.DeliveryReportService
	Deployments     *application.DeploymentService
	Targets         *application.TargetService
	ManagedResources *application.ManagedResourceService
	AddonMgr        *application.AddonManager
	Registry        *memworkflow.Registry
}

// Option configures a Harness.
type Option func(*config)

type config struct {
	activator application.SchemaActivator
}

// WithSchemaActivator sets the schema activator for the AddonManager.
// The default is a no-op activator.
func WithSchemaActivator(a application.SchemaActivator) Option {
	return func(c *config) { c.activator = a }
}

type noopActivator struct{}

func (noopActivator) Activate(_ context.Context, _ domain.ManagedResourceSchema) (application.SchemaHandle, error) {
	return application.SchemaHandle{}, nil
}
func (noopActivator) Deactivate(_ application.SchemaHandle) {}

// New creates a fully wired Harness with an in-memory SQLite store,
// workflow engine, and all platform services.
func New(t *testing.T, opts ...Option) *Harness {
	t.Helper()

	cfg := &config{}
	for _, o := range opts {
		o(cfg)
	}

	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	reg := &memworkflow.Registry{}
	router := delivery.NewRoutingDeliveryService()
	reporter := application.NewDeliveryReportService(store, reg)

	orchSpec := domain.NewOrchestrationWorkflowSpec(
		store, router, domain.StrategyFactory{Store: store}, reg,
		domain.WithAckRetryInterval(5*time.Second),
	)
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	createWf, err := reg.RegisterCreateDeployment(&domain.CreateDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	})
	if err != nil {
		t.Fatalf("RegisterCreateDeployment: %v", err)
	}

	cleanupWf, err := reg.RegisterDeleteDeploymentCleanup(&domain.DeleteDeploymentCleanupWorkflowSpec{Store: store})
	if err != nil {
		t.Fatalf("RegisterDeleteDeploymentCleanup: %v", err)
	}

	deleteWf, err := reg.RegisterDeleteDeployment(&domain.DeleteDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		Cleanup:       cleanupWf,
	})
	if err != nil {
		t.Fatalf("RegisterDeleteDeployment: %v", err)
	}

	resumeWf, err := reg.RegisterResumeDeployment(&domain.ResumeDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	})
	if err != nil {
		t.Fatalf("RegisterResumeDeployment: %v", err)
	}

	createMRWf, err := reg.RegisterCreateManagedResource(&domain.CreateManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	})
	if err != nil {
		t.Fatalf("RegisterCreateManagedResource: %v", err)
	}

	mrCleanupWf, err := reg.RegisterDeleteManagedResourceCleanup(&domain.DeleteManagedResourceCleanupWorkflowSpec{Store: store})
	if err != nil {
		t.Fatalf("RegisterDeleteManagedResourceCleanup: %v", err)
	}

	deleteMRWf, err := reg.RegisterDeleteManagedResource(&domain.DeleteManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		Cleanup:       mrCleanupWf,
	})
	if err != nil {
		t.Fatalf("RegisterDeleteManagedResource: %v", err)
	}

	resumeMRWf, err := reg.RegisterResumeManagedResource(&domain.ResumeManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	})
	if err != nil {
		t.Fatalf("RegisterResumeManagedResource: %v", err)
	}

	var activator application.SchemaActivator = noopActivator{}
	if cfg.activator != nil {
		activator = cfg.activator
	}

	typeSvc := &application.ManagedResourceTypeService{Store: store}
	addonMgr := application.NewAddonManager(application.AddonManagerDeps{
		Router:    router,
		TypeSvc:   typeSvc,
		Activator: activator,
	})

	return &Harness{
		Store:    store,
		Router:   router,
		Reporter: reporter,
		Deployments: &application.DeploymentService{
			Store:    store,
			CreateWF: createWf,
			DeleteWF: deleteWf,
			ResumeWF: resumeWf,
		},
		Targets: &application.TargetService{Store: store},
		ManagedResources: &application.ManagedResourceService{
			Store:    store,
			CreateWF: createMRWf,
			DeleteWF: deleteMRWf,
			ResumeWF: resumeMRWf,
		},
		AddonMgr: addonMgr,
		Registry: reg,
	}
}
