package bootstrap

import (
	"fmt"
	"log/slog"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/observability"
	transporthttp "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/http"
)

// registeredWorkflows are workflow handles and shared deps registered before
// the worker starts.
type registeredWorkflows struct {
	createWf      domain.CreateDeploymentWorkflow
	deleteWf      domain.DeleteDeploymentWorkflow
	resumeWf      domain.ResumeDeploymentWorkflow
	createMRWf    domain.CreateManagedResourceWorkflow
	deleteMRWf    domain.DeleteManagedResourceWorkflow
	resumeMRWf    domain.ResumeManagedResourceWorkflow
	authMethodSvc *application.AuthMethodService
	provenanceSvc *domain.ProvenanceService
}

// registerWorkflows registers every production workflow before Start on the
// registry. Callers must not start the worker until this returns successfully.
func registerWorkflows(
	reg domain.Registry,
	store domain.Store,
	vault domain.Vault,
	deliveryAgent domain.DeliveryAgent,
	authMethodRepo domain.AuthMethodRepository,
	oidcDeps OIDCDeps,
	setupHub *transporthttp.SetupHub,
	enabledAddons map[AddonName]bool,
	gcphcpTargetID string,
	keyResolver *domain.KeyResolver,
	logger *slog.Logger,
) (registeredWorkflows, error) {
	var out registeredWorkflows

	orchSpec := domain.NewOrchestrationWorkflowSpec(
		store, deliveryAgent, domain.StrategyFactory{Store: store}, reg,
		domain.WithFulfillmentObserver(observability.NewFulfillmentObserver(logger)),
		domain.WithVault(vault),
	)
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		return out, fmt.Errorf("register orchestration: %w", err)
	}

	out.createWf, err = reg.RegisterCreateDeployment(&domain.CreateDeploymentWorkflowSpec{
		Store: store, Orchestration: orchWf,
	})
	if err != nil {
		return out, fmt.Errorf("register create-deployment: %w", err)
	}

	deleteObs := observability.NewDeleteObserver(logger)
	cleanupWf, err := reg.RegisterDeleteDeploymentCleanup(&domain.DeleteDeploymentCleanupWorkflowSpec{
		Store: store, Observer: deleteObs,
	})
	if err != nil {
		return out, fmt.Errorf("register delete-deployment-cleanup: %w", err)
	}
	out.deleteWf, err = reg.RegisterDeleteDeployment(&domain.DeleteDeploymentWorkflowSpec{
		Store: store, Orchestration: orchWf, Cleanup: cleanupWf, Observer: deleteObs,
	})
	if err != nil {
		return out, fmt.Errorf("register delete-deployment: %w", err)
	}

	out.createMRWf, err = reg.RegisterCreateManagedResource(&domain.CreateManagedResourceWorkflowSpec{
		Store: store, Orchestration: orchWf,
	})
	if err != nil {
		return out, fmt.Errorf("register create-managed-resource: %w", err)
	}
	mrCleanupWf, err := reg.RegisterDeleteManagedResourceCleanup(&domain.DeleteManagedResourceCleanupWorkflowSpec{
		Store: store, Observer: deleteObs,
	})
	if err != nil {
		return out, fmt.Errorf("register delete-managed-resource-cleanup: %w", err)
	}
	out.deleteMRWf, err = reg.RegisterDeleteManagedResource(&domain.DeleteManagedResourceWorkflowSpec{
		Store: store, Orchestration: orchWf, Cleanup: mrCleanupWf, Observer: deleteObs,
	})
	if err != nil {
		return out, fmt.Errorf("register delete-managed-resource: %w", err)
	}

	provSpec := &domain.ProvisionIdPWorkflowSpec{
		AuthMethods:      authMethodRepo,
		Discovery:        oidcDeps.Discovery,
		CreateDeployment: out.createWf,
		EventSink:        setupHub,
	}
	if placement := buildTrustBundlePlacement(enabledAddons, gcphcpTargetID); placement.Type != "" {
		provSpec.TrustBundlePlacement = placement
	}
	// Facade assemblies may enable kind/gcphcp without Config.Addons; trust
	// placement for those is handled when the facade sets Config.Addons to match.
	provWf, err := reg.RegisterProvisionIdP(provSpec)
	if err != nil {
		return out, fmt.Errorf("register provision-idp: %w", err)
	}

	out.authMethodSvc = &application.AuthMethodService{
		Methods:     authMethodRepo,
		ProvisionWF: provWf,
	}
	out.provenanceSvc = &domain.ProvenanceService{
		KeyResolver: keyResolver,
		AuthMethods: authMethodRepo,
	}

	out.resumeWf, err = reg.RegisterResumeDeployment(&domain.ResumeDeploymentWorkflowSpec{
		Store: store, Orchestration: orchWf, ProvenanceSvc: out.provenanceSvc,
	})
	if err != nil {
		return out, fmt.Errorf("register resume-deployment: %w", err)
	}
	out.resumeMRWf, err = reg.RegisterResumeManagedResource(&domain.ResumeManagedResourceWorkflowSpec{
		Store: store, Orchestration: orchWf, ProvenanceSvc: out.provenanceSvc,
	})
	if err != nil {
		return out, fmt.Errorf("register resume-managed-resource: %w", err)
	}

	return out, nil
}
