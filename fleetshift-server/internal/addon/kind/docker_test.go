package kind_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"sigs.k8s.io/kind/pkg/cluster"
	"sigs.k8s.io/kind/pkg/log"

	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// TestKindAddon_RealDocker exercises the full addon lifecycle against
// a real Docker daemon. It creates a kind cluster, verifies it exists,
// then tears it down. Skipped when Docker is not available or when
// running with -short.
func TestKindAddon_RealDocker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real Docker test in short mode")
	}

	checker := cluster.NewProvider()

	if _, err := checker.List(); err != nil {
		t.Skipf("Docker not available: %v", err)
	}

	const clusterName = "fleetshift-test"

	t.Cleanup(func() {
		_ = checker.Delete(clusterName, "")
	})
	// Pre-clean in case a previous run left a stale cluster.
	_ = checker.Delete(clusterName, "")

	kindAgent := kindaddon.NewAgent(func(logger log.Logger) kindaddon.ClusterProvider {
		return cluster.NewProvider(cluster.ProviderWithLogger(logger))
	})
	router := delivery.NewRoutingDeliveryService()
	router.Register(kindaddon.TargetType, kindAgent)

	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	reg := &memworkflow.Registry{}

	orchSpec := &domain.OrchestrationWorkflowSpec{
		Store:      store,
		Delivery:   router,
		Strategies: domain.DefaultStrategyFactory{},
		Registry:   reg,
	}
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	cwfSpec := &domain.CreateDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	}
	createWf, err := reg.RegisterCreateDeployment(cwfSpec)
	if err != nil {
		t.Fatalf("RegisterCreateDeployment: %v", err)
	}

	cleanupSpec := &domain.DeleteCleanupWorkflowSpec{Store: store}
	cleanupWf, err := reg.RegisterDeleteCleanup(cleanupSpec)
	if err != nil {
		t.Fatalf("RegisterDeleteCleanup: %v", err)
	}

	deleteSpec := &domain.DeleteDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		Cleanup:       cleanupWf,
	}
	deleteWf, err := reg.RegisterDeleteDeployment(deleteSpec)
	if err != nil {
		t.Fatalf("RegisterDeleteDeployment: %v", err)
	}

	resumeSpec := &domain.ResumeDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	}
	resumeWf, err := reg.RegisterResumeDeployment(resumeSpec)
	if err != nil {
		t.Fatalf("RegisterResumeDeployment: %v", err)
	}

	targetSvc := &application.TargetService{Store: store}
	deploySvc := &application.DeploymentService{
		Store:    store,
		CreateWF: createWf,
		DeleteWF: deleteWf,
		ResumeWF: resumeWf,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := targetSvc.Register(ctx, domain.TargetInfo{
		ID:   "kind-docker",
		Type: kindaddon.TargetType,
		Name: "Docker Kind Provider",
	}); err != nil {
		t.Fatalf("Register target: %v", err)
	}

	spec := kindaddon.ClusterSpec{Name: clusterName}
	specBytes, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}

	_, err = deploySvc.Create(ctx, domain.CreateDeploymentInput{
		ID: "kind-docker-deploy",
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{
				ResourceType: kindaddon.ClusterResourceType,
				Raw:          json.RawMessage(specBytes),
			}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"kind-docker"},
		},
	})
	if err != nil {
		t.Fatalf("Create deployment: %v", err)
	}

	view := awaitState(ctx, t, store, "kind-docker-deploy", domain.FulfillmentStateActive)
	if len(view.Fulfillment.ResolvedTargets) != 1 || view.Fulfillment.ResolvedTargets[0] != "kind-docker" {
		t.Fatalf("unexpected ResolvedTargets: %v", view.Fulfillment.ResolvedTargets)
	}

	// Delivery is async; poll until the cluster appears or context expires.
	for {
		clusters, err := checker.List()
		if err != nil {
			t.Fatalf("provider.List: %v", err)
		}
		if slices.Contains(clusters, clusterName) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for kind cluster %q to be created", clusterName)
		case <-time.After(5 * time.Second):
		}
	}
}
