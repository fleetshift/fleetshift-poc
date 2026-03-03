package kind_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/syncworkflow"
)

// TestKindAddon_EndToEnd exercises the full addon lifecycle:
//
//  1. Register a kind delivery agent with the routing service.
//  2. Register a target of type "kind".
//  3. Create a deployment with an inline kind cluster manifest.
//  4. Verify the deployment reaches Active and the fake provider
//     received the cluster creation.
func TestKindAddon_EndToEnd(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	targetRepo := &sqlite.TargetRepo{DB: db}
	deploymentRepo := &sqlite.DeploymentRepo{DB: db}
	deliveryRepo := &sqlite.DeliveryRepo{DB: db}

	provider := newFakeProvider()
	kindAgent := kindaddon.NewAgent(fakeFactory(provider))
	router := delivery.NewRoutingDeliveryService()
	router.Register(kindaddon.TargetType, kindAgent)

	owf := &domain.OrchestrationWorkflow{
		Deployments: deploymentRepo,
		Targets:     targetRepo,
		Deliveries:  deliveryRepo,
		Delivery:    router,
		Strategies:  domain.DefaultStrategyFactory{},
	}
	cwf := &domain.CreateDeploymentWorkflow{Deployments: deploymentRepo}

	engine := &syncworkflow.Engine{}
	runners, err := engine.Register(owf, cwf)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	targetSvc := &application.TargetService{Targets: targetRepo}
	deploySvc := &application.DeploymentService{
		Deployments: deploymentRepo,
		Deliveries:  deliveryRepo,
		CreateWF:    runners.CreateDeployment,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := targetSvc.Register(ctx, domain.TargetInfo{
		ID:     "my-kind",
		Type:   kindaddon.TargetType,
		Name:   "Local Kind Provider",
		Labels: map[string]string{"env": "dev"},
	}); err != nil {
		t.Fatalf("Register target: %v", err)
	}

	clusterConfig := kindaddon.ClusterSpec{
		Name:   "dev-cluster",
		Config: json.RawMessage(`{"kind":"Cluster","apiVersion":"kind.x-k8s.io/v1alpha4","nodes":[{"role":"control-plane"}]}`),
	}
	configBytes, err := json.Marshal(clusterConfig)
	if err != nil {
		t.Fatalf("marshal cluster spec: %v", err)
	}

	_, err = deploySvc.Create(ctx, domain.CreateDeploymentInput{
		ID: "kind-deployment",
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{
				ResourceType: kindaddon.ClusterResourceType,
				Raw:          json.RawMessage(configBytes),
			}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"my-kind"},
		},
	})
	if err != nil {
		t.Fatalf("Create deployment: %v", err)
	}

	dep := awaitState(ctx, t, deploymentRepo, "kind-deployment", domain.DeploymentStateActive)
	if len(dep.ResolvedTargets) != 1 {
		t.Fatalf("ResolvedTargets: got %d, want 1", len(dep.ResolvedTargets))
	}
	if dep.ResolvedTargets[0] != "my-kind" {
		t.Errorf("ResolvedTargets[0] = %q, want %q", dep.ResolvedTargets[0], "my-kind")
	}

	<-provider.created
	if !provider.hasCluster("dev-cluster") {
		t.Error("expected kind cluster 'dev-cluster' to be created by the provider")
	}
}

func awaitState(ctx context.Context, t *testing.T, repo *sqlite.DeploymentRepo, id domain.DeploymentID, want domain.DeploymentState) domain.Deployment {
	t.Helper()
	for {
		dep, err := repo.Get(ctx, id)
		if err == nil && dep.State == want {
			return dep
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for deployment %s to reach state %q", id, want)
		case <-time.After(5 * time.Millisecond):
		}
	}
}
