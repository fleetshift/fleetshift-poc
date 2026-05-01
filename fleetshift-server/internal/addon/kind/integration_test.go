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
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
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
	store := &sqlite.Store{DB: db}

	provider := newFakeProvider()
	kindAgent := kindaddon.NewAgent(fakeFactory(provider))
	router := delivery.NewRoutingDeliveryService()
	router.Register(kindaddon.TargetType, kindAgent)

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

	view := awaitState(ctx, t, store, "kind-deployment", domain.FulfillmentStateActive)
	if len(view.Fulfillment.ResolvedTargets) != 1 {
		t.Fatalf("ResolvedTargets: got %d, want 1", len(view.Fulfillment.ResolvedTargets))
	}
	if view.Fulfillment.ResolvedTargets[0] != "my-kind" {
		t.Errorf("ResolvedTargets[0] = %q, want %q", view.Fulfillment.ResolvedTargets[0], "my-kind")
	}

	<-provider.created
	if !provider.hasCluster("dev-cluster") {
		t.Error("expected kind cluster 'dev-cluster' to be created by the provider")
	}
}

func awaitState(ctx context.Context, t *testing.T, store domain.Store, id domain.DeploymentID, want domain.FulfillmentState) domain.DeploymentView {
	t.Helper()
	for {
		tx, err := store.BeginReadOnly(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		view, err := tx.Deployments().GetView(ctx, id)
		tx.Rollback()
		if err == nil && view.Fulfillment.State == want {
			return view
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for deployment %s to reach state %q", id, want)
		case <-time.After(5 * time.Millisecond):
		}
	}
}
