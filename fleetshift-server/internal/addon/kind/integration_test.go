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

// buildReporter constructs a [application.DeliveryReportService] wired
// to a [memworkflow.Registry] for integration tests.
func buildReporter(store domain.Store, reg *memworkflow.Registry) *application.DeliveryReportService {
	return application.NewDeliveryReportService(store, reg)
}

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

	reg := &memworkflow.Registry{}
	reporter := buildReporter(store, reg)

	provider := newFakeProvider()
	kindAgent := kindaddon.NewAgent(reporter, fakeFactory(provider))
	router := delivery.NewRoutingDeliveryService()
	router.Register(kindaddon.TargetType, kindAgent)

	orchSpec := domain.NewOrchestrationWorkflowSpec(
		store, router, domain.StrategyFactory{Store: store}, reg,
		domain.WithAckRetryInterval(5*time.Second),
	)
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

	cleanupSpec := &domain.DeleteDeploymentCleanupWorkflowSpec{Store: store}
	cleanupWf, err := reg.RegisterDeleteDeploymentCleanup(cleanupSpec)
	if err != nil {
		t.Fatalf("RegisterDeleteDeploymentCleanup: %v", err)
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

	if err := targetSvc.Register(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:     "my-kind",
		Type:   kindaddon.TargetType,
		Name:   "Local Kind Provider",
		Labels: map[string]string{"env": "dev"},
	})); err != nil {
		t.Fatalf("Register target: %v", err)
	}

	clusterConfig := kindaddon.ClusterSpec{
		Name:  "dev-cluster",
		Nodes: []kindaddon.NodeSpec{{Role: "control-plane"}},
	}
	configBytes, err := json.Marshal(clusterConfig)
	if err != nil {
		t.Fatalf("marshal cluster spec: %v", err)
	}

	_, err = deploySvc.Create(ctx, domain.CreateDeploymentInput{
		Name: "deployments/kind-deployment",
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{
				ManifestType: kindaddon.ClusterManifestType,
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

	view := awaitState(ctx, t, store, "deployments/kind-deployment", domain.FulfillmentStateActive)
	if len(view.Fulfillment.ResolvedTargets()) != 1 {
		t.Fatalf("ResolvedTargets: got %d, want 1", len(view.Fulfillment.ResolvedTargets()))
	}
	if view.Fulfillment.ResolvedTargets()[0] != "my-kind" {
		t.Errorf("ResolvedTargets[0] = %q, want %q", view.Fulfillment.ResolvedTargets()[0], "my-kind")
	}

	<-provider.created
	if !provider.hasCluster("dev-cluster") {
		t.Error("expected kind cluster 'dev-cluster' to be created by the provider")
	}
}

// TestKindAddon_ManagedResource_EndToEnd exercises the managed resource
// path through to the kind delivery agent:
//
//  1. Register a kind delivery agent with the routing service.
//  2. Register a target that accepts kind cluster resources.
//  3. Register the kind managed resource type.
//  4. Create a managed resource via the service.
//  5. Verify the fulfillment reaches Active and the fake provider
//     received the cluster creation.
//
// Auth threading (DeliveryAuth.Caller propagation) is covered
// separately in application/e2e_managed_resource_test.go because
// a non-nil Caller triggers RBAC bootstrap which requires a live
// Kubernetes API server.
func TestKindAddon_ManagedResource_EndToEnd(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	reg := &memworkflow.Registry{}
	reporter := buildReporter(store, reg)

	provider := newFakeProvider()
	kindAgent := kindaddon.NewAgent(reporter, fakeFactory(provider))
	router := delivery.NewRoutingDeliveryService()
	router.Register(kindaddon.TargetType, kindAgent)

	orchSpec := domain.NewOrchestrationWorkflowSpec(
		store, router, domain.StrategyFactory{Store: store}, reg,
		domain.WithAckRetryInterval(5*time.Second),
	)
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	createMRSpec := &domain.CreateManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	}
	createMRWf, err := reg.RegisterCreateManagedResource(createMRSpec)
	if err != nil {
		t.Fatalf("RegisterCreateManagedResource: %v", err)
	}

	typeSvc := application.NewManagedResourceTypeService(store)
	resourceSvc := &application.ManagedResourceService{
		Store:    store,
		CreateWF: createMRWf,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// --- Step 1-2: Register target ---
	{
		tx, _ := store.Begin(ctx)
		_ = tx.Targets().Create(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
			ID:                    "kind-local",
			Type:                  kindaddon.TargetType,
			Name:                  "Local Kind Provider",
			AcceptedManifestTypes: []domain.ManifestType{kindaddon.ClusterManifestType},
		}))
		_ = tx.Commit()
	}

	// --- Step 3: Register managed resource type ---
	_, err = typeSvc.Create(ctx, application.CreateTypeInput{
		ResourceType:   kindaddon.ClusterResourceType,
		Relation:       domain.NewRegisteredSelfTarget("kind-local", kindaddon.ClusterManifestType),
		APIServiceName: "kind.fleetshift.io",
		APIVersion:     "v1",
		CollectionID:   "clusters",
		Signature: domain.Signature{
			Signer:         domain.FederatedIdentity{Subject: "kind-addon", Issuer: "https://kind.test"},
			ContentHash:    []byte("hash"),
			SignatureBytes: []byte("sig"),
		},
	})
	if err != nil {
		t.Fatalf("RegisterType: %v", err)
	}

	// --- Step 4: Create managed resource ---
	spec := json.RawMessage(`{"name":"mr-cluster","nodes":[{"role":"control-plane"},{"role":"worker"}]}`)

	view, err := resourceSvc.Create(ctx, application.CreateManagedResourceInput{
		ResourceType: kindaddon.ClusterResourceType,
		Name:         "mr-cluster",
		Spec:         spec,
	})
	if err != nil {
		t.Fatalf("Create managed resource: %v", err)
	}

	// --- Step 5: Wait for delivery and verify ---
	awaitFulfillment(ctx, t, store, view.Fulfillment.ID(), domain.FulfillmentStateActive)

	<-provider.created
	if !provider.hasCluster("mr-cluster") {
		t.Error("expected kind cluster 'mr-cluster' to be created by the provider")
	}
}

func awaitState(ctx context.Context, t *testing.T, store domain.Store, name domain.ResourceName, want domain.FulfillmentState) domain.DeploymentView {
	t.Helper()
	for {
		tx, err := store.BeginReadOnly(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		view, err := tx.Deployments().GetView(ctx, name)
		tx.Rollback()
		if err == nil && view.Fulfillment.State() == want {
			return view
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for deployment %s to reach state %q", name, want)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func awaitFulfillment(ctx context.Context, t *testing.T, store domain.Store, fID domain.FulfillmentID, want domain.FulfillmentState) {
	t.Helper()
	for {
		tx, err := store.BeginReadOnly(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		f, err := tx.Fulfillments().Get(ctx, fID)
		tx.Rollback()
		if err == nil && f.State() == want {
			return
		}
		select {
		case <-ctx.Done():
			var state domain.FulfillmentState
			if f != nil {
				state = f.State()
			}
			t.Fatalf("timed out waiting for fulfillment %s to reach state %q (current: %q)", fID, want, state)
		case <-time.After(5 * time.Millisecond):
		}
	}
}
