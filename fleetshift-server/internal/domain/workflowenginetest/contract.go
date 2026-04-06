// Package workflowenginetest provides contract tests for [domain.Registry]
// implementations. The test owns all orchestration: it provides infra (repos,
// delivery), builds the domain workflow specs, and calls the registry to
// obtain [domain.OrchestrationWorkflow] and [domain.CreateDeploymentWorkflow].
// The registry implementation just provides [domain.Registry]; it is unaware
// of how the tests work.
package workflowenginetest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// AgentRegistrar allows the contract test to register additional
// delivery agents. Typically backed by [delivery.RoutingDeliveryService].
type AgentRegistrar interface {
	Register(targetType domain.TargetType, agent domain.DeliveryAgent)
}

// Infra is the test-owned infrastructure: store, delivery, vault, and
// agent registration. The same infra is used for all engines;
// implementations do not provide it.
type Infra struct {
	Store          domain.Store
	Delivery       domain.DeliveryService
	Vault          domain.Vault
	AgentRegistrar AgentRegistrar
}

// InfraFactory creates infra for a test. Typically shared across engine tests
// (e.g. sqlite in-memory). Called once per subtest.
type InfraFactory func(t *testing.T) Infra

// RegistryFactory returns the [domain.Registry] under test. The registry
// may perform implementation-specific setup (e.g. launch DBOS, start worker)
// and register t.Cleanup for teardown. The registry is not given workflow
// specs; the contract builds them from infra and passes them to Register.
type RegistryFactory func(t *testing.T) domain.Registry

// workflows holds the registered workflow interfaces used by contract tests.
type workflows struct {
	Orchestration    domain.OrchestrationWorkflow
	CreateDeployment domain.CreateDeploymentWorkflow
}

// Run exercises the [domain.Registry] contract. It uses infraFactory
// to get repos and delivery, builds [OrchestrationWorkflowSpec] and
// [CreateDeploymentWorkflowSpec], calls registry.RegisterOrchestration
// and registry.RegisterCreateDeployment, then runs the same scenarios
// against the returned workflow interfaces and infra.
func Run(t *testing.T, infraFactory InfraFactory, registryFactory RegistryFactory) {
	t.Helper()

	t.Run("CreateDeployment_StaticPlacement", func(t *testing.T) {
		infra := infraFactory(t)
		wfs := registerWorkflows(t, infra, registryFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		registerTargets(ctx, t, infra, "t1", "t2", "t3")

		_, err := runCreateDeployment(ctx, t, wfs, domain.CreateDeploymentInput{
			ID: "d1",
			ManifestStrategy: domain.ManifestStrategySpec{
				Type:      domain.ManifestStrategyInline,
				Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"kind":"ConfigMap"}`)}},
			},
			PlacementStrategy: domain.PlacementStrategySpec{
				Type:    domain.PlacementStrategyStatic,
				Targets: []domain.TargetID{"t1", "t3"},
			},
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}

		dep := awaitDeploymentState(ctx, t, infra, "d1", domain.DeploymentStateActive)
		assertResolvedTargets(t, dep, "t1", "t3")

		records := queryDeliveries(ctx, t, infra, "d1")
		if len(records) != 2 {
			t.Fatalf("expected 2 delivery records, got %d", len(records))
		}
		for _, rec := range records {
			if rec.State != domain.DeliveryStateDelivered {
				t.Errorf("record for %s: State = %q, want %q", rec.TargetID, rec.State, domain.DeliveryStateDelivered)
			}
			if len(rec.Manifests) != 1 {
				t.Errorf("record for %s: Manifests len = %d, want 1", rec.TargetID, len(rec.Manifests))
			}
		}
	})

	t.Run("CreateDeployment_AllPlacement", func(t *testing.T) {
		infra := infraFactory(t)
		wfs := registerWorkflows(t, infra, registryFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		registerTargets(ctx, t, infra, "t1", "t2", "t3")

		_, err := runCreateDeployment(ctx, t, wfs, domain.CreateDeploymentInput{
			ID: "d1",
			ManifestStrategy: domain.ManifestStrategySpec{
				Type:      domain.ManifestStrategyInline,
				Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
			},
			PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll},
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}

		dep := awaitDeploymentState(ctx, t, infra, "d1", domain.DeploymentStateActive)
		assertResolvedTargets(t, dep, "t1", "t2", "t3")

		records := queryDeliveries(ctx, t, infra, "d1")
		if len(records) != 3 {
			t.Fatalf("expected 3 delivery records, got %d", len(records))
		}
	})

	t.Run("CreateDeployment_SelectorPlacement", func(t *testing.T) {
		infra := infraFactory(t)
		wfs := registerWorkflows(t, infra, registryFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		createTargets(ctx, t, infra,
			domain.TargetInfo{ID: "t1", Type: TestTargetType, Name: "cluster-prod", Labels: map[string]string{"env": "prod"}},
			domain.TargetInfo{ID: "t2", Type: TestTargetType, Name: "cluster-staging", Labels: map[string]string{"env": "staging"}},
			domain.TargetInfo{ID: "t3", Type: TestTargetType, Name: "cluster-prod-eu", Labels: map[string]string{"env": "prod"}},
		)

		_, err := runCreateDeployment(ctx, t, wfs, domain.CreateDeploymentInput{
			ID: "d1",
			ManifestStrategy: domain.ManifestStrategySpec{
				Type:      domain.ManifestStrategyInline,
				Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
			},
			PlacementStrategy: domain.PlacementStrategySpec{
				Type:           domain.PlacementStrategySelector,
				TargetSelector: &domain.TargetSelector{MatchLabels: map[string]string{"env": "prod"}},
			},
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}

		dep := awaitDeploymentState(ctx, t, infra, "d1", domain.DeploymentStateActive)
		assertResolvedTargets(t, dep, "t1", "t3")
	})

	t.Run("CreateDeployment_StaticPlacement_UnknownTarget", func(t *testing.T) {
		infra := infraFactory(t)
		wfs := registerWorkflows(t, infra, registryFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		registerTargets(ctx, t, infra, "t1")

		_, err := runCreateDeployment(ctx, t, wfs, domain.CreateDeploymentInput{
			ID: "d1",
			ManifestStrategy: domain.ManifestStrategySpec{
				Type:      domain.ManifestStrategyInline,
				Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
			},
			PlacementStrategy: domain.PlacementStrategySpec{
				Type:    domain.PlacementStrategyStatic,
				Targets: []domain.TargetID{"t1", "missing"},
			},
		})
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("expected ErrNotFound or nil, got: %v", err)
		}
	})

	t.Run("DeleteDeployment_RemovesRecords", func(t *testing.T) {
		infra := infraFactory(t)
		wfs := registerWorkflows(t, infra, registryFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		registerTargets(ctx, t, infra, "t1", "t2")

		_, err := runCreateDeployment(ctx, t, wfs, domain.CreateDeploymentInput{
			ID: "d1",
			ManifestStrategy: domain.ManifestStrategySpec{
				Type:      domain.ManifestStrategyInline,
				Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
			},
			PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll},
		})
		if err != nil {
			t.Fatal(err)
		}

		awaitDeploymentState(ctx, t, infra, "d1", domain.DeploymentStateActive)

		// Delete via the application service, which transitions
		// to Deleting, bumps generation, and triggers reconciliation.
		depSvc := &application.DeploymentService{
			Store:         infra.Store,
			Orchestration: wfs.Orchestration,
		}
		if _, err := depSvc.Delete(ctx, "d1"); err != nil {
			t.Fatalf("Delete: %v", err)
		}

		// Poll until the deployment record is gone.
		for {
			_, err := queryDeployment(ctx, t, infra, "d1")
			if errors.Is(err, domain.ErrNotFound) {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("timed out waiting for deployment to be deleted")
			case <-time.After(50 * time.Millisecond):
			}
		}

		records := queryDeliveries(ctx, t, infra, "d1")
		if len(records) != 0 {
			t.Fatalf("expected 0 delivery records after delete, got %d", len(records))
		}
	})

	t.Run("CreateDeployment_DuplicateID", func(t *testing.T) {
		infra := infraFactory(t)
		wfs := registerWorkflows(t, infra, registryFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		registerTargets(ctx, t, infra, "t1", "t2")

		input := domain.CreateDeploymentInput{
			ID: "d1",
			ManifestStrategy: domain.ManifestStrategySpec{
				Type:      domain.ManifestStrategyInline,
				Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
			},
			PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll},
		}
		_, err := runCreateDeployment(ctx, t, wfs, input)
		if err != nil {
			t.Fatalf("first Create: %v", err)
		}

		_, err = runCreateDeployment(ctx, t, wfs, input)
		if err != nil {
			// Engine rejected duplicate: error should be ErrAlreadyExists (or wrapped).
			if !errors.Is(err, domain.ErrAlreadyExists) {
				t.Logf("second Create returned error (acceptable): %v", err)
			}
			return
		}
		// Engine may be idempotent (same workflow instance ID) and return success.
		dep, err := queryDeployment(ctx, t, infra, "d1")
		if err != nil || dep.ID != "d1" {
			t.Fatalf("second Create succeeded but deployment d1 missing or wrong: %v", err)
		}
	})

	t.Run("CreateDeployment_SelectorPlacement_ZeroMatches", func(t *testing.T) {
		infra := infraFactory(t)
		wfs := registerWorkflows(t, infra, registryFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		createTargets(ctx, t, infra,
			domain.TargetInfo{ID: "t1", Type: TestTargetType, Name: "a", Labels: map[string]string{"env": "prod"}},
			domain.TargetInfo{ID: "t2", Type: TestTargetType, Name: "b", Labels: map[string]string{"env": "staging"}},
		)

		_, err := runCreateDeployment(ctx, t, wfs, domain.CreateDeploymentInput{
			ID: "d1",
			ManifestStrategy: domain.ManifestStrategySpec{
				Type:      domain.ManifestStrategyInline,
				Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
			},
			PlacementStrategy: domain.PlacementStrategySpec{
				Type:           domain.PlacementStrategySelector,
				TargetSelector: &domain.TargetSelector{MatchLabels: map[string]string{"env": "dev"}},
			},
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}

		dep := awaitDeploymentState(ctx, t, infra, "d1", domain.DeploymentStateFailed)
		if len(dep.ResolvedTargets) != 0 {
			t.Fatalf("selector matched no targets: ResolvedTargets = %v, want []", dep.ResolvedTargets)
		}

		records := queryDeliveries(ctx, t, infra, "d1")
		if len(records) != 0 {
			t.Fatalf("expected 0 delivery records, got %d", len(records))
		}
	})

	t.Run("DeliveryOutputs_RegistersTargetAndStoresSecret", func(t *testing.T) {
		infra := infraFactory(t)
		wfs := registerWorkflows(t, infra, registryFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		createTargets(ctx, t, infra,
			domain.TargetInfo{ID: "provisioner", Type: OutputTargetType, Name: "provisioner"},
		)

		_, err := runCreateDeployment(ctx, t, wfs, domain.CreateDeploymentInput{
			ID: "d-outputs",
			ManifestStrategy: domain.ManifestStrategySpec{
				Type:      domain.ManifestStrategyInline,
				Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"name":"new-cluster"}`)}},
			},
			PlacementStrategy: domain.PlacementStrategySpec{
				Type:    domain.PlacementStrategyStatic,
				Targets: []domain.TargetID{"provisioner"},
			},
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}

		awaitDeploymentState(ctx, t, infra, "d-outputs", domain.DeploymentStateActive)

		tx, err := infra.Store.BeginReadOnly(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}

		tgt, err := tx.Targets().Get(ctx, "k8s-new-cluster")
		if err != nil {
			tx.Rollback()
			t.Fatalf("provisioned target not found: %v", err)
		}
		tx.Rollback()

		if tgt.Type != "kubernetes" {
			t.Errorf("target type = %q, want %q", tgt.Type, "kubernetes")
		}
		if tgt.Properties["kubeconfig_ref"] != "targets/k8s-new-cluster/kubeconfig" {
			t.Errorf("target kubeconfig_ref = %q, want %q", tgt.Properties["kubeconfig_ref"], "targets/k8s-new-cluster/kubeconfig")
		}

		if infra.Vault != nil {
			secret, err := infra.Vault.Get(ctx, "targets/k8s-new-cluster/kubeconfig")
			if err != nil {
				t.Fatalf("vault secret not found: %v", err)
			}
			if string(secret) != "fake-kubeconfig-data" {
				t.Errorf("vault secret = %q, want %q", secret, "fake-kubeconfig-data")
			}
		}
	})

	t.Run("TwoDeployments_Isolation", func(t *testing.T) {
		infra := infraFactory(t)
		wfs := registerWorkflows(t, infra, registryFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		registerTargets(ctx, t, infra, "t1", "t2", "t3")

		_, err := runCreateDeployment(ctx, t, wfs, domain.CreateDeploymentInput{
			ID: "d1",
			ManifestStrategy: domain.ManifestStrategySpec{
				Type:      domain.ManifestStrategyInline,
				Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
			},
			PlacementStrategy: domain.PlacementStrategySpec{
				Type:    domain.PlacementStrategyStatic,
				Targets: []domain.TargetID{"t1", "t3"},
			},
		})
		if err != nil {
			t.Fatalf("Create d1: %v", err)
		}

		_, err = runCreateDeployment(ctx, t, wfs, domain.CreateDeploymentInput{
			ID: "d2",
			ManifestStrategy: domain.ManifestStrategySpec{
				Type:      domain.ManifestStrategyInline,
				Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
			},
			PlacementStrategy: domain.PlacementStrategySpec{
				Type:    domain.PlacementStrategyStatic,
				Targets: []domain.TargetID{"t2"},
			},
		})
		if err != nil {
			t.Fatalf("Create d2: %v", err)
		}

		dep1 := awaitDeploymentState(ctx, t, infra, "d1", domain.DeploymentStateActive)
		dep2 := awaitDeploymentState(ctx, t, infra, "d2", domain.DeploymentStateActive)

		assertResolvedTargets(t, dep1, "t1", "t3")
		assertResolvedTargets(t, dep2, "t2")

		records1 := queryDeliveries(ctx, t, infra, "d1")
		records2 := queryDeliveries(ctx, t, infra, "d2")
		if len(records1) != 2 {
			t.Fatalf("d1: expected 2 delivery records, got %d", len(records1))
		}
		if len(records2) != 1 {
			t.Fatalf("d2: expected 1 delivery record, got %d", len(records2))
		}
	})

	t.Run("StartOrchestration_AlreadyRunning", func(t *testing.T) {
		infra := infraFactory(t)
		wfs := registerWorkflows(t, infra, registryFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		registerTargets(ctx, t, infra, "t1")

		// Seed the deployment directly (bypassing create workflow) so
		// there is no prior orchestration instance to race against.
		seedDeployment(ctx, t, infra, domain.Deployment{
			ID:         "dup",
			Generation: 1,
			ManifestStrategy: domain.ManifestStrategySpec{
				Type:      domain.ManifestStrategyInline,
				Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
			},
			PlacementStrategy: domain.PlacementStrategySpec{
				Type:    domain.PlacementStrategyStatic,
				Targets: []domain.TargetID{"t1"},
			},
			State: domain.DeploymentStateCreating,
		})

		// First Start should succeed (no workflow currently running).
		exec, err := wfs.Orchestration.Start(ctx, "dup")
		if err != nil {
			t.Fatalf("first Start: %v", err)
		}

		// Second Start while the first is still running: expect
		// ErrAlreadyRunning (go-workflows, memworkflow) or nil (DBOS).
		_, err = wfs.Orchestration.Start(ctx, "dup")
		if err != nil && !errors.Is(err, domain.ErrAlreadyRunning) {
			t.Fatalf("second Start: got %v, want ErrAlreadyRunning or nil", err)
		}

		// Let the first workflow complete.
		if exec != nil {
			exec.AwaitResult(ctx)
		}
	})

}

// registerWorkflows builds workflow specs from infra, calls
// registry.RegisterOrchestration and registry.RegisterCreateDeployment,
// returns the registered workflow interfaces.
func registerWorkflows(t *testing.T, infra Infra, registryFactory RegistryFactory) workflows {
	t.Helper()
	if infra.AgentRegistrar != nil {
		infra.AgentRegistrar.Register(OutputTargetType, &outputAgent{})
	}
	reg := registryFactory(t)

	orchSpec := &domain.OrchestrationWorkflowSpec{
		Store:      infra.Store,
		Delivery:   infra.Delivery,
		Strategies: domain.DefaultStrategyFactory{},
		Registry:   reg,
		Vault:      infra.Vault,
	}
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	cwfSpec := &domain.CreateDeploymentWorkflowSpec{
		Store:         infra.Store,
		Orchestration: orchWf,
	}
	createWf, err := reg.RegisterCreateDeployment(cwfSpec)
	if err != nil {
		t.Fatalf("RegisterCreateDeployment: %v", err)
	}

	return workflows{
		Orchestration:    orchWf,
		CreateDeployment: createWf,
	}
}

func runCreateDeployment(ctx context.Context, t *testing.T, wfs workflows, in domain.CreateDeploymentInput) (domain.Deployment, error) {
	t.Helper()
	exec, err := wfs.CreateDeployment.Start(ctx, in)
	if err != nil {
		return domain.Deployment{}, err
	}
	return exec.AwaitResult(ctx)
}

// TestTargetType is the default target type used by contract tests.
const TestTargetType domain.TargetType = "test"

// OutputTargetType is a target type whose delivery agent produces
// [domain.ProvisionedTarget] and [domain.ProducedSecret] outputs.
// Used by the delivery-outputs contract test.
const OutputTargetType domain.TargetType = "output-test"

func registerTargets(ctx context.Context, t *testing.T, infra Infra, ids ...string) {
	t.Helper()
	tx, err := infra.Store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()
	for _, id := range ids {
		must(t, tx.Targets().Create(ctx, domain.TargetInfo{
			ID:   domain.TargetID(id),
			Type: TestTargetType,
			Name: "cluster-" + id,
		}))
	}
	must(t, tx.Commit())
}

func awaitDeploymentState(ctx context.Context, t *testing.T, infra Infra, id domain.DeploymentID, want domain.DeploymentState) domain.Deployment {
	t.Helper()
	for {
		tx, err := infra.Store.BeginReadOnly(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		dep, err := tx.Deployments().Get(ctx, id)
		tx.Rollback()
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get(%s): %v", id, err)
		}
		if err == nil && dep.State == want {
			return dep
		}
		select {
		case <-ctx.Done():
			last := domain.DeploymentState("")
			if err == nil {
				last = dep.State
			}
			t.Fatalf("timed out waiting for deployment %s to reach state %q (last: %q)", id, want, last)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func assertResolvedTargets(t *testing.T, dep domain.Deployment, expectedIDs ...string) {
	t.Helper()
	if len(dep.ResolvedTargets) != len(expectedIDs) {
		t.Fatalf("ResolvedTargets: got %d, want %d", len(dep.ResolvedTargets), len(expectedIDs))
	}
	got := make(map[domain.TargetID]bool)
	for _, id := range dep.ResolvedTargets {
		got[id] = true
	}
	for _, id := range expectedIDs {
		if !got[domain.TargetID(id)] {
			t.Errorf("expected target %q in ResolvedTargets", id)
		}
	}
}

func queryDeliveries(ctx context.Context, t *testing.T, infra Infra, depID domain.DeploymentID) []domain.Delivery {
	t.Helper()
	tx, err := infra.Store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()
	records, err := tx.Deliveries().ListByDeployment(ctx, depID)
	if err != nil {
		t.Fatalf("ListByDeployment: %v", err)
	}
	return records
}

func queryDeployment(ctx context.Context, t *testing.T, infra Infra, id domain.DeploymentID) (domain.Deployment, error) {
	t.Helper()
	tx, err := infra.Store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()
	return tx.Deployments().Get(ctx, id)
}


func seedDeployment(ctx context.Context, t *testing.T, infra Infra, dep domain.Deployment) {
	t.Helper()
	if dep.UID == "" {
		dep.UID = "test-uid"
	}
	if dep.Etag == "" {
		dep.Etag = "test-etag"
	}
	if dep.CreatedAt.IsZero() {
		dep.CreatedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	if dep.UpdatedAt.IsZero() {
		dep.UpdatedAt = dep.CreatedAt
	}
	tx, err := infra.Store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()
	must(t, tx.Deployments().Create(ctx, dep))
	must(t, tx.Commit())
}

func createTargets(ctx context.Context, t *testing.T, infra Infra, targets ...domain.TargetInfo) {
	t.Helper()
	tx, err := infra.Store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()
	for _, tgt := range targets {
		must(t, tx.Targets().Create(ctx, tgt))
	}
	must(t, tx.Commit())
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// outputAgent implements [domain.DeliveryAgent] by producing a
// [domain.ProvisionedTarget] and [domain.ProducedSecret] from each
// delivery. The manifest's "name" field determines the target ID.
type outputAgent struct{}

func (a *outputAgent) Deliver(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, manifests []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	var spec struct{ Name string }
	if err := json.Unmarshal(manifests[0].Raw, &spec); err != nil {
		return domain.DeliveryResult{State: domain.DeliveryStateFailed, Message: err.Error()}, err
	}
	targetID := domain.TargetID("k8s-" + spec.Name)
	secretRef := domain.SecretRef("targets/" + string(targetID) + "/kubeconfig")

	go signaler.Done(context.Background(), domain.DeliveryResult{
		State: domain.DeliveryStateDelivered,
		ProvisionedTargets: []domain.ProvisionedTarget{{
			ID:   targetID,
			Type: "kubernetes",
			Name: spec.Name,
			Properties: map[string]string{
				"kubeconfig_ref": string(secretRef),
			},
		}},
		ProducedSecrets: []domain.ProducedSecret{{
			Ref:   secretRef,
			Value: []byte("fake-kubeconfig-data"),
		}},
	})

	return domain.DeliveryResult{State: domain.DeliveryStateAccepted}, nil
}

func (a *outputAgent) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ *domain.DeliverySignaler) error {
	return nil
}
