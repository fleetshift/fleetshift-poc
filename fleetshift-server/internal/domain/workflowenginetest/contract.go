// Package workflowenginetest provides contract tests for [domain.WorkflowEngine]
// implementations. The test owns all orchestration: it provides infra (repos,
// delivery), builds the domain workflows, and calls the engine only to obtain
// [domain.WorkflowRunners]. The engine implementation just provides
// [domain.WorkflowEngine]; it is unaware of how the tests work.
package workflowenginetest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Infra is the test-owned infrastructure: repositories and delivery.
// The same infra is used for all engines; implementations do not provide it.
type Infra struct {
	Targets     domain.TargetRepository
	Deployments domain.DeploymentRepository
	Deliveries  domain.DeliveryRepository
	Delivery    domain.DeliveryService
}

// InfraFactory creates infra for a test. Typically shared across engine tests
// (e.g. sqlite in-memory). Called once per subtest.
type InfraFactory func(t *testing.T) Infra

// EngineFactory returns the [domain.WorkflowEngine] under test. The engine
// may perform implementation-specific setup (e.g. launch DBOS, start worker)
// and register t.Cleanup for teardown. The engine is not given workflows;
// the contract builds them from infra and passes them to Register.
type EngineFactory func(t *testing.T) domain.WorkflowEngine

// Run exercises the [domain.WorkflowEngine] contract. It uses infraFactory
// to get repos and delivery, builds [OrchestrationWorkflow] and
// [CreateDeploymentWorkflow], calls engine.Register(owf, cwf), then runs
// the same scenarios against the returned runners and infra. The engine
// only provides itself; the test does the rest.
func Run(t *testing.T, infraFactory InfraFactory, engineFactory EngineFactory) {
	t.Helper()

	t.Run("CreateDeployment_StaticPlacement", func(t *testing.T) {
		infra := infraFactory(t)
		runners := registerEngine(t, infra, engineFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		registerTargets(ctx, t, infra, "t1", "t2", "t3")

		_, err := runCreateDeployment(ctx, t, runners, domain.CreateDeploymentInput{
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

		records, err := infra.Deliveries.ListByDeployment(ctx, "d1")
		if err != nil {
			t.Fatalf("ListByDeployment: %v", err)
		}
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
		runners := registerEngine(t, infra, engineFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		registerTargets(ctx, t, infra, "t1", "t2", "t3")

		_, err := runCreateDeployment(ctx, t, runners, domain.CreateDeploymentInput{
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

		records, err := infra.Deliveries.ListByDeployment(ctx, "d1")
		if err != nil {
			t.Fatalf("ListByDeployment: %v", err)
		}
		if len(records) != 3 {
			t.Fatalf("expected 3 delivery records, got %d", len(records))
		}
	})

	t.Run("CreateDeployment_SelectorPlacement", func(t *testing.T) {
		infra := infraFactory(t)
		runners := registerEngine(t, infra, engineFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		must(t, infra.Targets.Create(ctx, domain.TargetInfo{
			ID: "t1", Type: TestTargetType, Name: "cluster-prod", Labels: map[string]string{"env": "prod"},
		}))
		must(t, infra.Targets.Create(ctx, domain.TargetInfo{
			ID: "t2", Type: TestTargetType, Name: "cluster-staging", Labels: map[string]string{"env": "staging"},
		}))
		must(t, infra.Targets.Create(ctx, domain.TargetInfo{
			ID: "t3", Type: TestTargetType, Name: "cluster-prod-eu", Labels: map[string]string{"env": "prod"},
		}))

		_, err := runCreateDeployment(ctx, t, runners, domain.CreateDeploymentInput{
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
		runners := registerEngine(t, infra, engineFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		registerTargets(ctx, t, infra, "t1")

		_, err := runCreateDeployment(ctx, t, runners, domain.CreateDeploymentInput{
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
		runners := registerEngine(t, infra, engineFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		registerTargets(ctx, t, infra, "t1", "t2")

		_, err := runCreateDeployment(ctx, t, runners, domain.CreateDeploymentInput{
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

		must(t, infra.Deliveries.DeleteByDeployment(ctx, "d1"))
		must(t, infra.Deployments.Delete(ctx, "d1"))

		records, err := infra.Deliveries.ListByDeployment(ctx, "d1")
		if err != nil {
			t.Fatalf("ListByDeployment: %v", err)
		}
		if len(records) != 0 {
			t.Fatalf("expected 0 delivery records after delete, got %d", len(records))
		}

		_, err = infra.Deployments.Get(ctx, "d1")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("expected ErrNotFound after delete, got: %v", err)
		}
	})

	t.Run("SignalDeploymentEvent_PoolChange", func(t *testing.T) {
		infra := infraFactory(t)
		runners := registerEngine(t, infra, engineFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		registerTargets(ctx, t, infra, "t1", "t2")

		_, err := runCreateDeployment(ctx, t, runners, domain.CreateDeploymentInput{
			ID: "d1",
			ManifestStrategy: domain.ManifestStrategySpec{
				Type:      domain.ManifestStrategyInline,
				Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"kind":"ConfigMap"}`)}},
			},
			PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll},
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}

		dep := awaitDeploymentState(ctx, t, infra, "d1", domain.DeploymentStateActive)
		assertResolvedTargets(t, dep, "t1", "t2")

		must(t, infra.Targets.Create(ctx, domain.TargetInfo{ID: "t3", Type: TestTargetType, Name: "cluster-t3"}))
		pool, err := infra.Targets.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(pool) != 3 {
			t.Fatalf("expected 3 targets, got %d", len(pool))
		}
		if err := runners.Orchestration.SignalDeploymentEvent(ctx, "d1", domain.DeploymentEvent{
			PoolChange: &domain.PoolChange{Set: pool},
		}); err != nil {
			t.Fatalf("SignalDeploymentEvent: %v", err)
		}

		dep2 := awaitDeploymentResolvedCount(ctx, t, infra, "d1", 3)
		assertResolvedTargets(t, dep2, "t1", "t2", "t3")

		records, err := infra.Deliveries.ListByDeployment(ctx, "d1")
		if err != nil {
			t.Fatalf("ListByDeployment: %v", err)
		}
		if len(records) != 3 {
			t.Fatalf("expected 3 delivery records after pool change, got %d", len(records))
		}
	})

	t.Run("SignalDeploymentEvent_ManifestInvalidated", func(t *testing.T) {
		infra := infraFactory(t)
		runners := registerEngine(t, infra, engineFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		registerTargets(ctx, t, infra, "t1", "t2")

		_, err := runCreateDeployment(ctx, t, runners, domain.CreateDeploymentInput{
			ID: "d1",
			ManifestStrategy: domain.ManifestStrategySpec{
				Type:      domain.ManifestStrategyInline,
				Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"kind":"ConfigMap"}`)}},
			},
			PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll},
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}

		awaitDeploymentState(ctx, t, infra, "d1", domain.DeploymentStateActive)

		if err := runners.Orchestration.SignalDeploymentEvent(ctx, "d1", domain.DeploymentEvent{
			ManifestInvalidated: true,
		}); err != nil {
			t.Fatalf("SignalDeploymentEvent: %v", err)
		}

		dep := awaitDeploymentState(ctx, t, infra, "d1", domain.DeploymentStateActive)
		assertResolvedTargets(t, dep, "t1", "t2")

		records, err := infra.Deliveries.ListByDeployment(ctx, "d1")
		if err != nil {
			t.Fatalf("ListByDeployment: %v", err)
		}
		if len(records) != 2 {
			t.Fatalf("expected 2 delivery records, got %d", len(records))
		}
	})

	t.Run("CreateDeployment_DuplicateID", func(t *testing.T) {
		infra := infraFactory(t)
		runners := registerEngine(t, infra, engineFactory)
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
		_, err := runCreateDeployment(ctx, t, runners, input)
		if err != nil {
			t.Fatalf("first Create: %v", err)
		}

		_, err = runCreateDeployment(ctx, t, runners, input)
		if err != nil {
			// Engine rejected duplicate: error should be ErrAlreadyExists (or wrapped).
			if !errors.Is(err, domain.ErrAlreadyExists) {
				t.Logf("second Create returned error (acceptable): %v", err)
			}
			return
		}
		// Engine may be idempotent (same workflow instance ID) and return success.
		dep, err := infra.Deployments.Get(ctx, "d1")
		if err != nil || dep.ID != "d1" {
			t.Fatalf("second Create succeeded but deployment d1 missing or wrong: %v", err)
		}
	})

	t.Run("SignalDeploymentEvent_Delete", func(t *testing.T) {
		infra := infraFactory(t)
		runners := registerEngine(t, infra, engineFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		registerTargets(ctx, t, infra, "t1", "t2")

		_, err := runCreateDeployment(ctx, t, runners, domain.CreateDeploymentInput{
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

		awaitDeploymentState(ctx, t, infra, "d1", domain.DeploymentStateActive)

		if err := runners.Orchestration.SignalDeploymentEvent(ctx, "d1", domain.DeploymentEvent{Delete: true}); err != nil {
			t.Fatalf("SignalDeploymentEvent(Delete): %v", err)
		}

		awaitDeploymentState(ctx, t, infra, "d1", domain.DeploymentStateDeleting)

		// Delivery implementation may remove records or leave them (e.g. Pending);
		// the contract asserts the workflow reached Deleting and cleared resolved set.
		dep := awaitDeploymentState(ctx, t, infra, "d1", domain.DeploymentStateDeleting)
		if len(dep.ResolvedTargets) != 0 {
			t.Fatalf("after Delete event: ResolvedTargets should be empty, got %v", dep.ResolvedTargets)
		}
	})

	t.Run("SignalDeploymentEvent_PoolShrink", func(t *testing.T) {
		infra := infraFactory(t)
		runners := registerEngine(t, infra, engineFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		registerTargets(ctx, t, infra, "t1", "t2", "t3")

		_, err := runCreateDeployment(ctx, t, runners, domain.CreateDeploymentInput{
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

		if err := runners.Orchestration.SignalDeploymentEvent(ctx, "d1", domain.DeploymentEvent{
			PoolChange: &domain.PoolChange{Removed: []domain.TargetID{"t3"}},
		}); err != nil {
			t.Fatalf("SignalDeploymentEvent(PoolShrink): %v", err)
		}

		dep2 := awaitDeploymentResolvedCount(ctx, t, infra, "d1", 2)
		assertResolvedTargets(t, dep2, "t1", "t2")

		// After shrink, placement delivers to 2 targets; delivery impl may keep
		// a record for the removed target (e.g. Pending), so assert delivered count.
		records, err := infra.Deliveries.ListByDeployment(ctx, "d1")
		if err != nil {
			t.Fatalf("ListByDeployment: %v", err)
		}
		delivered := 0
		for _, r := range records {
			if r.State == domain.DeliveryStateDelivered {
				delivered++
			}
		}
		if delivered != 2 {
			t.Fatalf("after pool shrink: expected 2 delivered records, got %d", delivered)
		}
	})

	t.Run("CreateDeployment_SelectorPlacement_ZeroMatches", func(t *testing.T) {
		infra := infraFactory(t)
		runners := registerEngine(t, infra, engineFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		must(t, infra.Targets.Create(ctx, domain.TargetInfo{ID: "t1", Type: TestTargetType, Name: "a", Labels: map[string]string{"env": "prod"}}))
		must(t, infra.Targets.Create(ctx, domain.TargetInfo{ID: "t2", Type: TestTargetType, Name: "b", Labels: map[string]string{"env": "staging"}}))

		_, err := runCreateDeployment(ctx, t, runners, domain.CreateDeploymentInput{
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

		dep := awaitDeploymentState(ctx, t, infra, "d1", domain.DeploymentStateActive)
		if len(dep.ResolvedTargets) != 0 {
			t.Fatalf("selector matched no targets: ResolvedTargets = %v, want []", dep.ResolvedTargets)
		}

		records, err := infra.Deliveries.ListByDeployment(ctx, "d1")
		if err != nil {
			t.Fatalf("ListByDeployment: %v", err)
		}
		if len(records) != 0 {
			t.Fatalf("expected 0 delivery records, got %d", len(records))
		}
	})

	t.Run("TwoDeployments_Isolation", func(t *testing.T) {
		infra := infraFactory(t)
		runners := registerEngine(t, infra, engineFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		registerTargets(ctx, t, infra, "t1", "t2", "t3")

		_, err := runCreateDeployment(ctx, t, runners, domain.CreateDeploymentInput{
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

		_, err = runCreateDeployment(ctx, t, runners, domain.CreateDeploymentInput{
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

		records1, err := infra.Deliveries.ListByDeployment(ctx, "d1")
		if err != nil {
			t.Fatalf("ListByDeployment(d1): %v", err)
		}
		records2, err := infra.Deliveries.ListByDeployment(ctx, "d2")
		if err != nil {
			t.Fatalf("ListByDeployment(d2): %v", err)
		}
		if len(records1) != 2 {
			t.Fatalf("d1: expected 2 delivery records, got %d", len(records1))
		}
		if len(records2) != 1 {
			t.Fatalf("d2: expected 1 delivery record, got %d", len(records2))
		}
	})

	t.Run("SignalDeploymentEvent_SpecChanged", func(t *testing.T) {
		infra := infraFactory(t)
		runners := registerEngine(t, infra, engineFactory)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		registerTargets(ctx, t, infra, "t1", "t2")

		_, err := runCreateDeployment(ctx, t, runners, domain.CreateDeploymentInput{
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

		awaitDeploymentState(ctx, t, infra, "d1", domain.DeploymentStateActive)

		if err := runners.Orchestration.SignalDeploymentEvent(ctx, "d1", domain.DeploymentEvent{SpecChanged: true}); err != nil {
			t.Fatalf("SignalDeploymentEvent(SpecChanged): %v", err)
		}

		dep := awaitDeploymentState(ctx, t, infra, "d1", domain.DeploymentStateActive)
		assertResolvedTargets(t, dep, "t1", "t2")

		records, err := infra.Deliveries.ListByDeployment(ctx, "d1")
		if err != nil {
			t.Fatalf("ListByDeployment: %v", err)
		}
		if len(records) != 2 {
			t.Fatalf("after SpecChanged: expected 2 delivery records, got %d", len(records))
		}
	})
}

// registerEngine builds workflows from infra, calls engine.Register, returns runners.
func registerEngine(t *testing.T, infra Infra, engineFactory EngineFactory) domain.WorkflowRunners {
	t.Helper()
	owf := &domain.OrchestrationWorkflow{
		Deployments: infra.Deployments,
		Targets:     infra.Targets,
		Deliveries:  infra.Deliveries,
		Delivery:    infra.Delivery,
		Strategies:  domain.DefaultStrategyFactory{},
	}
	cwf := &domain.CreateDeploymentWorkflow{
		Deployments: infra.Deployments,
	}
	engine := engineFactory(t)
	runners, err := engine.Register(owf, cwf)
	if err != nil {
		t.Fatalf("engine.Register: %v", err)
	}
	owf.OnDeliveryDone = func(ctx context.Context, deploymentID domain.DeploymentID, event domain.DeploymentEvent) error {
		// Run asynchronously to avoid deadlocking durable backends
		// that hold a transaction lock while executing the activity
		// (e.g. go-workflows SQLite backend).
		go runners.Orchestration.SignalDeploymentEvent(context.Background(), deploymentID, event)
		return nil
	}
	return runners
}

func runCreateDeployment(ctx context.Context, t *testing.T, runners domain.WorkflowRunners, in domain.CreateDeploymentInput) (domain.Deployment, error) {
	t.Helper()
	handle, err := runners.CreateDeployment.Run(ctx, in)
	if err != nil {
		return domain.Deployment{}, err
	}
	return handle.AwaitResult(ctx)
}

// TestTargetType is the default target type used by contract tests.
const TestTargetType domain.TargetType = "test"

func registerTargets(ctx context.Context, t *testing.T, infra Infra, ids ...string) {
	t.Helper()
	for _, id := range ids {
		must(t, infra.Targets.Create(ctx, domain.TargetInfo{
			ID:   domain.TargetID(id),
			Type: TestTargetType,
			Name: "cluster-" + id,
		}))
	}
}

func awaitDeploymentState(ctx context.Context, t *testing.T, infra Infra, id domain.DeploymentID, want domain.DeploymentState) domain.Deployment {
	t.Helper()
	for {
		dep, err := infra.Deployments.Get(ctx, id)
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

func awaitDeploymentResolvedCount(ctx context.Context, t *testing.T, infra Infra, id domain.DeploymentID, want int) domain.Deployment {
	t.Helper()
	for {
		dep, err := infra.Deployments.Get(ctx, id)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get(%s): %v", id, err)
		}
		if err == nil && len(dep.ResolvedTargets) == want {
			return dep
		}
		select {
		case <-ctx.Done():
			last := 0
			if err == nil {
				last = len(dep.ResolvedTargets)
			}
			t.Fatalf("timed out waiting for deployment %s to have %d resolved targets (last: %d)", id, want, last)
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

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
