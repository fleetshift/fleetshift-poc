package kubernetes_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	kubernetes "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// fakeVault is a simple in-memory Vault for testing.
type fakeVault struct {
	secrets map[domain.SecretRef][]byte
}

func (v *fakeVault) Get(_ context.Context, ref domain.SecretRef) ([]byte, error) {
	val, ok := v.secrets[ref]
	if !ok {
		return nil, fmt.Errorf("secret %q not found", ref)
	}
	return val, nil
}

func (v *fakeVault) Put(_ context.Context, ref domain.SecretRef, val []byte) error {
	v.secrets[ref] = val
	return nil
}

func (v *fakeVault) Delete(_ context.Context, ref domain.SecretRef) error {
	delete(v.secrets, ref)
	return nil
}

func newTestManager(t *testing.T) *kubernetes.AgentPool {
	t.Helper()
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	vault := &fakeVault{secrets: make(map[domain.SecretRef][]byte)}
	logger := slog.Default()
	return kubernetes.NewAgentPool(context.Background(), store, vault, nil, nopReporter{}, nil, nil, logger)
}

func testTarget(id string) domain.TargetInfo {
	return domain.NewTargetInfo(
		domain.TargetID(id),
		"kubernetes",
		"Test Cluster",
		domain.TargetStateReady,
		nil,
		map[string]string{
			"api_server":            "https://127.0.0.1:6443",
			"service_account_token": "fake-token",
		},
		nil,
	)
}

func TestStartIndexing_StartsAgent(t *testing.T) {
	mgr := newTestManager(t)
	t.Cleanup(mgr.StopAll)

	ctx := context.Background()
	target := testTarget("test-target")

	if err := mgr.StartIndexing(ctx, target); err != nil {
		t.Fatalf("StartIndexing: %v", err)
	}

	ta := mgr.GetAgent("test-target")
	if ta == nil {
		t.Fatal("expected GetAgent to return agent, got nil")
	}
	if ta.TargetID() != "test-target" {
		t.Errorf("TargetID = %q, want %q", ta.TargetID(), "test-target")
	}
}

func TestStartIndexing_Idempotent(t *testing.T) {
	mgr := newTestManager(t)
	t.Cleanup(mgr.StopAll)

	ctx := context.Background()
	target := testTarget("test-target")

	if err := mgr.StartIndexing(ctx, target); err != nil {
		t.Fatalf("first StartIndexing: %v", err)
	}

	// Second call should be a no-op.
	if err := mgr.StartIndexing(ctx, target); err != nil {
		t.Fatalf("second StartIndexing: %v", err)
	}

	ta := mgr.GetAgent("test-target")
	if ta == nil {
		t.Fatal("expected GetAgent to return agent, got nil")
	}
}

func TestStopIndexing_StopsAgent(t *testing.T) {
	mgr := newTestManager(t)

	ctx := context.Background()
	target := testTarget("test-target")

	if err := mgr.StartIndexing(ctx, target); err != nil {
		t.Fatalf("StartIndexing: %v", err)
	}

	ta := mgr.GetAgent("test-target")
	if ta == nil {
		t.Fatal("expected agent to be running")
	}

	if err := mgr.StopIndexing(ctx, target); err != nil {
		t.Fatalf("StopIndexing: %v", err)
	}

	// Agent should be stopped and removed.
	if mgr.GetAgent("test-target") != nil {
		t.Error("expected GetAgent to return nil after stop")
	}

	// Done channel should be closed.
	select {
	case <-ta.Done():
		// expected
	default:
		t.Error("expected Done channel to be closed after Stop")
	}
}

func TestStartIndexing_AgentSurvivesCallerCancel(t *testing.T) {
	mgr := newTestManager(t)
	t.Cleanup(mgr.StopAll)

	callerCtx, callerCancel := context.WithCancel(context.Background())

	if err := mgr.StartIndexing(callerCtx, testTarget("test-target")); err != nil {
		t.Fatalf("StartIndexing: %v", err)
	}

	ta := mgr.GetAgent("test-target")
	if ta == nil {
		t.Fatal("expected agent to be running")
	}

	// Simulate the caller's context being cancelled (e.g. a Temporal
	// activity returning). The agent must stay alive.
	callerCancel()

	select {
	case <-ta.Done():
		t.Fatal("agent stopped when caller context was cancelled — agent lifetime must not depend on caller context")
	default:
		// expected: agent is still running
	}
}

func TestGetAgent_UnknownReturnsNil(t *testing.T) {
	mgr := newTestManager(t)

	if ta := mgr.GetAgent("nonexistent"); ta != nil {
		t.Errorf("expected nil for unknown target, got %v", ta)
	}
}

func TestStopAll_StopsAllAgents(t *testing.T) {
	mgr := newTestManager(t)

	ctx := context.Background()

	for _, id := range []string{"target-a", "target-b", "target-c"} {
		if err := mgr.StartIndexing(ctx, testTarget(id)); err != nil {
			t.Fatalf("StartIndexing(%s): %v", id, err)
		}
	}

	// Verify all are running.
	for _, id := range []string{"target-a", "target-b", "target-c"} {
		if mgr.GetAgent(domain.TargetID(id)) == nil {
			t.Fatalf("expected agent %s to be running", id)
		}
	}

	mgr.StopAll()

	// All should be gone.
	for _, id := range []string{"target-a", "target-b", "target-c"} {
		if mgr.GetAgent(domain.TargetID(id)) != nil {
			t.Errorf("expected agent %s to be stopped after StopAll", id)
		}
	}
}
