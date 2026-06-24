package kubernetes_test

import (
	"context"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// TestAgent_Properties verifies that the Agent created by
// StartIndexing exposes the correct API server and K8s clients.
func TestAgent_Properties(t *testing.T) {
	mgr := newTestManager(t)
	t.Cleanup(mgr.StopAll)

	target := domain.NewTargetInfo(
		"prop-test",
		"kubernetes",
		"Property Test",
		domain.TargetStateReady,
		nil,
		map[string]string{
			"api_server":            "https://127.0.0.1:6443",
			"service_account_token": "my-token",
		},
		nil,
	)

	ctx := context.Background()
	if err := mgr.StartIndexing(ctx, target); err != nil {
		t.Fatalf("StartIndexing: %v", err)
	}

	ta := mgr.GetAgent("prop-test")
	if ta == nil {
		t.Fatal("expected agent, got nil")
	}

	if got := ta.APIServer(); got != "https://127.0.0.1:6443" {
		t.Errorf("APIServer() = %q, want %q", got, "https://127.0.0.1:6443")
	}

	if ta.DynClient() == nil {
		t.Error("expected DynClient to be non-nil")
	}

	if ta.DiscClient() == nil {
		t.Error("expected DiscClient to be non-nil")
	}
}
