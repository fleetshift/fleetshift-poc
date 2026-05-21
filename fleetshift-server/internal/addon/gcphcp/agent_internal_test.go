package gcphcp

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type reconcileContextKey string

func TestNewReconcileContext_AddsDeadlineAndPreservesValues(t *testing.T) {
	origTimeout := reconcileTimeout
	reconcileTimeout = 25 * time.Millisecond
	defer func() {
		reconcileTimeout = origTimeout
	}()

	requestCtx, cancel := context.WithCancel(context.Background())
	requestCtx = context.WithValue(requestCtx, reconcileContextKey("trace-id"), "trace-123")
	cancel()

	runCtx, runCancel := newReconcileContext(context.WithoutCancel(requestCtx))
	defer runCancel()

	if got := runCtx.Value(reconcileContextKey("trace-id")); got != "trace-123" {
		t.Fatalf("context value = %v, want trace-123", got)
	}

	deadline, ok := runCtx.Deadline()
	if !ok {
		t.Fatal("expected reconcile context to have a deadline")
	}

	if remaining := time.Until(deadline); remaining <= 0 || remaining > reconcileTimeout+100*time.Millisecond {
		t.Fatalf("deadline remaining = %v, want within (0, %v]", remaining, reconcileTimeout+100*time.Millisecond)
	}

	select {
	case <-runCtx.Done():
		t.Fatal("reconcile context should not be done immediately")
	default:
	}
}

func TestAcceptGeneration_FirstDeliveryAccepted(t *testing.T) {
	agent := &Agent{clusterGen: make(map[string]domain.Generation)}

	if !agent.acceptGeneration("cluster-a", 5) {
		t.Fatal("first delivery should be accepted")
	}
}

func TestAcceptGeneration_AcceptsNewerGeneration(t *testing.T) {
	agent := &Agent{clusterGen: make(map[string]domain.Generation)}

	agent.acceptGeneration("cluster-a", 5)
	if !agent.acceptGeneration("cluster-a", 10) {
		t.Fatal("newer generation should be accepted")
	}
}

func TestAcceptGeneration_RejectsStaleGeneration(t *testing.T) {
	agent := &Agent{clusterGen: make(map[string]domain.Generation)}

	agent.acceptGeneration("cluster-a", 10)
	if agent.acceptGeneration("cluster-a", 5) {
		t.Fatal("stale generation should be rejected")
	}
}

func TestAcceptGeneration_AcceptsSameGenerationRetry(t *testing.T) {
	agent := &Agent{clusterGen: make(map[string]domain.Generation)}

	agent.acceptGeneration("cluster-a", 10)
	if !agent.acceptGeneration("cluster-a", 10) {
		t.Fatal("same-generation retry should be accepted")
	}
}

func TestAcceptGeneration_IndependentPerCluster(t *testing.T) {
	agent := &Agent{clusterGen: make(map[string]domain.Generation)}

	agent.acceptGeneration("cluster-a", 10)
	if !agent.acceptGeneration("cluster-b", 5) {
		t.Fatal("different cluster should track independently")
	}
}

func TestDeliveryResultForReconcileError_AuthExpiredReturnsAuthFailed(t *testing.T) {
	err := newAuthExpiredError(fmt.Errorf("CLS API GET /api/v1/clusters failed (HTTP 401): token expired"))
	result := deliveryResultForReconcileError(err)

	if result.State != domain.DeliveryStateAuthFailed {
		t.Fatalf("state = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
	if !strings.Contains(result.Message, "credentials expired") {
		t.Fatalf("message = %q, want 'credentials expired' context", result.Message)
	}
	if !strings.Contains(result.Message, "401") {
		t.Fatalf("message = %q, want wrapped cause mentioning 401", result.Message)
	}
}

func TestDeliveryResultForReconcileError_AuthExpiredTakesPrecedenceOverPostProvision(t *testing.T) {
	inner := newPostProvisionRegistrationError(fmt.Errorf("bootstrap failed"))
	err := newAuthExpiredError(inner)
	result := deliveryResultForReconcileError(err)

	if result.State != domain.DeliveryStateAuthFailed {
		t.Fatalf("state = %q, want %q — auth expired should take precedence", result.State, domain.DeliveryStateAuthFailed)
	}
}

func TestDeliveryResultForReconcileError_WrappedAuthExpiredReturnsAuthFailed(t *testing.T) {
	authErr := newAuthExpiredError(fmt.Errorf("IAM returned status 401"))
	wrapped := fmt.Errorf("broker auth exchange: %w", authErr)
	result := deliveryResultForReconcileError(wrapped)

	if result.State != domain.DeliveryStateAuthFailed {
		t.Fatalf("state = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
}

func TestDeliveryResultForReconcileError_PostProvisionRegistrationErrorUsesExplicitMessage(t *testing.T) {
	result := deliveryResultForReconcileError(
		newPostProvisionRegistrationError(fmt.Errorf("bootstrap guest cluster after 3 attempts: RBAC not ready")),
	)

	if result.State != domain.DeliveryStateFailed {
		t.Fatalf("state = %q, want %q", result.State, domain.DeliveryStateFailed)
	}
	if !strings.Contains(result.Message, "cluster provisioned and management-plane ready") {
		t.Fatalf("message = %q, want management-plane ready context", result.Message)
	}
	if !strings.Contains(result.Message, "guest target registration did not complete") {
		t.Fatalf("message = %q, want guest registration context", result.Message)
	}
	if !strings.Contains(result.Message, "RBAC not ready") {
		t.Fatalf("message = %q, want wrapped bootstrap cause", result.Message)
	}
}
