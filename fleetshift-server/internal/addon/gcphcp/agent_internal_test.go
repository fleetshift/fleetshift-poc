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
