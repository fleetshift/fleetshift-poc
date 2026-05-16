package gcphcp

import (
	"context"
	"testing"
	"time"
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
