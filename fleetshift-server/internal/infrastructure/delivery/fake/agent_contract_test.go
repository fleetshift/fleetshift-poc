package fake_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery/fake"
)

type recordingReporter struct {
	mu      sync.Mutex
	events  []domain.DeliveryEvent
	results []domain.DeliveryResult
}

func (r *recordingReporter) ReportEvent(_ context.Context, _ domain.DeliveryID, _ domain.Generation, event domain.DeliveryEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
	return nil
}

func (r *recordingReporter) ReportResult(_ context.Context, _ domain.DeliveryID, _ domain.Generation, result domain.DeliveryResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = append(r.results, result)
	return nil
}

func (r *recordingReporter) ListActiveDeliveries(context.Context, []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, nil
}

func (r *recordingReporter) waitResults(t *testing.T, n int) {
	t.Helper()
	waitUntil(t, 2*time.Second, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.results) >= n
	}, "timed out waiting for results")
}

func waitCalls(t *testing.T, ctrl *fake.Controller, n int) {
	t.Helper()
	waitUntil(t, 2*time.Second, func() bool {
		return len(ctrl.Calls()) >= n
	}, "timed out waiting for agent calls")
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(msg)
}

func testTarget() domain.TargetInfo {
	return domain.NewTargetInfo(
		"t1",
		"fake",
		"fake-1",
		domain.TargetStateReady,
		nil, nil,
		[]domain.ManifestType{"hermetic.resource"},
	)
}

func TestContract_DefaultSuccess(t *testing.T) {
	rep := &recordingReporter{}
	agent, ctrl := fake.New(rep)
	ctx := context.Background()

	if err := agent.Deliver(ctx, testTarget(), "f1:t1", nil, domain.DeliveryAuth{}, nil, 1); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	rep.waitResults(t, 1)

	calls := ctrl.Calls()
	if len(calls) != 1 || calls[0].Kind != fake.CallDeliver {
		t.Fatalf("Calls = %+v, want one deliver", calls)
	}
	reports := ctrl.Reports()
	if len(reports) != 1 || reports[0].Result == nil || reports[0].Result.State != domain.DeliveryStateDelivered {
		t.Fatalf("Reports = %+v, want one delivered", reports)
	}
}

func TestContract_GateReleaseAndProgress(t *testing.T) {
	rep := &recordingReporter{}
	agent, ctrl := fake.New(rep)
	ctx := context.Background()

	if err := ctrl.Gate(); err != nil {
		t.Fatalf("Gate: %v", err)
	}
	if err := ctrl.ReportProgress("waiting for release"); err != nil {
		t.Fatalf("ReportProgress before deliver: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- agent.Deliver(ctx, testTarget(), "f1:t1", nil, domain.DeliveryAuth{}, nil, 1)
	}()

	waitCalls(t, ctrl, 1)

	if err := ctrl.ReportProgress("still gated"); err != nil {
		t.Fatalf("ReportProgress while gated: %v", err)
	}

	select {
	case <-done:
		t.Fatal("Deliver returned before Release")
	case <-time.After(50 * time.Millisecond):
	}

	if err := ctrl.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Deliver after release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Deliver did not unblock")
	}
	rep.waitResults(t, 1)

	var progress int
	for _, r := range ctrl.Reports() {
		if r.Kind == fake.ReportEvent && r.Event != nil && r.Event.Kind == domain.DeliveryEventProgress {
			progress++
		}
	}
	if progress < 2 {
		t.Fatalf("progress events = %d, want >= 2; reports=%+v", progress, ctrl.Reports())
	}
}

func TestContract_TransientFailureThenSuccess(t *testing.T) {
	rep := &recordingReporter{}
	agent, ctrl := fake.New(rep)
	ctx := context.Background()

	if err := ctrl.InjectTransientFailure(1); err != nil {
		t.Fatalf("InjectTransientFailure: %v", err)
	}
	if err := agent.Deliver(ctx, testTarget(), "f1:t1", nil, domain.DeliveryAuth{}, nil, 1); err == nil {
		t.Fatal("expected transient failure")
	}
	if err := agent.Deliver(ctx, testTarget(), "f1:t1", nil, domain.DeliveryAuth{}, nil, 1); err != nil {
		t.Fatalf("second Deliver: %v", err)
	}
	rep.waitResults(t, 1)

	calls := ctrl.Calls()
	if len(calls) != 2 {
		t.Fatalf("Calls = %d, want 2", len(calls))
	}
}

func TestContract_QueueSuccessResetsMode(t *testing.T) {
	rep := &recordingReporter{}
	agent, ctrl := fake.New(rep)
	ctx := context.Background()

	if err := ctrl.InjectTransientFailure(3); err != nil {
		t.Fatalf("InjectTransientFailure: %v", err)
	}
	ctrl.QueueSuccess()
	if err := agent.Deliver(ctx, testTarget(), "f1:t1", nil, domain.DeliveryAuth{}, nil, 1); err != nil {
		t.Fatalf("Deliver after QueueSuccess: %v", err)
	}
	rep.waitResults(t, 1)

	if err := ctrl.Gate(); err != nil {
		t.Fatalf("Gate: %v", err)
	}
	ctrl.QueueSuccess()
	if err := agent.Deliver(ctx, testTarget(), "f1:t1", nil, domain.DeliveryAuth{}, nil, 2); err != nil {
		t.Fatalf("Deliver after QueueSuccess cleared gate: %v", err)
	}
	rep.waitResults(t, 2)
}

func TestContract_ControllerInvalidOps(t *testing.T) {
	_, ctrl := fake.New(&recordingReporter{})

	t.Run("gateWhileGated", func(t *testing.T) {
		if err := ctrl.Gate(); err != nil {
			t.Fatalf("Gate: %v", err)
		}
		err := ctrl.Gate()
		if err == nil || !strings.Contains(err.Error(), "already gated") {
			t.Fatalf("Gate while gated = %v, want already gated", err)
		}
		ctrl.QueueSuccess()
	})

	t.Run("reportProgressNotGated", func(t *testing.T) {
		ctrl.QueueSuccess()
		err := ctrl.ReportProgress("nope")
		if err == nil || !strings.Contains(err.Error(), "not gated") {
			t.Fatalf("ReportProgress = %v, want not gated", err)
		}
	})

	t.Run("releaseNotGated", func(t *testing.T) {
		ctrl.QueueSuccess()
		err := ctrl.Release()
		if err == nil || !strings.Contains(err.Error(), "not gated") {
			t.Fatalf("Release = %v, want not gated", err)
		}
	})

	t.Run("transientCountInvalid", func(t *testing.T) {
		err := ctrl.InjectTransientFailure(0)
		if err == nil || !strings.Contains(err.Error(), "must be >= 1") {
			t.Fatalf("InjectTransientFailure(0) = %v, want count validation error", err)
		}
	})
}

func TestContract_GateCancelledByContext(t *testing.T) {
	rep := &recordingReporter{}
	agent, ctrl := fake.New(rep)

	if err := ctrl.Gate(); err != nil {
		t.Fatalf("Gate: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- agent.Deliver(ctx, testTarget(), "f1:t1", nil, domain.DeliveryAuth{}, nil, 1)
	}()

	waitCalls(t, ctrl, 1)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Deliver = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Deliver did not unblock on cancel")
	}

	// Release must not be required after cancel; a fresh success path works.
	ctrl.QueueSuccess()
	if err := agent.Deliver(context.Background(), testTarget(), "f1:t1", nil, domain.DeliveryAuth{}, nil, 2); err != nil {
		t.Fatalf("Deliver after cancel: %v", err)
	}
	rep.waitResults(t, 1)
}

func TestContract_OrderedInspection(t *testing.T) {
	rep := &recordingReporter{}
	agent, ctrl := fake.New(rep)
	ctx := context.Background()

	_ = agent.Deliver(ctx, testTarget(), "f1:t1", nil, domain.DeliveryAuth{}, nil, 1)
	_ = agent.Remove(ctx, testTarget(), "f1:t1", nil, domain.DeliveryAuth{}, nil, 2)
	rep.waitResults(t, 2)

	calls := ctrl.Calls()
	if len(calls) != 2 {
		t.Fatalf("Calls = %d, want 2", len(calls))
	}
	if calls[0].Kind != fake.CallDeliver || calls[1].Kind != fake.CallRemove {
		t.Fatalf("order = %v,%v", calls[0].Kind, calls[1].Kind)
	}
	if !calls[0].At.Before(calls[1].At) && !calls[0].At.Equal(calls[1].At) {
		t.Fatalf("timestamps out of order: %v then %v", calls[0].At, calls[1].At)
	}
}
