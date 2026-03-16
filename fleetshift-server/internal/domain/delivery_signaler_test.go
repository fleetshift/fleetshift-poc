package domain_test

import (
	"context"
	"sync"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// recordingDeliveryObserver captures delivery events and completions
// for test assertions. Thread-safe for use with async delivery agents.
type recordingDeliveryObserver struct {
	domain.NoOpDeliveryObserver
	mu      sync.Mutex
	events  []domain.DeliveryEvent
	results []domain.DeliveryResult
}

func (o *recordingDeliveryObserver) EventEmitted(ctx context.Context, _ domain.DeliveryID, _ domain.TargetInfo, event domain.DeliveryEvent) (context.Context, domain.EventEmittedProbe) {
	o.mu.Lock()
	o.events = append(o.events, event)
	o.mu.Unlock()
	return ctx, domain.NoOpEventEmittedProbe{}
}

func (o *recordingDeliveryObserver) Completed(ctx context.Context, _ domain.DeliveryID, _ domain.TargetInfo, result domain.DeliveryResult) (context.Context, domain.CompletedProbe) {
	o.mu.Lock()
	o.results = append(o.results, result)
	o.mu.Unlock()
	return ctx, domain.NoOpCompletedProbe{}
}

func (o *recordingDeliveryObserver) snapshot() ([]domain.DeliveryEvent, []domain.DeliveryResult) {
	o.mu.Lock()
	defer o.mu.Unlock()
	events := make([]domain.DeliveryEvent, len(o.events))
	copy(events, o.events)
	results := make([]domain.DeliveryResult, len(o.results))
	copy(results, o.results)
	return events, results
}

func TestDeliverySignaler_Emit_CallsObserver(t *testing.T) {
	obs := &recordingDeliveryObserver{}
	signaler := domain.NewDeliverySignaler(
		"d1", "del-1", domain.TargetInfo{ID: "t1", Type: "kind"},
		nil, nil, obs,
	)

	signaler.Emit(context.Background(), domain.DeliveryEvent{
		Kind:    domain.DeliveryEventProgress,
		Message: "creating cluster",
	})

	events, _ := obs.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Kind != domain.DeliveryEventProgress {
		t.Errorf("kind = %q, want %q", events[0].Kind, domain.DeliveryEventProgress)
	}
	if events[0].Message != "creating cluster" {
		t.Errorf("message = %q, want %q", events[0].Message, "creating cluster")
	}
}

func TestDeliverySignaler_Emit_MultipleEvents(t *testing.T) {
	obs := &recordingDeliveryObserver{}
	signaler := domain.NewDeliverySignaler(
		"d1", "del-1", domain.TargetInfo{ID: "t1", Type: "kind"},
		nil, nil, obs,
	)

	signaler.Emit(context.Background(), domain.DeliveryEvent{
		Kind:    domain.DeliveryEventProgress,
		Message: "step 1",
	})
	signaler.Emit(context.Background(), domain.DeliveryEvent{
		Kind:    domain.DeliveryEventProgress,
		Message: "step 2",
	})
	signaler.Emit(context.Background(), domain.DeliveryEvent{
		Kind:    domain.DeliveryEventWarning,
		Message: "slow network",
	})

	events, _ := obs.snapshot()
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[2].Kind != domain.DeliveryEventWarning {
		t.Errorf("events[2].Kind = %q, want %q", events[2].Kind, domain.DeliveryEventWarning)
	}
}

func TestDeliverySignaler_Done_CallsObserver(t *testing.T) {
	obs := &recordingDeliveryObserver{}
	signaler := domain.NewDeliverySignaler(
		"d1", "del-1", domain.TargetInfo{ID: "t1", Type: "kind"},
		nil, nil, obs,
	)

	signaler.Done(context.Background(), domain.DeliveryResult{
		State: domain.DeliveryStateDelivered,
	})

	_, results := obs.snapshot()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].State != domain.DeliveryStateDelivered {
		t.Errorf("state = %q, want %q", results[0].State, domain.DeliveryStateDelivered)
	}
}

func TestDeliverySignaler_NilObserver_DoesNotPanic(t *testing.T) {
	signaler := domain.NewDeliverySignaler(
		"d1", "del-1", domain.TargetInfo{ID: "t1"},
		nil, nil, nil,
	)

	signaler.Emit(context.Background(), domain.DeliveryEvent{
		Kind:    domain.DeliveryEventProgress,
		Message: "should not panic",
	})
	signaler.Done(context.Background(), domain.DeliveryResult{
		State: domain.DeliveryStateDelivered,
	})
}
