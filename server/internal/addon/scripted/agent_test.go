package scripted_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/scripted"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// --- test helpers ---

// channelReporter captures delivery events and results on buffered
// channels for deterministic test waits.
type channelReporter struct {
	mu     sync.Mutex
	events []domain.DeliveryEvent
	ch     chan domain.DeliveryEvent
	done   chan domain.DeliveryResult
}

func newChannelReporter() *channelReporter {
	return &channelReporter{
		ch:   make(chan domain.DeliveryEvent, 100),
		done: make(chan domain.DeliveryResult, 10),
	}
}

func (r *channelReporter) ReportEvent(_ context.Context, _ domain.DeliveryID, _ domain.Generation, event domain.DeliveryEvent) error {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
	r.ch <- event
	return nil
}

func (r *channelReporter) ReportResult(_ context.Context, _ domain.DeliveryID, _ domain.Generation, result domain.DeliveryResult) error {
	r.done <- result
	return nil
}

func (r *channelReporter) ListActiveDeliveries(_ context.Context, _ []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, nil
}

// recordingInventory captures inventory delta batches.
type recordingInventory struct {
	mu      sync.Mutex
	batches []domain.InventoryDeltaBatch
}

func (r *recordingInventory) ApplyDeltaBatch(_ context.Context, batch domain.InventoryDeltaBatch) error {
	r.mu.Lock()
	r.batches = append(r.batches, batch)
	r.mu.Unlock()
	return nil
}

func (r *recordingInventory) getBatches() []domain.InventoryDeltaBatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]domain.InventoryDeltaBatch, len(r.batches))
	copy(out, r.batches)
	return out
}

// nopInventory discards all inventory reports.
type nopInventory struct{}

func (nopInventory) ApplyDeltaBatch(context.Context, domain.InventoryDeltaBatch) error { return nil }

func scriptedTarget() domain.TargetInfo {
	return domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   scripted.TargetID,
		Type: scripted.TargetType,
		Name: "Local Scripted Provider",
	})
}

func wrapSpec(t *testing.T, spec json.RawMessage) (domain.ExtensionResourceUID, json.RawMessage) {
	t.Helper()
	uid := domain.ExtensionResourceUID(uuid.New())
	raw, err := domain.WrapManagedResourceSpec("test-resource", uid, spec)
	if err != nil {
		t.Fatalf("WrapManagedResourceSpec: %v", err)
	}
	return uid, raw
}

func newTestAgent(t *testing.T, reporter domain.DeliveryReporter, inventory domain.InventoryReporter) *scripted.Agent {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	codec, err := scripted.NewCodec(context.Background())
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}
	return scripted.NewAgent(reporter, inventory, codec, scripted.NewPlanner(), ctx)
}

// --- tests ---

func TestAgent_Deliver_ImmediateSuccess(t *testing.T) {
	reporter := newChannelReporter()
	inv := &recordingInventory{}
	agent := newTestAgent(t, reporter, inv)

	_, raw := wrapSpec(t, json.RawMessage(`{}`))
	manifests := []domain.Manifest{{
		ManifestType: scripted.ManagedManifestType,
		Raw:          raw,
	}}

	err := agent.Deliver(context.Background(), scriptedTarget(), "d1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	// Should get a progress event (ack accepted).
	select {
	case event := <-reporter.ch:
		if event.Kind != domain.DeliveryEventProgress {
			t.Errorf("event kind = %v, want progress", event.Kind)
		}
		if event.Message != "scripted deliver accepted" {
			t.Errorf("event message = %q", event.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ack event")
	}

	// Should get a delivered result (completion).
	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Errorf("result state = %v, want delivered", result.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	// Should have projected inventory.
	if err := agent.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	batches := inv.getBatches()
	if len(batches) == 0 {
		t.Error("expected at least one inventory batch")
	}
}

func TestAgent_Deliver_AckFailureThenSuccess(t *testing.T) {
	reporter := newChannelReporter()
	agent := newTestAgent(t, reporter, nopInventory{})

	// Spec: ack fails once, then succeeds.
	_, raw := wrapSpec(t, json.RawMessage(`{
		"behavior": {
			"delivery": {
				"acknowledgement": {
					"outcome": {"sequence": {"values": ["FAILURE", "SUCCESS"]}}
				}
			}
		}
	}`))
	manifests := []domain.Manifest{{
		ManifestType: scripted.ManagedManifestType,
		Raw:          raw,
	}}

	// First attempt: ack should fail -- Deliver returns an error.
	err := agent.Deliver(context.Background(), scriptedTarget(), "d1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err == nil {
		t.Fatal("expected ack failure error, got nil")
	}

	// No event or result should have been reported.
	select {
	case event := <-reporter.ch:
		t.Fatalf("unexpected event after ack failure: %v", event)
	default:
	}

	// Second attempt with a new delivery ID (platform retry): ack should succeed.
	err = agent.Deliver(context.Background(), scriptedTarget(), "d2", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("second Deliver: %v", err)
	}

	// Should get the ack event now.
	select {
	case event := <-reporter.ch:
		if event.Kind != domain.DeliveryEventProgress {
			t.Errorf("event kind = %v, want progress", event.Kind)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ack event")
	}

	// Should get delivered result.
	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Errorf("result state = %v, want delivered", result.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	if err := agent.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestAgent_Deliver_CompletionFailureThenSuccess(t *testing.T) {
	reporter := newChannelReporter()
	agent := newTestAgent(t, reporter, nopInventory{})

	_, raw := wrapSpec(t, json.RawMessage(`{
		"behavior": {
			"delivery": {
				"completion": {
					"outcome": {"sequence": {"values": ["FAILURE", "SUCCESS"]}}
				}
			}
		}
	}`))
	manifests := []domain.Manifest{{
		ManifestType: scripted.ManagedManifestType,
		Raw:          raw,
	}}

	// First attempt: ack succeeds, completion fails.
	err := agent.Deliver(context.Background(), scriptedTarget(), "d1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	// Wait for ack event.
	select {
	case <-reporter.ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ack event")
	}

	// Wait for failed result.
	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateFailed {
			t.Errorf("first result state = %v, want failed", result.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first result")
	}

	// Second attempt (platform retry): ack succeeds, completion succeeds.
	err = agent.Deliver(context.Background(), scriptedTarget(), "d2", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("second Deliver: %v", err)
	}

	select {
	case <-reporter.ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for second ack event")
	}

	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Errorf("second result state = %v, want delivered", result.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for second result")
	}

	if err := agent.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestAgent_Deliver_WithLatency(t *testing.T) {
	reporter := newChannelReporter()
	agent := newTestAgent(t, reporter, nopInventory{})

	_, raw := wrapSpec(t, json.RawMessage(`{
		"behavior": {
			"delivery": {
				"acknowledgement": {
					"latency": {"constant": "0.05s"},
					"outcome": {"constant": "SUCCESS"}
				},
				"completion": {
					"latency": {"constant": "0.05s"},
					"outcome": {"constant": "SUCCESS"}
				}
			}
		}
	}`))
	manifests := []domain.Manifest{{
		ManifestType: scripted.ManagedManifestType,
		Raw:          raw,
	}}

	start := time.Now()
	err := agent.Deliver(context.Background(), scriptedTarget(), "d1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	ackElapsed := time.Since(start)

	// Ack latency should be at least 50ms.
	if ackElapsed < 40*time.Millisecond {
		t.Errorf("ack returned too quickly: %v", ackElapsed)
	}

	// Wait for completion result.
	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Errorf("result state = %v, want delivered", result.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	if err := agent.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestAgent_Remove_ImmediateSuccess(t *testing.T) {
	reporter := newChannelReporter()
	agent := newTestAgent(t, reporter, nopInventory{})

	_, raw := wrapSpec(t, json.RawMessage(`{}`))
	manifests := []domain.Manifest{{
		ManifestType: scripted.ManagedManifestType,
		Raw:          raw,
	}}

	err := agent.Remove(context.Background(), scriptedTarget(), "d1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}

	// Ack event.
	select {
	case event := <-reporter.ch:
		if event.Message != "scripted remove accepted" {
			t.Errorf("event message = %q, want 'scripted remove accepted'", event.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ack event")
	}

	// Delivered result.
	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Errorf("result state = %v, want delivered", result.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	if err := agent.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestAgent_Deliver_InvalidSpec(t *testing.T) {
	reporter := newChannelReporter()
	agent := newTestAgent(t, reporter, nopInventory{})

	_, raw := wrapSpec(t, json.RawMessage(`{
		"behavior": {
			"delivery": {
				"acknowledgement": {
					"latency": {"constant": "301s"}
				}
			}
		}
	}`))
	manifests := []domain.Manifest{{
		ManifestType: scripted.ManagedManifestType,
		Raw:          raw,
	}}

	err := agent.Deliver(context.Background(), scriptedTarget(), "d1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err == nil {
		t.Fatal("expected validation error for 301s latency, got nil")
	}
}

func TestAgent_Deliver_WrongTarget(t *testing.T) {
	reporter := newChannelReporter()
	agent := newTestAgent(t, reporter, nopInventory{})

	_, raw := wrapSpec(t, json.RawMessage(`{}`))
	manifests := []domain.Manifest{{
		ManifestType: scripted.ManagedManifestType,
		Raw:          raw,
	}}

	wrongTarget := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "wrong-target",
		Type: "wrong",
		Name: "Wrong",
	})

	err := agent.Deliver(context.Background(), wrongTarget, "d1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err == nil {
		t.Fatal("expected error for wrong target, got nil")
	}
}

func TestAgent_Deliver_WrongManifestType(t *testing.T) {
	reporter := newChannelReporter()
	agent := newTestAgent(t, reporter, nopInventory{})

	_, raw := wrapSpec(t, json.RawMessage(`{}`))
	manifests := []domain.Manifest{{
		ManifestType: "wrong.manifest.type",
		Raw:          raw,
	}}

	err := agent.Deliver(context.Background(), scriptedTarget(), "d1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err == nil {
		t.Fatal("expected error for wrong manifest type, got nil")
	}
}

func TestAgent_Close_CancelsInFlightWork(t *testing.T) {
	reporter := newChannelReporter()

	// Create agent with an explicitly controlled appCtx so we can
	// cancel it before Close, matching the real shutdown sequence
	// where bootstrap calls appCancel() before addon close hooks.
	appCtx, appCancel := context.WithCancel(context.Background())
	codec, err := scripted.NewCodec(context.Background())
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}
	agent := scripted.NewAgent(reporter, nopInventory{}, codec, scripted.NewPlanner(), appCtx)

	// Use a long completion latency so work is still in-flight when we close.
	_, raw := wrapSpec(t, json.RawMessage(`{
		"behavior": {
			"delivery": {
				"completion": {
					"latency": {"constant": "60s"},
					"outcome": {"constant": "SUCCESS"}
				}
			}
		}
	}`))
	manifests := []domain.Manifest{{
		ManifestType: scripted.ManagedManifestType,
		Raw:          raw,
	}}

	err = agent.Deliver(context.Background(), scriptedTarget(), "d1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	// Wait for ack event.
	select {
	case <-reporter.ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ack event")
	}

	// Cancel appCtx first (like bootstrap shutdown does), then Close
	// should join the in-flight work promptly.
	appCancel()

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- agent.Close(context.Background())
	}()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return promptly after cancellation")
	}
}

func TestAgent_Deliver_InventoryProjection(t *testing.T) {
	reporter := newChannelReporter()
	inv := &recordingInventory{}
	agent := newTestAgent(t, reporter, inv)

	_, raw := wrapSpec(t, json.RawMessage(`{
		"inventory": {
			"labels": {"env": "test", "tier": "backend"},
			"observation": {"nodes": 3}
		}
	}`))
	manifests := []domain.Manifest{{
		ManifestType: scripted.ManagedManifestType,
		Raw:          raw,
	}}

	err := agent.Deliver(context.Background(), scriptedTarget(), "d1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	// Wait for ack + completion.
	select {
	case <-reporter.ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ack")
	}
	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Fatalf("result state = %v, want delivered", result.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	if err := agent.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	batches := inv.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 inventory batch, got %d", len(batches))
	}
	if len(batches[0].Reports) != 1 {
		t.Fatalf("expected 1 report in batch, got %d", len(batches[0].Reports))
	}
	report := batches[0].Reports[0]
	if report.ResourceType != scripted.ResourceType {
		t.Errorf("resource type = %q, want %q", report.ResourceType, scripted.ResourceType)
	}
	if len(report.ReplaceLabels) != 2 {
		t.Errorf("labels count = %d, want 2", len(report.ReplaceLabels))
	}
	if report.ReplaceLabels["env"] != "test" {
		t.Errorf("labels[env] = %q, want test", report.ReplaceLabels["env"])
	}
	if report.Observation == nil {
		t.Error("observation is nil, want non-nil")
	}
}
