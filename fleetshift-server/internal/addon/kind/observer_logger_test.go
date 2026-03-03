package kind_test

import (
	"context"
	"testing"
	"time"

	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func fixedNow() time.Time {
	return time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)
}

func TestObserverLogger_V0_EmitsProgress(t *testing.T) {
	var events []domain.DeliveryEvent
	signaler := recordingSignaler(&events)
	logger := kindaddon.NewObserverLogger(context.Background(), signaler, fixedNow)

	logger.V(0).Info("Ensuring node image")
	logger.V(0).Infof("Preparing nodes %d", 3)

	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	for _, e := range events {
		if e.Kind != domain.DeliveryEventProgress {
			t.Errorf("event kind = %q, want %q", e.Kind, domain.DeliveryEventProgress)
		}
		if e.Timestamp != fixedNow() {
			t.Errorf("timestamp = %v, want %v", e.Timestamp, fixedNow())
		}
	}
	if events[0].Message != "Ensuring node image" {
		t.Errorf("message = %q, want %q", events[0].Message, "Ensuring node image")
	}
	if events[1].Message != "Preparing nodes 3" {
		t.Errorf("message = %q, want %q", events[1].Message, "Preparing nodes 3")
	}
}

func TestObserverLogger_V1Plus_Discarded(t *testing.T) {
	var events []domain.DeliveryEvent
	signaler := recordingSignaler(&events)
	logger := kindaddon.NewObserverLogger(context.Background(), signaler, fixedNow)

	logger.V(1).Info("debug message")
	logger.V(2).Infof("trace %s", "detail")

	if len(events) != 0 {
		t.Fatalf("expected 0 events for V(1+), got %d", len(events))
	}
}

func TestObserverLogger_WarnAndError(t *testing.T) {
	var events []domain.DeliveryEvent
	signaler := recordingSignaler(&events)
	logger := kindaddon.NewObserverLogger(context.Background(), signaler, fixedNow)

	logger.Warn("something wrong")
	logger.Warnf("bad %s", "thing")
	logger.Error("failure")
	logger.Errorf("failed: %v", "boom")

	if len(events) != 4 {
		t.Fatalf("expected 4 events, got %d", len(events))
	}
	if events[0].Kind != domain.DeliveryEventWarning {
		t.Errorf("event[0] kind = %q, want warning", events[0].Kind)
	}
	if events[1].Kind != domain.DeliveryEventWarning {
		t.Errorf("event[1] kind = %q, want warning", events[1].Kind)
	}
	if events[2].Kind != domain.DeliveryEventError {
		t.Errorf("event[2] kind = %q, want error", events[2].Kind)
	}
	if events[3].Kind != domain.DeliveryEventError {
		t.Errorf("event[3] kind = %q, want error", events[3].Kind)
	}
}

func TestObserverLogger_V0_Enabled(t *testing.T) {
	signaler := recordingSignaler(&[]domain.DeliveryEvent{})
	logger := kindaddon.NewObserverLogger(context.Background(), signaler, fixedNow)

	if !logger.V(0).Enabled() {
		t.Error("V(0) should be enabled")
	}
	if logger.V(1).Enabled() {
		t.Error("V(1) should not be enabled")
	}
}
