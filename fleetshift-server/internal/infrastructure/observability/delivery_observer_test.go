package observability_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/observability"
)

func TestDeliveryObserver_EventEmitted_LogsEvent(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewDeliveryObserver(logger)
	target := domain.TargetInfo{ID: "t1", Type: "kind", Name: "cluster-1"}

	ctx, probe := obs.EventEmitted(context.Background(), "del-1", target, domain.DeliveryEvent{
		Kind:    domain.DeliveryEventProgress,
		Message: "creating cluster",
	})
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	probe.End()

	records := handler.Records()
	if len(records) != 1 {
		t.Fatalf("expected 1 log record, got %d", len(records))
	}
	if records[0].Message != "delivery event" {
		t.Errorf("message = %q, want %q", records[0].Message, "delivery event")
	}
	if records[0].Level != slog.LevelInfo {
		t.Errorf("level = %v, want %v", records[0].Level, slog.LevelInfo)
	}
}

func TestDeliveryObserver_EventEmitted_WarningLevel(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewDeliveryObserver(logger)
	target := domain.TargetInfo{ID: "t1", Type: "kind"}

	_, probe := obs.EventEmitted(context.Background(), "del-2", target, domain.DeliveryEvent{
		Kind:    domain.DeliveryEventWarning,
		Message: "slow network",
	})
	probe.End()

	records := handler.Records()
	if len(records) != 1 {
		t.Fatalf("expected 1 log record, got %d", len(records))
	}
	if records[0].Level != slog.LevelWarn {
		t.Errorf("level = %v, want %v", records[0].Level, slog.LevelWarn)
	}
}

func TestDeliveryObserver_Completed_LogsResult(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewDeliveryObserver(logger)
	target := domain.TargetInfo{ID: "t1", Type: "kind", Name: "cluster-1"}

	ctx, probe := obs.Completed(context.Background(), "del-3", target, domain.DeliveryResult{
		State: domain.DeliveryStateDelivered,
	})
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	probe.End()

	records := handler.Records()
	messages := make([]string, len(records))
	for i, r := range records {
		messages[i] = r.Message
	}

	want := []string{
		"delivery completed",
		"delivery completion done",
	}
	if len(messages) != len(want) {
		t.Fatalf("got %d records %v, want %d %v", len(messages), messages, len(want), want)
	}
	for i, w := range want {
		if messages[i] != w {
			t.Errorf("record[%d] message = %q, want %q", i, messages[i], w)
		}
	}
}

func TestDeliveryObserver_CompletedProbe_ErrorLogsAtErrorLevel(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewDeliveryObserver(logger)
	target := domain.TargetInfo{ID: "t1", Type: "kind"}

	_, probe := obs.Completed(context.Background(), "del-4", target, domain.DeliveryResult{
		State: domain.DeliveryStateFailed,
	})
	probe.Error(domain.ErrNotFound)
	probe.End()

	records := handler.Records()
	var failRecord *slog.Record
	for i := range records {
		if records[i].Message == "delivery completion failed" {
			failRecord = &records[i]
			break
		}
	}
	if failRecord == nil {
		t.Fatal("expected 'delivery completion failed' log record")
	}
	if failRecord.Level != slog.LevelError {
		t.Errorf("level = %v, want %v", failRecord.Level, slog.LevelError)
	}
}
