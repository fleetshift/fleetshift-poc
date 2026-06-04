package observability_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/observability"
)

func TestDeliveryObserver_ReportEventStarted_LogsEvent(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewDeliveryObserver(logger)

	ctx, probe := obs.ReportEventStarted(context.Background(), "del-1", 0)
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	probe.Event(domain.DeliveryEvent{
		Kind:    domain.DeliveryEventProgress,
		Message: "creating cluster",
	})
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

func TestDeliveryObserver_ReportEventStarted_WarningLevel(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewDeliveryObserver(logger)

	_, probe := obs.ReportEventStarted(context.Background(), "del-2", 0)
	probe.Event(domain.DeliveryEvent{
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

func TestDeliveryObserver_ReportEventStarted_ErrorLogsAtErrorLevel(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewDeliveryObserver(logger)

	_, probe := obs.ReportEventStarted(context.Background(), "del-5", 0)
	probe.Event(domain.DeliveryEvent{
		Kind:    domain.DeliveryEventProgress,
		Message: "applying",
	})
	probe.Error(domain.ErrNotFound)
	probe.End()

	records := handler.Records()
	var failRecord *slog.Record
	for i := range records {
		if records[i].Message == "delivery event failed" {
			failRecord = &records[i]
			break
		}
	}
	if failRecord == nil {
		t.Fatal("expected 'delivery event failed' log record")
	}
	if failRecord.Level != slog.LevelError {
		t.Errorf("level = %v, want %v", failRecord.Level, slog.LevelError)
	}
}

func TestDeliveryObserver_ReportEventStarted_Stale(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewDeliveryObserver(logger)

	_, probe := obs.ReportEventStarted(context.Background(), "del-6", 3)
	probe.Event(domain.DeliveryEvent{
		Kind:    domain.DeliveryEventProgress,
		Message: "stale event",
	})
	probe.Stale(3, 5)
	probe.End()

	records := handler.Records()
	if len(records) != 1 {
		t.Fatalf("expected 1 log record (stale), got %d", len(records))
	}
	if records[0].Message != "delivery event stale" {
		t.Errorf("message = %q, want %q", records[0].Message, "delivery event stale")
	}
	if records[0].Level != slog.LevelDebug {
		t.Errorf("level = %v, want %v", records[0].Level, slog.LevelDebug)
	}
}

func TestDeliveryObserver_ReportResultStarted_LogsResult(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewDeliveryObserver(logger)

	ctx, probe := obs.ReportResultStarted(context.Background(), "del-3", 0)
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	probe.Result(domain.DeliveryResult{
		State: domain.DeliveryStateDelivered,
	})
	probe.End()

	records := handler.Records()
	if len(records) != 1 {
		t.Fatalf("expected 1 log record, got %d", len(records))
	}
	if records[0].Message != "delivery result done" {
		t.Errorf("message = %q, want %q", records[0].Message, "delivery result done")
	}
	if records[0].Level != slog.LevelInfo {
		t.Errorf("level = %v, want %v", records[0].Level, slog.LevelInfo)
	}
}

func TestDeliveryObserver_ReportResultProbe_ErrorLogsAtErrorLevel(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewDeliveryObserver(logger)

	_, probe := obs.ReportResultStarted(context.Background(), "del-4", 0)
	probe.Result(domain.DeliveryResult{
		State: domain.DeliveryStateFailed,
	})
	probe.Error(domain.ErrNotFound)
	probe.End()

	records := handler.Records()
	var failRecord *slog.Record
	for i := range records {
		if records[i].Message == "delivery result failed" {
			failRecord = &records[i]
			break
		}
	}
	if failRecord == nil {
		t.Fatal("expected 'delivery result failed' log record")
	}
	if failRecord.Level != slog.LevelError {
		t.Errorf("level = %v, want %v", failRecord.Level, slog.LevelError)
	}
}

func TestDeliveryObserver_ReportResultStarted_Stale(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewDeliveryObserver(logger)

	_, probe := obs.ReportResultStarted(context.Background(), "del-7", 3)
	probe.Result(domain.DeliveryResult{
		State: domain.DeliveryStateDelivered,
	})
	probe.Stale(3, 5)
	probe.End()

	records := handler.Records()
	if len(records) != 1 {
		t.Fatalf("expected 1 log record (stale), got %d", len(records))
	}
	if records[0].Message != "delivery result stale" {
		t.Errorf("message = %q, want %q", records[0].Message, "delivery result stale")
	}
	if records[0].Level != slog.LevelDebug {
		t.Errorf("level = %v, want %v", records[0].Level, slog.LevelDebug)
	}
}
