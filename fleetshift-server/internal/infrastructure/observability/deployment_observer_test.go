package observability_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/observability"
)

func TestDeploymentObserver_RunStarted_LogsAndReturnsProbe(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewDeploymentObserver(logger)
	ctx, probe := obs.RunStarted(context.Background(), "dep-1")
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}

	records := handler.Records()
	if len(records) != 1 {
		t.Fatalf("expected 1 log record from RunStarted, got %d", len(records))
	}
	if records[0].Message != "deployment run started" {
		t.Errorf("message = %q, want %q", records[0].Message, "deployment run started")
	}

	probe.End()
}

func TestDeploymentRunProbe_FullLifecycle(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewDeploymentObserver(logger)
	_, probe := obs.RunStarted(context.Background(), "dep-2")

	probe.EventReceived(domain.DeploymentEvent{
		PoolChange: &domain.PoolChange{},
	})
	probe.StateChanged(domain.DeploymentStateActive)
	probe.End()

	records := handler.Records()
	messages := make([]string, len(records))
	for i, r := range records {
		messages[i] = r.Message
	}

	want := []string{
		"deployment run started",
		"deployment event received",
		"deployment state changed",
		"deployment run completed",
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

func TestDeploymentRunProbe_ManifestsFiltered_AllDroppedLogsWarning(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewDeploymentObserver(logger)
	_, probe := obs.RunStarted(context.Background(), "dep-filter")

	probe.ManifestsFiltered(domain.TargetInfo{ID: "k8s-1", Type: "kubernetes"}, 2, 0)
	probe.End()

	records := handler.Records()
	var filterRecord *slog.Record
	for i := range records {
		if records[i].Message == "all manifests filtered for target" {
			filterRecord = &records[i]
			break
		}
	}
	if filterRecord == nil {
		t.Fatal("expected 'all manifests filtered for target' log record")
	}
	if filterRecord.Level != slog.LevelWarn {
		t.Errorf("level = %v, want %v", filterRecord.Level, slog.LevelWarn)
	}
}

func TestDeploymentRunProbe_ManifestsFiltered_PartialLogsDebug(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewDeploymentObserver(logger)
	_, probe := obs.RunStarted(context.Background(), "dep-partial")

	probe.ManifestsFiltered(domain.TargetInfo{ID: "kind-1", Type: "kind"}, 3, 2)
	probe.End()

	records := handler.Records()
	var filterRecord *slog.Record
	for i := range records {
		if records[i].Message == "manifests filtered for target" {
			filterRecord = &records[i]
			break
		}
	}
	if filterRecord == nil {
		t.Fatal("expected 'manifests filtered for target' log record")
	}
	if filterRecord.Level != slog.LevelDebug {
		t.Errorf("level = %v, want %v", filterRecord.Level, slog.LevelDebug)
	}
}

func TestDeploymentRunProbe_ErrorLogsAtErrorLevel(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewDeploymentObserver(logger)
	_, probe := obs.RunStarted(context.Background(), "dep-3")

	probe.Error(domain.ErrNotFound)
	probe.End()

	records := handler.Records()
	var endRecord *slog.Record
	for i := range records {
		if records[i].Message == "deployment run failed" {
			endRecord = &records[i]
			break
		}
	}
	if endRecord == nil {
		t.Fatal("expected 'deployment run failed' log record")
	}
	if endRecord.Level != slog.LevelError {
		t.Errorf("level = %v, want %v", endRecord.Level, slog.LevelError)
	}
}

func TestDeploymentRunProbe_DeliveryOutputsProcessed_LogsTargets(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewDeploymentObserver(logger)
	_, probe := obs.RunStarted(context.Background(), "dep-outputs")

	probe.DeliveryOutputsProcessed([]domain.ProvisionedTarget{
		{ID: "k8s-cluster1", Type: "kubernetes", Name: "cluster1"},
	}, 1)
	probe.End()

	records := handler.Records()
	var outputRecord *slog.Record
	for i := range records {
		if records[i].Message == "delivery outputs processed" {
			outputRecord = &records[i]
			break
		}
	}
	if outputRecord == nil {
		t.Fatal("expected 'delivery outputs processed' log record")
	}
	if outputRecord.Level != slog.LevelInfo {
		t.Errorf("level = %v, want %v", outputRecord.Level, slog.LevelInfo)
	}
}
