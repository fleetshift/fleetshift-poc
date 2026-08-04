package observability_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/observability"
)

func TestDeleteObserver_DeleteDeploymentStarted_LogsAndReturnsProbe(t *testing.T) {
	handler := newRecordingHandler(&slog.HandlerOptions{Level: slog.LevelDebug})
	obs := observability.NewDeleteObserver(slog.New(handler))

	ctx, probe := obs.DeleteDeploymentStarted(context.Background(), "deployments/dep-1")
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	probe.Mutated("ful-1", 1)
	probe.CleanupStarted()
	probe.End()

	records := handler.Records()
	messages := collectMessages(records)

	want := []string{
		"delete deployment completed",
	}
	for _, w := range want {
		if !containsMessage(messages, w) {
			t.Errorf("expected log message %q, got messages: %v", w, messages)
		}
	}
}

func TestDeleteObserver_DeleteDeploymentProbe_ErrorLogsAtErrorLevel(t *testing.T) {
	handler := newRecordingHandler(&slog.HandlerOptions{Level: slog.LevelDebug})
	obs := observability.NewDeleteObserver(slog.New(handler))

	_, probe := obs.DeleteDeploymentStarted(context.Background(), "deployments/dep-err")
	probe.Error(errors.New("mutation failed"))
	probe.End()

	records := handler.Records()
	var failRecord *slog.Record
	for i := range records {
		if records[i].Message == "delete deployment failed" {
			failRecord = &records[i]
			break
		}
	}
	if failRecord == nil {
		t.Fatal("expected 'delete deployment failed' log record")
	}
	if failRecord.Level != slog.LevelError {
		t.Errorf("level = %v, want %v", failRecord.Level, slog.LevelError)
	}
}

func TestDeleteObserver_DeleteManagedResourceStarted_LogsAndReturnsProbe(t *testing.T) {
	handler := newRecordingHandler(&slog.HandlerOptions{Level: slog.LevelDebug})
	obs := observability.NewDeleteObserver(slog.New(handler))

	ctx, probe := obs.DeleteManagedResourceStarted(context.Background(), "clusters", "my-cluster")
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	probe.Mutated("ful-2", 3)
	probe.CleanupStarted()
	probe.End()

	records := handler.Records()
	messages := collectMessages(records)

	if !containsMessage(messages, "delete managed resource completed") {
		t.Errorf("expected 'delete managed resource completed', got messages: %v", messages)
	}
}

func TestDeleteObserver_DeleteManagedResourceProbe_ErrorLogsAtErrorLevel(t *testing.T) {
	handler := newRecordingHandler(&slog.HandlerOptions{Level: slog.LevelDebug})
	obs := observability.NewDeleteObserver(slog.New(handler))

	_, probe := obs.DeleteManagedResourceStarted(context.Background(), "clusters", "my-cluster")
	probe.Error(errors.New("not found"))
	probe.End()

	records := handler.Records()
	var failRecord *slog.Record
	for i := range records {
		if records[i].Message == "delete managed resource failed" {
			failRecord = &records[i]
			break
		}
	}
	if failRecord == nil {
		t.Fatal("expected 'delete managed resource failed' log record")
	}
	if failRecord.Level != slog.LevelError {
		t.Errorf("level = %v, want %v", failRecord.Level, slog.LevelError)
	}
}

func TestDeleteObserver_DeploymentCleanupStarted_FullLifecycle(t *testing.T) {
	handler := newRecordingHandler(&slog.HandlerOptions{Level: slog.LevelDebug})
	obs := observability.NewDeleteObserver(slog.New(handler))

	ctx, probe := obs.DeploymentCleanupStarted(context.Background(), domain.DeleteDeploymentCleanupInput{
		Name:          "deployments/dep-1",
		FulfillmentID: "ful-1",
	})
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	probe.SignalReceived()
	probe.RowsDeleted()
	probe.End()

	records := handler.Records()
	messages := collectMessages(records)

	wantContains := []string{
		"delete cleanup signal received",
		"delete cleanup rows deleted",
		"delete cleanup completed",
	}
	for _, w := range wantContains {
		if !containsMessage(messages, w) {
			t.Errorf("expected log message %q, got messages: %v", w, messages)
		}
	}
}

func TestDeleteObserver_ManagedResourceCleanupStarted_FullLifecycle(t *testing.T) {
	handler := newRecordingHandler(&slog.HandlerOptions{Level: slog.LevelDebug})
	obs := observability.NewDeleteObserver(slog.New(handler))

	ctx, probe := obs.ManagedResourceCleanupStarted(context.Background(), domain.DeleteManagedResourceCleanupInput{
		ResourceType:  "test.fleetshift.io/Cluster",
		Name:          "my-cluster",
		FulfillmentID: "ful-2",
	})
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	probe.SignalReceived()
	probe.RowsDeleted()
	probe.End()

	records := handler.Records()
	messages := collectMessages(records)

	wantContains := []string{
		"delete cleanup signal received",
		"delete cleanup rows deleted",
		"delete cleanup completed",
	}
	for _, w := range wantContains {
		if !containsMessage(messages, w) {
			t.Errorf("expected log message %q, got messages: %v", w, messages)
		}
	}
}

func TestDeleteObserver_CleanupProbe_ErrorLogsAtErrorLevel(t *testing.T) {
	handler := newRecordingHandler(&slog.HandlerOptions{Level: slog.LevelDebug})
	obs := observability.NewDeleteObserver(slog.New(handler))

	_, probe := obs.DeploymentCleanupStarted(context.Background(), domain.DeleteDeploymentCleanupInput{
		Name:          "deployments/dep-err",
		FulfillmentID: "ful-err",
	})
	probe.Error(errors.New("signal timeout"))
	probe.End()

	records := handler.Records()
	var failRecord *slog.Record
	for i := range records {
		if records[i].Message == "delete cleanup failed" {
			failRecord = &records[i]
			break
		}
	}
	if failRecord == nil {
		t.Fatal("expected 'delete cleanup failed' log record")
	}
	if failRecord.Level != slog.LevelError {
		t.Errorf("level = %v, want %v", failRecord.Level, slog.LevelError)
	}
}

func TestDeleteObserver_MutateDeploymentProbe_Success(t *testing.T) {
	handler := newRecordingHandler(&slog.HandlerOptions{Level: slog.LevelDebug})
	obs := observability.NewDeleteObserver(slog.New(handler))

	_, probe := obs.MutateDeploymentStarted(context.Background(), "deployments/dep-1")
	probe.End()

	records := handler.Records()
	if !containsMessage(collectMessages(records), "mutate deployment to deleting completed") {
		t.Errorf("expected 'mutate deployment to deleting completed', got messages: %v", collectMessages(records))
	}
}

func TestDeleteObserver_MutateDeploymentProbe_ErrorLogsAtErrorLevel(t *testing.T) {
	handler := newRecordingHandler(&slog.HandlerOptions{Level: slog.LevelDebug})
	obs := observability.NewDeleteObserver(slog.New(handler))

	_, probe := obs.MutateDeploymentStarted(context.Background(), "deployments/dep-err")
	probe.Error(errors.New("begin tx failed"))
	probe.End()

	records := handler.Records()
	var failRecord *slog.Record
	for i := range records {
		if records[i].Message == "mutate deployment to deleting failed" {
			failRecord = &records[i]
			break
		}
	}
	if failRecord == nil {
		t.Fatal("expected 'mutate deployment to deleting failed' log record")
	}
	if failRecord.Level != slog.LevelError {
		t.Errorf("level = %v, want %v", failRecord.Level, slog.LevelError)
	}
}

func TestDeleteObserver_MutateManagedResourceProbe_ErrorLogsAtErrorLevel(t *testing.T) {
	handler := newRecordingHandler(&slog.HandlerOptions{Level: slog.LevelDebug})
	obs := observability.NewDeleteObserver(slog.New(handler))

	_, probe := obs.MutateManagedResourceStarted(context.Background(), "clusters", "my-cluster")
	probe.Error(errors.New("begin tx failed"))
	probe.End()

	records := handler.Records()
	var failRecord *slog.Record
	for i := range records {
		if records[i].Message == "mutate managed resource to deleting failed" {
			failRecord = &records[i]
			break
		}
	}
	if failRecord == nil {
		t.Fatal("expected 'mutate managed resource to deleting failed' log record")
	}
	if failRecord.Level != slog.LevelError {
		t.Errorf("level = %v, want %v", failRecord.Level, slog.LevelError)
	}
}

func TestDeleteObserver_MutateManagedResourceProbe_Success(t *testing.T) {
	handler := newRecordingHandler(&slog.HandlerOptions{Level: slog.LevelDebug})
	obs := observability.NewDeleteObserver(slog.New(handler))

	_, probe := obs.MutateManagedResourceStarted(context.Background(), "clusters", "my-cluster")
	probe.End()

	records := handler.Records()
	if !containsMessage(collectMessages(records), "mutate managed resource to deleting completed") {
		t.Errorf("expected 'mutate managed resource to deleting completed', got messages: %v", collectMessages(records))
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func collectMessages(records []slog.Record) []string {
	messages := make([]string, len(records))
	for i, r := range records {
		messages[i] = r.Message
	}
	return messages
}

func containsMessage(messages []string, want string) bool {
	for _, m := range messages {
		if m == want {
			return true
		}
	}
	return false
}
