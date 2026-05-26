package observability_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/observability"
)

func TestFulfillmentObserver_RunStarted_LogsAndReturnsProbe(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	ctx, probe := obs.RunStarted(context.Background(), "ful-1")
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}

	records := handler.Records()
	if len(records) != 1 {
		t.Fatalf("expected 1 log record from RunStarted, got %d", len(records))
	}
	if records[0].Message != "fulfillment run started" {
		t.Errorf("message = %q, want %q", records[0].Message, "fulfillment run started")
	}

	probe.End()
}

func TestFulfillmentRunProbe_FullLifecycle(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.RunStarted(context.Background(), "ful-2")

	probe.StateChanged(domain.FulfillmentStateActive)
	probe.End()

	records := handler.Records()
	messages := make([]string, len(records))
	for i, r := range records {
		messages[i] = r.Message
	}

	want := []string{
		"fulfillment run started",
		"fulfillment state changed",
		"fulfillment run completed",
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

func TestFulfillmentRunProbe_ManifestsFiltered_AllDroppedLogsWarning(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.RunStarted(context.Background(), "ful-filter")

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

func TestFulfillmentRunProbe_ManifestsFiltered_PartialLogsDebug(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.RunStarted(context.Background(), "ful-partial")

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

func TestFulfillmentRunProbe_ErrorLogsAtErrorLevel(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.RunStarted(context.Background(), "ful-3")

	probe.Error(domain.ErrNotFound)
	probe.End()

	records := handler.Records()
	var endRecord *slog.Record
	for i := range records {
		if records[i].Message == "fulfillment run failed" {
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

func TestFulfillmentRunProbe_DispatchCycle_FullLifecycle(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.RunStarted(context.Background(), "ful-dispatch")

	dprobe := probe.DispatchCycleStarted(2, 1)
	dprobe.Dispatched("d1:t1", false)
	dprobe.AckReceived("d1:t1")
	dprobe.Completed("d1:t1", domain.DeliveryStateDelivered)
	dprobe.End()
	probe.End()

	records := handler.Records()
	messages := make([]string, len(records))
	for i, r := range records {
		messages[i] = r.Message
	}

	wantContains := []string{
		"dispatch cycle started",
		"delivery dispatched",
		"delivery ack received",
		"delivery completed",
		"dispatch cycle completed",
	}
	for _, want := range wantContains {
		found := false
		for _, msg := range messages {
			if msg == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected log message %q, got messages: %v", want, messages)
		}
	}
}

func TestFulfillmentRunProbe_DispatchCycle_AckTimeout(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.RunStarted(context.Background(), "ful-timeout")

	dprobe := probe.DispatchCycleStarted(1, 1)
	dprobe.Dispatched("d1:t1", false)
	dprobe.AckTimeout(1)
	dprobe.End()
	probe.End()

	records := handler.Records()
	var timeoutRecord *slog.Record
	for i := range records {
		if records[i].Message == "delivery ack timeout" {
			timeoutRecord = &records[i]
			break
		}
	}
	if timeoutRecord == nil {
		t.Fatal("expected 'delivery ack timeout' log record")
	}
	if timeoutRecord.Level != slog.LevelWarn {
		t.Errorf("level = %v, want %v", timeoutRecord.Level, slog.LevelWarn)
	}
}

func TestAcquireLockProbe_FullLifecycle(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.AcquireLockStarted(context.Background(), "ful-lock")

	probe.LockAcquired(false)
	probe.PoolLoaded(3)
	probe.EvidenceResolved(true)
	probe.End()

	records := handler.Records()
	messages := make([]string, len(records))
	for i, r := range records {
		messages[i] = r.Message
	}

	wantContains := []string{
		"acquire lock started",
		"orchestration lock acquired",
		"target pool loaded",
		"attestation evidence resolved",
		"acquire lock completed",
	}
	for _, want := range wantContains {
		found := false
		for _, msg := range messages {
			if msg == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected log message %q, got messages: %v", want, messages)
		}
	}
}

func TestDeliverProbe_NewDelivery(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.DeliverStarted(context.Background(), domain.DeliverInput{
		FulfillmentID: "ful-1",
		DeliveryID:    "d1:t1",
		Target:        domain.TargetInfo{ID: "t1"},
	})

	probe.NewDelivery()
	probe.End()

	records := handler.Records()
	found := false
	for _, r := range records {
		if r.Message == "new delivery created" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'new delivery created' log record")
	}
}

func TestRemoveProbe_Withdrawn(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.RemoveStarted(context.Background(), domain.RemoveInput{
		FulfillmentID: "ful-1",
		DeliveryID:    "d1:t1",
		Target:        domain.TargetInfo{ID: "t1"},
	})

	probe.Withdrawn()
	probe.End()

	records := handler.Records()
	found := false
	for _, r := range records {
		if r.Message == "delivery withdrawn" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'delivery withdrawn' log record")
	}
}

func TestPersistReconciliationProbe_FullLifecycle(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.PersistReconciliationStarted(context.Background(), "ful-persist")

	probe.Persisted(domain.FulfillmentStateActive, false)
	probe.End()

	records := handler.Records()
	found := false
	for _, r := range records {
		if r.Message == "reconciliation persisted" {
			found = true
			if r.Level != slog.LevelInfo {
				t.Errorf("level = %v, want %v", r.Level, slog.LevelInfo)
			}
			break
		}
	}
	if !found {
		t.Error("expected 'reconciliation persisted' log record")
	}
}

func TestProcessOutputsProbe_TargetsRegistered(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.ProcessOutputsStarted(context.Background())

	probe.SecretsStored(1)
	probe.TargetsRegistered(2)
	probe.End()

	records := handler.Records()
	found := false
	for _, r := range records {
		if r.Message == "delivery outputs processed" {
			found = true
			if r.Level != slog.LevelInfo {
				t.Errorf("level = %v, want %v", r.Level, slog.LevelInfo)
			}
			break
		}
	}
	if !found {
		t.Error("expected 'delivery outputs processed' log record")
	}
}
