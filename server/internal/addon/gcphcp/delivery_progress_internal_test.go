package gcphcp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// TestDeliveryProgress_CompleteRetriesTransientReportResult verifies Complete
// retries ReportResult on transient errors before succeeding.
func TestDeliveryProgress_CompleteRetriesTransientReportResult(t *testing.T) {
	reporter := &flakyCompleteReporter{failTimes: 2}
	progress := newDeliveryProgress(reporter, "d1", 3)

	err := progress.Complete(context.Background(), domain.DeliveryResult{
		State: domain.DeliveryStateDelivered,
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if got := reporter.calls.Load(); got != 3 {
		t.Fatalf("ReportResult calls = %d, want 3 (2 transient + 1 success)", got)
	}
	if reporter.last.State != domain.DeliveryStateDelivered {
		t.Fatalf("last result state = %q, want %q", reporter.last.State, domain.DeliveryStateDelivered)
	}
}

// flakyCompleteReporter fails ReportResult failTimes times, then succeeds.
type flakyCompleteReporter struct {
	failTimes int
	calls     atomic.Int32
	last      domain.DeliveryResult
}

// ReportEvent implements [domain.DeliveryReporter].
func (r *flakyCompleteReporter) ReportEvent(context.Context, domain.DeliveryID, domain.Generation, domain.DeliveryEvent) error {
	return nil
}

// ReportResult fails until failTimes transient failures have been recorded.
func (r *flakyCompleteReporter) ReportResult(
	_ context.Context,
	_ domain.DeliveryID,
	_ domain.Generation,
	result domain.DeliveryResult,
) error {
	n := int(r.calls.Add(1))
	if n <= r.failTimes {
		return errors.New("temporary report unavailable")
	}
	r.last = result
	return nil
}

// ListActiveDeliveries implements [domain.DeliveryReporter].
func (r *flakyCompleteReporter) ListActiveDeliveries(context.Context, []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, nil
}
