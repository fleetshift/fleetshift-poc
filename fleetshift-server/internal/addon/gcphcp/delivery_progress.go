package gcphcp

import (
	"context"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// deliveryProgress adapts the platform's DeliveryReporter into a
// per-delivery handle that the reconciler and its subsystems can call
// without threading reporter + deliveryID separately.
type deliveryProgress struct {
	reporter   domain.DeliveryReporter
	deliveryID domain.DeliveryID
}

func newDeliveryProgress(reporter domain.DeliveryReporter, id domain.DeliveryID) *deliveryProgress {
	return &deliveryProgress{reporter: reporter, deliveryID: id}
}

// Event reports a non-terminal delivery event (progress, warning, error).
// Errors are intentionally swallowed — progress events are informational
// and must not abort the reconciler.
func (p *deliveryProgress) Event(ctx context.Context, event domain.DeliveryEvent) {
	if p == nil || p.reporter == nil {
		return
	}
	_ = p.reporter.ReportEvent(ctx, p.deliveryID, event)
}

// Complete reports a terminal delivery result and signals fulfillment
// completion. Unlike Event, the error is returned so callers can log
// or react when the platform fails to record the outcome.
func (p *deliveryProgress) Complete(ctx context.Context, result domain.DeliveryResult) error {
	if p == nil || p.reporter == nil {
		return nil
	}
	return p.reporter.ReportResult(ctx, p.deliveryID, result)
}
