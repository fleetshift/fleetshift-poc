package gcphcp

import (
	"context"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// deliveryProgress adapts the platform's DeliveryReporter into a
// per-delivery handle that the reconciler and its subsystems can call
// without threading reporter + deliveryID separately.
type deliveryProgress struct {
	reporter   domain.DeliveryReporter
	deliveryID domain.DeliveryID
	generation domain.Generation
}

func newDeliveryProgress(reporter domain.DeliveryReporter, id domain.DeliveryID, gen domain.Generation) *deliveryProgress {
	return &deliveryProgress{reporter: reporter, deliveryID: id, generation: gen}
}

// Event reports a non-terminal delivery event (progress, warning, error).
// Errors are intentionally swallowed — progress events are informational
// and must not abort the reconciler.
func (p *deliveryProgress) Event(ctx context.Context, event domain.DeliveryEvent) {
	if p == nil || p.reporter == nil {
		return
	}
	_ = p.reporter.ReportEvent(ctx, p.deliveryID, p.generation, event)
}

// Info emits an informational progress event.
func (p *deliveryProgress) Info(ctx context.Context, message string) {
	p.Event(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   message,
	})
}

// Warn emits a warning event.
func (p *deliveryProgress) Warn(ctx context.Context, message string) {
	p.Event(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventWarning,
		Message:   message,
	})
}

// Complete reports a terminal delivery result and signals fulfillment
// completion. Unlike Event, the error is returned so callers can log
// or react when the platform fails to record the outcome.
func (p *deliveryProgress) Complete(ctx context.Context, result domain.DeliveryResult) error {
	if p == nil || p.reporter == nil {
		return nil
	}
	return p.reporter.ReportResult(ctx, p.deliveryID, p.generation, result)
}
