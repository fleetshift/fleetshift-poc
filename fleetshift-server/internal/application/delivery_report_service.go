package application

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// DeliveryReportService implements [domain.DeliveryReporter] as an
// application-layer service. It handles addon-to-platform delivery
// lifecycle updates: transitioning delivery state, signaling
// fulfillment workflows, and calling [domain.DeliveryObserver].
//
// This service stays long term. Even with gRPC transport, the
// transport handler delegates to this service.
//
// TODO: Naming may change
type DeliveryReportService struct {
	store    domain.Store
	signaler domain.FulfillmentSignaler
	observer domain.DeliveryObserver
}

// DeliveryReportServiceOption configures a [DeliveryReportService].
type DeliveryReportServiceOption func(*DeliveryReportService)

// WithDeliveryObserver sets the observer for delivery lifecycle events.
func WithDeliveryObserver(o domain.DeliveryObserver) DeliveryReportServiceOption {
	return func(s *DeliveryReportService) { s.observer = o }
}

// NewDeliveryReportService creates a service with the given
// dependencies. The signaler is called to notify the fulfillment
// workflow when a delivery reaches a terminal state; any
// [domain.Registry] satisfies [domain.FulfillmentSignaler].
func NewDeliveryReportService(
	store domain.Store,
	signaler domain.FulfillmentSignaler,
	opts ...DeliveryReportServiceOption,
) *DeliveryReportService {
	s := &DeliveryReportService{
		store:    store,
		signaler: signaler,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// ReportEvent records a non-terminal delivery event. If the delivery
// is not yet in [domain.DeliveryStateProgressing], it transitions it.
func (s *DeliveryReportService) ReportEvent(ctx context.Context, deliveryID domain.DeliveryID, event domain.DeliveryEvent) error {
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	d, err := tx.Deliveries().Get(ctx, deliveryID)
	if err != nil {
		return fmt.Errorf("get delivery: %w", err)
	}

	needsTransition := d.State != domain.DeliveryStateProgressing
	if needsTransition {
		d.State = domain.DeliveryStateProgressing
		if err := tx.Deliveries().Put(ctx, d); err != nil {
			return fmt.Errorf("update delivery state: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	if s.observer != nil {
		target := s.lookupTarget(ctx, d.TargetID)
		_, probe := s.observer.EventEmitted(ctx, deliveryID, target, event)
		probe.End()
	}
	return nil
}

// ReportResult records the terminal outcome of a delivery, updates
// the delivery's state, and signals the fulfillment workflow.
// TODO: This is not atomic with fulfillment signal; requires own workflow
func (s *DeliveryReportService) ReportResult(ctx context.Context, deliveryID domain.DeliveryID, result domain.DeliveryResult) error {
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	d, err := tx.Deliveries().Get(ctx, deliveryID)
	if err != nil {
		return fmt.Errorf("get delivery: %w", err)
	}

	d.State = result.State
	if err := tx.Deliveries().Put(ctx, d); err != nil {
		return fmt.Errorf("update delivery state: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	if s.observer != nil {
		target := s.lookupTarget(ctx, d.TargetID)
		_, probe := s.observer.Completed(ctx, deliveryID, target, result)
		probe.End()
	}

	if s.signaler != nil {
		if err := s.signaler.SignalFulfillmentEvent(ctx, d.FulfillmentID, domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Result:     result,
			},
		}); err != nil {
			return fmt.Errorf("signal workflow: %w", err)
		}
	}
	return nil
}

// ListActiveDeliveries returns deliveries in non-terminal states.
// If targetIDs is non-empty, only deliveries for those targets are
// returned.
func (s *DeliveryReportService) ListActiveDeliveries(ctx context.Context, targetIDs []domain.TargetID) ([]domain.Delivery, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	deliveries, err := tx.Deliveries().ListActive(ctx, targetIDs)
	if err != nil {
		return nil, fmt.Errorf("list active deliveries: %w", err)
	}
	return deliveries, nil
}

// lookupTarget is a best-effort read of the target for observer
// callbacks. Returns a minimal TargetInfo with just the ID if the
// lookup fails.
func (s *DeliveryReportService) lookupTarget(ctx context.Context, targetID domain.TargetID) domain.TargetInfo {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return domain.TargetInfo{ID: targetID}
	}
	defer tx.Rollback()
	t, err := tx.Targets().Get(ctx, targetID)
	if err != nil {
		return domain.TargetInfo{ID: targetID}
	}
	return t
}
