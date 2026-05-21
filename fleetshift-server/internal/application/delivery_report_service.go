package application

import (
	"context"
	"errors"
	"fmt"
	"time"

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
	store       domain.Store
	signaler    domain.FulfillmentSignaler
	attestation domain.AttestationAssembler
	observer    domain.DeliveryObserver
}

// DeliveryReportServiceOption configures a [DeliveryReportService].
type DeliveryReportServiceOption func(*DeliveryReportService)

// WithDeliveryObserver sets the observer for delivery lifecycle events.
func WithDeliveryObserver(o domain.DeliveryObserver) DeliveryReportServiceOption {
	return func(s *DeliveryReportService) { s.observer = o }
}

// WithAttestationAssembler sets the assembler used to reconstruct
// attestation bundles in [DeliveryReportService.ListActiveDeliveries].
func WithAttestationAssembler(a domain.AttestationAssembler) DeliveryReportServiceOption {
	return func(s *DeliveryReportService) { s.attestation = a }
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
// Events for deliveries already in a terminal state are silently
// ignored (the state machine rejects the transition).
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

	if err := d.TransitionTo(domain.DeliveryStateProgressing, time.Now()); err != nil {
		if errors.Is(err, domain.ErrIllegalStateTransition) {
			return nil
		}
		return fmt.Errorf("transition delivery state: %w", err)
	}
	if err := tx.Deliveries().Put(ctx, d); err != nil {
		return fmt.Errorf("update delivery state: %w", err)
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

// ReportResult records a delivery state transition and, for terminal
// states, signals the fulfillment workflow. Returns nil without
// persisting if the state machine rejects the transition (e.g. the
// delivery is already terminal).
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

	if err := d.TransitionTo(result.State, time.Now()); err != nil {
		if errors.Is(err, domain.ErrIllegalStateTransition) {
			return nil
		}
		return fmt.Errorf("transition delivery state: %w", err)
	}
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

	if s.signaler != nil && result.State.IsTerminal() {
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

// ListActiveDeliveries returns non-terminal deliveries enriched with
// target info, caller auth, and (when signed) re-assembled
// attestation. Deliveries whose fulfillment has advanced past the
// delivery's generation are filtered out because their auth and
// attestation cannot be correctly reconstructed.
//
// TODO: A Pending delivery returned here may also arrive via
// DeliveryAgent.Deliver if the addon starts up while a
// DeliverToTarget activity is in flight. Addons must deduplicate
// by DeliveryID across both paths.
func (s *DeliveryReportService) ListActiveDeliveries(ctx context.Context, targetIDs []domain.TargetID) ([]domain.ActiveDelivery, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	deliveries, err := tx.Deliveries().ListActive(ctx, targetIDs)
	if err != nil {
		return nil, fmt.Errorf("list active deliveries: %w", err)
	}
	if len(deliveries) == 0 {
		return nil, nil
	}

	fulfillments := make(map[domain.FulfillmentID]*domain.Fulfillment, len(deliveries))
	targets := make(map[domain.TargetID]domain.TargetInfo, len(deliveries))
	for _, d := range deliveries {
		fulfillments[d.FulfillmentID] = nil
		targets[d.TargetID] = domain.TargetInfo{}
	}

	for fID := range fulfillments {
		f, err := tx.Fulfillments().Get(ctx, fID)
		if errors.Is(err, domain.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("get fulfillment %s: %w", fID, err)
		}
		fulfillments[fID] = f
	}

	for tID := range targets {
		t, err := tx.Targets().Get(ctx, tID)
		if errors.Is(err, domain.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("get target %s: %w", tID, err)
		}
		targets[tID] = t
	}

	// Pre-resolve attestation evidence per fulfillment. Fulfillments
	// without provenance (unsigned / token-passthrough) get nil.
	evidence := make(map[domain.FulfillmentID]*domain.ResolvedEvidence, len(fulfillments))
	for fID, f := range fulfillments {
		if f == nil || f.Provenance == nil {
			continue
		}
		ev, err := s.attestation.Resolve(ctx, tx, f)
		if err != nil {
			// TODO: revisit this / need to add observers
			continue // best-effort: skip attestation if evidence resolution fails
		}
		evidence[fID] = ev
	}

	var result []domain.ActiveDelivery
	for _, d := range deliveries {
		f := fulfillments[d.FulfillmentID]
		if f == nil {
			continue // fulfillment deleted
		}
		if d.Generation < f.Generation {
			continue // stale: fulfillment has advanced
		}

		t, ok := targets[d.TargetID]
		if !ok || t.ID == "" {
			continue // target deleted
		}

		ad := domain.ActiveDelivery{
			Delivery: d,
			Target:   t,
			Auth:     f.Auth,
		}
		if ev := evidence[d.FulfillmentID]; ev != nil {
			ad.Attestation = domain.AssembleDeliverAttestation(*f, d.Manifests, ev)
		}
		result = append(result, ad)
	}
	return result, nil
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
