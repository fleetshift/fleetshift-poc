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
	now         func() time.Time
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

// WithDeliveryReportClock overrides the wall-clock used for delivery
// state transition timestamps. Defaults to [time.Now]. A nil fn is
// treated as a no-op to prevent nil-dereference panics at runtime.
func WithDeliveryReportClock(fn func() time.Time) DeliveryReportServiceOption {
	return func(s *DeliveryReportService) {
		if fn != nil {
			s.now = fn
		}
	}
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
		observer: domain.NoOpDeliveryObserver{},
		now:      time.Now,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// ReportEvent records a non-terminal delivery event. If the delivery
// is not yet in [domain.DeliveryStateProgressing], it transitions it.
// When this is the first transition out of [domain.DeliveryStatePending],
// a [domain.DeliveryAckedEvent] signal is sent to the fulfillment
// workflow so it knows the addon received the work.
// Events for deliveries already in a terminal state are silently
// ignored (the state machine rejects the transition).
// Reports whose generation does not match the delivery's current
// generation are silently discarded (stale work).
// FIXME: This is not atomic with fulfillment signal; requires own workflow
func (s *DeliveryReportService) ReportEvent(ctx context.Context, deliveryID domain.DeliveryID, generation domain.Generation, event domain.DeliveryEvent) error {
	ctx, probe := s.observer.ReportEventStarted(ctx, deliveryID, generation, event)
	defer probe.End()

	tx, err := s.store.Begin(ctx)
	if err != nil {
		probe.Error(err)
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	d, err := tx.Deliveries().Get(ctx, deliveryID)
	if err != nil {
		probe.Error(err)
		return fmt.Errorf("get delivery: %w", err)
	}

	if d.Generation() != generation {
		probe.Stale(generation, d.Generation())
		return nil
	}

	wasPending := d.State() == domain.DeliveryStatePending

	if err := d.TransitionTo(domain.DeliveryStateProgressing, s.now()); err != nil {
		if errors.Is(err, domain.ErrIllegalStateTransition) {
			return nil
		}
		probe.Error(err)
		return fmt.Errorf("transition delivery state: %w", err)
	}
	if err := tx.Deliveries().Put(ctx, d); err != nil {
		probe.Error(err)
		return fmt.Errorf("update delivery state: %w", err)
	}

	if err := tx.Commit(); err != nil {
		probe.Error(err)
		return fmt.Errorf("commit: %w", err)
	}

	if wasPending && s.signaler != nil {
		if err := s.signaler.SignalFulfillmentEvent(ctx, d.FulfillmentID(), domain.FulfillmentEvent{
			DeliveryAcked: &domain.DeliveryAckedEvent{DeliveryID: deliveryID, Generation: generation},
		}); err != nil {
			return fmt.Errorf("signal ack: %w", err)
		}
	}
	return nil
}

// ReportResult records a delivery state transition and signals the
// fulfillment workflow on two occasions:
//   - When the delivery first transitions out of [domain.DeliveryStatePending]
//     (ack signal), telling the workflow the addon received the work.
//   - When a terminal state is reached (completion signal), telling
//     the workflow the work is done.
//
// Reports whose generation does not match the delivery's current
// generation are silently discarded (stale work).
//
// Returns nil without persisting if the state machine rejects the
// transition (e.g. the delivery is already terminal).
// FIXME: This is not atomic with fulfillment signal; requires own workflow
func (s *DeliveryReportService) ReportResult(ctx context.Context, deliveryID domain.DeliveryID, generation domain.Generation, result domain.DeliveryResult) error {
	ctx, probe := s.observer.ReportResultStarted(ctx, deliveryID, generation, result)
	defer probe.End()

	tx, err := s.store.Begin(ctx)
	if err != nil {
		probe.Error(err)
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	d, err := tx.Deliveries().Get(ctx, deliveryID)
	if err != nil {
		probe.Error(err)
		return fmt.Errorf("get delivery: %w", err)
	}

	if d.Generation() != generation {
		probe.Stale(generation, d.Generation())
		return nil
	}

	wasPending := d.State() == domain.DeliveryStatePending

	if err := d.TransitionTo(result.State, s.now()); err != nil {
		if errors.Is(err, domain.ErrIllegalStateTransition) {
			return nil
		}
		probe.Error(err)
		return fmt.Errorf("transition delivery state: %w", err)
	}
	if err := tx.Deliveries().Put(ctx, d); err != nil {
		probe.Error(err)
		return fmt.Errorf("update delivery state: %w", err)
	}
	if err := tx.Commit(); err != nil {
		probe.Error(err)
		return fmt.Errorf("commit: %w", err)
	}

	if s.signaler != nil {
		// TODO: maybe result should not accept non terminal states? not sure it makes sense otherwise
		if wasPending && !result.State.IsTerminal() {
			if err := s.signaler.SignalFulfillmentEvent(ctx, d.FulfillmentID(), domain.FulfillmentEvent{
				DeliveryAcked: &domain.DeliveryAckedEvent{DeliveryID: deliveryID, Generation: generation},
			}); err != nil {
				return fmt.Errorf("signal ack: %w", err)
			}
		}
		if result.State.IsTerminal() {
			if err := s.signaler.SignalFulfillmentEvent(ctx, d.FulfillmentID(), domain.FulfillmentEvent{
				DeliveryCompleted: &domain.DeliveryCompletionEvent{
					DeliveryID: deliveryID,
					Generation: generation,
					Result:     result,
				},
			}); err != nil {
				return fmt.Errorf("signal workflow: %w", err)
			}
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
		fulfillments[d.FulfillmentID()] = nil
		targets[d.TargetID()] = domain.TargetInfo{}
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
		if f == nil || f.Provenance() == nil {
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
		f := fulfillments[d.FulfillmentID()]
		if f == nil {
			continue // fulfillment deleted
		}
		if d.Generation() < f.Generation() {
			continue // stale: fulfillment has advanced
		}

		t, ok := targets[d.TargetID()]
		if !ok || t.ID() == "" {
			continue // target deleted
		}

		ad := domain.ActiveDelivery{
			Delivery: d,
			Target:   t,
			Auth:     f.Auth(),
		}
		if ev := evidence[d.FulfillmentID()]; ev != nil {
			ad.Attestation = domain.AssembleDeliverAttestation(*f, d.Manifests(), ev)
		}
		result = append(result, ad)
	}
	return result, nil
}
