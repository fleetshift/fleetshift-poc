package domain

import "context"

// DeliverySignaler manages the domain responsibilities of a single
// delivery: updating delivery state in the repository and signaling
// the fulfillment workflow on completion. It calls
// [DeliveryObserver] methods at the appropriate points for
// observability, creating a short-lived probe per operation so that
// each receives the caller's context.
//
// DeliverySignaler is a concrete struct, not an interface. There is
// exactly one valid implementation: update state + signal workflow.
// Testability comes from injecting dependencies (repos, signal func),
// not from interface substitution.
//
// A zero-value &DeliverySignaler{} is safe to use: nil fields mean no
// state updates, no signaling, and no observer callbacks.
type DeliverySignaler struct {
	FulfillmentID FulfillmentID
	DeliveryID    DeliveryID
	Target        TargetInfo
	Store         Store
	Signal        func(context.Context, FulfillmentID, FulfillmentEvent) error
	observer      DeliveryObserver
	progressed    bool
}

// NewDeliverySignaler creates a DeliverySignaler. If observer is nil,
// observer calls are skipped.
func NewDeliverySignaler(
	fulfillmentID FulfillmentID,
	deliveryID DeliveryID,
	target TargetInfo,
	store Store,
	signal func(context.Context, FulfillmentID, FulfillmentEvent) error,
	observer DeliveryObserver,
) *DeliverySignaler {
	return &DeliverySignaler{
		FulfillmentID: fulfillmentID,
		DeliveryID:    deliveryID,
		Target:        target,
		Store:         store,
		Signal:        signal,
		observer:      observer,
	}
}

// Emit records a delivery event. On the first call it transitions the
// delivery to [DeliveryStateProgressing] in the repository within a
// single transaction (read-modify-write).
func (s *DeliverySignaler) Emit(ctx context.Context, event DeliveryEvent) {
	var probe EventEmittedProbe = NoOpEventEmittedProbe{}
	if s.observer != nil {
		ctx, probe = s.observer.EventEmitted(ctx, s.DeliveryID, s.Target, event)
	}
	defer probe.End()
	if !s.progressed && s.Store != nil {
		s.progressed = true
		if err := s.updateDeliveryState(ctx, DeliveryStateProgressing); err != nil {
			probe.Error(err)
		}
	}
}

// Done updates the delivery's terminal state in the repository,
// signals the workflow, and notifies the observer. The state update
// runs in its own transaction; signaling happens after commit.
func (s *DeliverySignaler) Done(ctx context.Context, result DeliveryResult) {
	var probe CompletedProbe = NoOpCompletedProbe{}
	if s.observer != nil {
		ctx, probe = s.observer.Completed(ctx, s.DeliveryID, s.Target, result)
	}
	defer probe.End()
	if s.Store != nil {
		if err := s.updateDeliveryState(ctx, result.State); err != nil {
			probe.Error(err)
		}
	}
	if s.Signal != nil {
		if err := s.Signal(ctx, s.FulfillmentID, FulfillmentEvent{
			DeliveryCompleted: &DeliveryCompletionEvent{
				DeliveryID: s.DeliveryID,
				Result:     result,
			},
		}); err != nil {
			probe.Error(err)
		}
	}
}

// updateDeliveryState performs a transactional read-modify-write to
// update the delivery's state.
func (s *DeliverySignaler) updateDeliveryState(ctx context.Context, state DeliveryState) error {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	d, err := tx.Deliveries().Get(ctx, s.DeliveryID)
	if err != nil {
		return err
	}
	d.State = state
	if err := tx.Deliveries().Put(ctx, d); err != nil {
		return err
	}
	return tx.Commit()
}
