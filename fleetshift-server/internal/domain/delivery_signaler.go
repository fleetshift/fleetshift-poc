package domain

import "context"

// DeliverySignaler manages the domain responsibilities of a single
// delivery: updating delivery state in the repository and signaling
// the deployment workflow on completion. It calls
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
	DeploymentID DeploymentID
	DeliveryID   DeliveryID
	Target       TargetInfo
	Deliveries   DeliveryRepository
	Signal       func(context.Context, DeploymentID, DeploymentEvent) error
	observer     DeliveryObserver
	progressed   bool
}

// NewDeliverySignaler creates a DeliverySignaler. If observer is nil,
// observer calls are skipped.
func NewDeliverySignaler(
	deploymentID DeploymentID,
	deliveryID DeliveryID,
	target TargetInfo,
	deliveries DeliveryRepository,
	signal func(context.Context, DeploymentID, DeploymentEvent) error,
	observer DeliveryObserver,
) *DeliverySignaler {
	return &DeliverySignaler{
		DeploymentID: deploymentID,
		DeliveryID:   deliveryID,
		Target:       target,
		Deliveries:   deliveries,
		Signal:       signal,
		observer:     observer,
	}
}

// Emit records a delivery event. On the first call it transitions the
// delivery to [DeliveryStateProgressing] in the repository.
func (s *DeliverySignaler) Emit(ctx context.Context, event DeliveryEvent) {
	var probe EventEmittedProbe = NoOpEventEmittedProbe{}
	if s.observer != nil {
		ctx, probe = s.observer.EventEmitted(ctx, s.DeliveryID, s.Target, event)
	}
	defer probe.End()
	if !s.progressed && s.Deliveries != nil {
		s.progressed = true
		d, err := s.Deliveries.Get(ctx, s.DeliveryID)
		if err != nil {
			probe.Error(err)
			return
		}
		d.State = DeliveryStateProgressing
		if err := s.Deliveries.Put(ctx, d); err != nil {
			probe.Error(err)
		}
	}
}

// Done updates the delivery's terminal state in the repository,
// signals the workflow, and notifies the observer.
func (s *DeliverySignaler) Done(ctx context.Context, result DeliveryResult) {
	var probe CompletedProbe = NoOpCompletedProbe{}
	if s.observer != nil {
		ctx, probe = s.observer.Completed(ctx, s.DeliveryID, s.Target, result)
	}
	defer probe.End()
	if s.Deliveries != nil {
		d, err := s.Deliveries.Get(ctx, s.DeliveryID)
		if err != nil {
			probe.Error(err)
		} else {
			d.State = result.State
			if err := s.Deliveries.Put(ctx, d); err != nil {
				probe.Error(err)
			}
		}
	}
	if s.Signal != nil {
		if err := s.Signal(ctx, s.DeploymentID, DeploymentEvent{
			DeliveryCompleted: &DeliveryCompletionEvent{
				DeliveryID: s.DeliveryID,
				Result:     result,
			},
		}); err != nil {
			probe.Error(err)
		}
	}
}
