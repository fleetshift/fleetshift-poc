package domain

import "context"

// FulfillmentObserver is called at key points during fulfillment
// orchestration. Implementations should embed [NoOpFulfillmentObserver]
// for forward compatibility with new methods added to this interface.
type FulfillmentObserver interface {
	// RunStarted is called when the orchestration workflow begins
	// processing a fulfillment. Returns a potentially modified context
	// and a probe to track the run.
	RunStarted(ctx context.Context, fulfillmentID FulfillmentID) (context.Context, FulfillmentRunProbe)
}

// FulfillmentRunProbe tracks a single orchestration run.
// Implementations should embed [NoOpFulfillmentRunProbe] for forward
// compatibility.
type FulfillmentRunProbe interface {
	// EventReceived is called when the workflow receives a fulfillment event.
	EventReceived(event FulfillmentEvent)

	// StateChanged is called when the fulfillment transitions to a new state.
	StateChanged(state FulfillmentState)

	// ManifestsFiltered is called after [FilterAcceptedManifests] runs for
	// a target. total is the pre-filter count; accepted is the post-filter
	// count. When accepted is zero the target receives no delivery.
	ManifestsFiltered(target TargetInfo, total, accepted int)

	// DeliveryOutputsProcessed is called after [ProcessDeliveryOutputs]
	// registers provisioned targets and stores secrets from a delivery
	// result.
	DeliveryOutputsProcessed(targets []ProvisionedTarget, secrets int)

	// Error is called when an error occurs during the run.
	Error(err error)

	// End signals the run is complete (for timing). Called via defer.
	End()
}

// NoOpFulfillmentObserver is a [FulfillmentObserver] that returns a no-op probe.
type NoOpFulfillmentObserver struct{}

func (NoOpFulfillmentObserver) RunStarted(ctx context.Context, _ FulfillmentID) (context.Context, FulfillmentRunProbe) {
	return ctx, NoOpFulfillmentRunProbe{}
}

// NoOpFulfillmentRunProbe is a [FulfillmentRunProbe] that discards all events.
type NoOpFulfillmentRunProbe struct{}

func (NoOpFulfillmentRunProbe) EventReceived(FulfillmentEvent)                    {}
func (NoOpFulfillmentRunProbe) StateChanged(FulfillmentState)                     {}
func (NoOpFulfillmentRunProbe) ManifestsFiltered(TargetInfo, int, int)            {}
func (NoOpFulfillmentRunProbe) DeliveryOutputsProcessed([]ProvisionedTarget, int) {}
func (NoOpFulfillmentRunProbe) Error(error)                                       {}
func (NoOpFulfillmentRunProbe) End()                                              {}
