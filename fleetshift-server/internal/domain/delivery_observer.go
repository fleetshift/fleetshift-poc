package domain

import "context"

// DeliveryObserver is called at key points during delivery lifecycle
// reporting. Each method corresponds to one [DeliveryReporter]
// operation, receives the caller's context, and returns a short-lived
// probe for that operation.
// Implementations should embed [NoOpDeliveryObserver] for forward
// compatibility with new methods added to this interface.
type DeliveryObserver interface {
	// EventEmitted is called when the delivery agent emits an event
	// via [DeliveryReporter.ReportEvent].
	EventEmitted(ctx context.Context, deliveryID DeliveryID, target TargetInfo, event DeliveryEvent) (context.Context, EventEmittedProbe)

	// Completed is called when the delivery reaches a terminal state
	// via [DeliveryReporter.ReportResult].
	Completed(ctx context.Context, deliveryID DeliveryID, target TargetInfo, result DeliveryResult) (context.Context, CompletedProbe)
}

// EventEmittedProbe tracks a single [DeliveryReporter.ReportEvent]
// invocation. Implementations should embed [NoOpEventEmittedProbe]
// for forward compatibility.
type EventEmittedProbe interface {
	Error(err error)
	End()
}

// CompletedProbe tracks a single [DeliveryReporter.ReportResult]
// invocation. Implementations should embed [NoOpCompletedProbe] for
// forward compatibility.
type CompletedProbe interface {
	Error(err error)
	End()
}

// NoOpDeliveryObserver is a [DeliveryObserver] that returns no-op probes.
type NoOpDeliveryObserver struct{}

func (NoOpDeliveryObserver) EventEmitted(ctx context.Context, _ DeliveryID, _ TargetInfo, _ DeliveryEvent) (context.Context, EventEmittedProbe) {
	return ctx, NoOpEventEmittedProbe{}
}

func (NoOpDeliveryObserver) Completed(ctx context.Context, _ DeliveryID, _ TargetInfo, _ DeliveryResult) (context.Context, CompletedProbe) {
	return ctx, NoOpCompletedProbe{}
}

// NoOpEventEmittedProbe is an [EventEmittedProbe] that discards all calls.
type NoOpEventEmittedProbe struct{}

func (NoOpEventEmittedProbe) Error(error) {}
func (NoOpEventEmittedProbe) End()        {}

// NoOpCompletedProbe is a [CompletedProbe] that discards all calls.
type NoOpCompletedProbe struct{}

func (NoOpCompletedProbe) Error(error) {}
func (NoOpCompletedProbe) End()        {}
