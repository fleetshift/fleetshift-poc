package domain

import "context"

// DeliveryObserver is called at key points during delivery lifecycle
// reporting. Each method corresponds to one [DeliveryReporter]
// operation, receives the caller's context, and returns a short-lived
// probe for that operation.
// Implementations should embed [NoOpDeliveryObserver] for forward
// compatibility with new methods added to this interface.
type DeliveryObserver interface {
	// ReportEventStarted is called when the delivery agent reports a
	// non-terminal event via [DeliveryReporter.ReportEvent].
	ReportEventStarted(ctx context.Context, deliveryID DeliveryID, generation Generation, event DeliveryEvent) (context.Context, ReportEventProbe)

	// ReportResultStarted is called when the delivery agent reports a
	// terminal result via [DeliveryReporter.ReportResult].
	ReportResultStarted(ctx context.Context, deliveryID DeliveryID, generation Generation, result DeliveryResult) (context.Context, ReportResultProbe)
}

// ReportEventProbe tracks a single [DeliveryReporter.ReportEvent]
// invocation. Implementations should embed [NoOpReportEventProbe]
// for forward compatibility.
type ReportEventProbe interface {
	// Stale is called when the report's generation does not match
	// the delivery's current generation and is silently discarded.
	Stale(reportGen, currentGen Generation)

	// Error is called when an error occurs.
	Error(err error)

	// End signals the operation is complete (for timing). Called via defer.
	End()
}

// ReportResultProbe tracks a single [DeliveryReporter.ReportResult]
// invocation. Implementations should embed [NoOpReportResultProbe]
// for forward compatibility.
type ReportResultProbe interface {
	// Stale is called when the report's generation does not match
	// the delivery's current generation and is silently discarded.
	Stale(reportGen, currentGen Generation)

	// Error is called when an error occurs.
	Error(err error)

	// End signals the operation is complete (for timing). Called via defer.
	End()
}

// NoOpDeliveryObserver is a [DeliveryObserver] that returns no-op probes.
type NoOpDeliveryObserver struct{}

func (NoOpDeliveryObserver) ReportEventStarted(ctx context.Context, _ DeliveryID, _ Generation, _ DeliveryEvent) (context.Context, ReportEventProbe) {
	return ctx, NoOpReportEventProbe{}
}

func (NoOpDeliveryObserver) ReportResultStarted(ctx context.Context, _ DeliveryID, _ Generation, _ DeliveryResult) (context.Context, ReportResultProbe) {
	return ctx, NoOpReportResultProbe{}
}

// NoOpReportEventProbe is a [ReportEventProbe] that discards all calls.
type NoOpReportEventProbe struct{}

func (NoOpReportEventProbe) Stale(Generation, Generation) {}
func (NoOpReportEventProbe) Error(error)                  {}
func (NoOpReportEventProbe) End()                         {}

// NoOpReportResultProbe is a [ReportResultProbe] that discards all calls.
type NoOpReportResultProbe struct{}

func (NoOpReportResultProbe) Stale(Generation, Generation) {}
func (NoOpReportResultProbe) Error(error)                  {}
func (NoOpReportResultProbe) End()                         {}
