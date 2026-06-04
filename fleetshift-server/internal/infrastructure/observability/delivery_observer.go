package observability

import (
	"context"
	"log/slog"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// DeliveryObserver is a [domain.DeliveryObserver] that logs delivery
// lifecycle events via [slog].
type DeliveryObserver struct {
	domain.NoOpDeliveryObserver
	logger *slog.Logger
}

// NewDeliveryObserver returns a DeliveryObserver that logs to logger.
func NewDeliveryObserver(logger *slog.Logger) *DeliveryObserver {
	return &DeliveryObserver{logger: logger.With("component", "delivery")}
}

func (o *DeliveryObserver) ReportEventStarted(ctx context.Context, deliveryID domain.DeliveryID, generation domain.Generation, event domain.DeliveryEvent) (context.Context, domain.ReportEventProbe) {
	return ctx, &reportEventProbe{
		logger:     o.logger.With(slog.String("delivery_id", string(deliveryID))),
		ctx:        ctx,
		startTime:  time.Now(),
		generation: generation,
		event:      event,
	}
}

func (o *DeliveryObserver) ReportResultStarted(ctx context.Context, deliveryID domain.DeliveryID, generation domain.Generation, result domain.DeliveryResult) (context.Context, domain.ReportResultProbe) {
	return ctx, &reportResultProbe{
		logger:     o.logger.With(slog.String("delivery_id", string(deliveryID))),
		ctx:        ctx,
		startTime:  time.Now(),
		generation: generation,
		result:     result,
	}
}

type reportEventProbe struct {
	domain.NoOpReportEventProbe
	logger     *slog.Logger
	ctx        context.Context
	startTime  time.Time
	generation domain.Generation
	event      domain.DeliveryEvent
	stale      bool
	err        error
}

func (p *reportEventProbe) Stale(reportGen, currentGen domain.Generation) {
	p.stale = true
	if p.logger.Enabled(p.ctx, slog.LevelDebug) {
		p.logger.LogAttrs(p.ctx, slog.LevelDebug, "delivery event stale",
			slog.Int64("report_generation", int64(reportGen)),
			slog.Int64("current_generation", int64(currentGen)),
		)
	}
}

func (p *reportEventProbe) Error(err error) { p.err = err }

func (p *reportEventProbe) End() {
	duration := time.Since(p.startTime)
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "delivery event failed",
			slog.Duration("duration", duration),
			slog.String("error", p.err.Error()),
		)
		return
	}
	if p.stale {
		return
	}
	level := deliveryEventLevel(p.event.Kind)
	if !p.logger.Enabled(p.ctx, level) {
		return
	}
	p.logger.LogAttrs(p.ctx, level, "delivery event",
		slog.Duration("duration", duration),
		slog.String("event_kind", string(p.event.Kind)),
		slog.String("message", p.event.Message),
	)
}

type reportResultProbe struct {
	domain.NoOpReportResultProbe
	logger     *slog.Logger
	ctx        context.Context
	startTime  time.Time
	generation domain.Generation
	result     domain.DeliveryResult
	stale      bool
	err        error
}

func (p *reportResultProbe) Stale(reportGen, currentGen domain.Generation) {
	p.stale = true
	if p.logger.Enabled(p.ctx, slog.LevelDebug) {
		p.logger.LogAttrs(p.ctx, slog.LevelDebug, "delivery result stale",
			slog.Int64("report_generation", int64(reportGen)),
			slog.Int64("current_generation", int64(currentGen)),
		)
	}
}

func (p *reportResultProbe) Error(err error) { p.err = err }

func (p *reportResultProbe) End() {
	duration := time.Since(p.startTime)
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "delivery result failed",
			slog.Duration("duration", duration),
			slog.String("error", p.err.Error()),
		)
		return
	}
	if p.stale {
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	attrs := []slog.Attr{
		slog.Duration("duration", duration),
		slog.String("state", string(p.result.State)),
	}
	if p.result.Message != "" {
		attrs = append(attrs, slog.String("message", p.result.Message))
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "delivery result done", attrs...)
}

// MultiDeliveryObserver chains multiple [domain.DeliveryObserver]
// implementations, calling each in order.
type MultiDeliveryObserver struct {
	domain.NoOpDeliveryObserver
	observers []domain.DeliveryObserver
}

// NewMultiDeliveryObserver returns an observer that delegates to all given observers.
func NewMultiDeliveryObserver(observers ...domain.DeliveryObserver) *MultiDeliveryObserver {
	return &MultiDeliveryObserver{observers: observers}
}

func (m *MultiDeliveryObserver) EventEmitted(ctx context.Context, deliveryID domain.DeliveryID, target domain.TargetInfo, event domain.DeliveryEvent) (context.Context, domain.EventEmittedProbe) {
	probes := make([]domain.EventEmittedProbe, 0, len(m.observers))
	for _, o := range m.observers {
		var p domain.EventEmittedProbe
		ctx, p = o.EventEmitted(ctx, deliveryID, target, event)
		probes = append(probes, p)
	}
	return ctx, &multiEventProbe{probes: probes}
}

func (m *MultiDeliveryObserver) Completed(ctx context.Context, deliveryID domain.DeliveryID, target domain.TargetInfo, result domain.DeliveryResult) (context.Context, domain.CompletedProbe) {
	probes := make([]domain.CompletedProbe, 0, len(m.observers))
	for _, o := range m.observers {
		var p domain.CompletedProbe
		ctx, p = o.Completed(ctx, deliveryID, target, result)
		probes = append(probes, p)
	}
	return ctx, &multiCompletedProbe{probes: probes}
}

type multiEventProbe struct {
	domain.NoOpEventEmittedProbe
	probes []domain.EventEmittedProbe
}

func (m *multiEventProbe) Error(err error) {
	for _, p := range m.probes {
		p.Error(err)
	}
}

func (m *multiEventProbe) End() {
	for _, p := range m.probes {
		p.End()
	}
}

type multiCompletedProbe struct {
	domain.NoOpCompletedProbe
	probes []domain.CompletedProbe
}

func (m *multiCompletedProbe) Error(err error) {
	for _, p := range m.probes {
		p.Error(err)
	}
}

func (m *multiCompletedProbe) End() {
	for _, p := range m.probes {
		p.End()
	}
}

func deliveryEventLevel(kind domain.DeliveryEventKind) slog.Level {
	switch kind {
	case domain.DeliveryEventWarning:
		return slog.LevelWarn
	case domain.DeliveryEventError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
