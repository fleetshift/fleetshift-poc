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

func (o *DeliveryObserver) EventEmitted(ctx context.Context, deliveryID domain.DeliveryID, target domain.TargetInfo, event domain.DeliveryEvent) (context.Context, domain.EventEmittedProbe) {
	logger := o.loggerFor(deliveryID, target)
	level := deliveryEventLevel(event.Kind)
	if logger.Enabled(ctx, level) {
		logger.LogAttrs(ctx, level, "delivery event",
			slog.String("event_kind", string(event.Kind)),
			slog.String("message", event.Message),
		)
	}
	return ctx, &eventEmittedProbe{
		logger:    logger,
		ctx:       ctx,
		startTime: time.Now(),
	}
}

func (o *DeliveryObserver) Completed(ctx context.Context, deliveryID domain.DeliveryID, target domain.TargetInfo, result domain.DeliveryResult) (context.Context, domain.CompletedProbe) {
	logger := o.loggerFor(deliveryID, target)
	if logger.Enabled(ctx, slog.LevelInfo) {
		attrs := []slog.Attr{
			slog.String("state", string(result.State)),
		}
		if result.Message != "" {
			attrs = append(attrs, slog.String("message", result.Message))
		}
		logger.LogAttrs(ctx, slog.LevelInfo, "delivery completed", attrs...)
	}
	return ctx, &completedProbe{
		logger:    logger,
		ctx:       ctx,
		startTime: time.Now(),
	}
}

func (o *DeliveryObserver) loggerFor(deliveryID domain.DeliveryID, target domain.TargetInfo) *slog.Logger {
	return o.logger.With(
		slog.String("delivery_id", string(deliveryID)),
		slog.String("target_id", string(target.ID)),
		slog.String("target_type", string(target.Type)),
	)
}

type eventEmittedProbe struct {
	domain.NoOpEventEmittedProbe
	logger    *slog.Logger
	ctx       context.Context
	startTime time.Time
	err       error
}

func (p *eventEmittedProbe) Error(err error) { p.err = err }

func (p *eventEmittedProbe) End() {
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "delivery emit failed",
			slog.Duration("duration", time.Since(p.startTime)),
			slog.String("error", p.err.Error()),
		)
	}
}

type completedProbe struct {
	domain.NoOpCompletedProbe
	logger    *slog.Logger
	ctx       context.Context
	startTime time.Time
	err       error
}

func (p *completedProbe) Error(err error) { p.err = err }

func (p *completedProbe) End() {
	duration := time.Since(p.startTime)
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "delivery completion failed",
			slog.Duration("duration", duration),
			slog.String("error", p.err.Error()),
		)
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "delivery completion done",
		slog.Duration("duration", duration),
	)
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
