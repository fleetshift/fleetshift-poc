// Package observability provides slog-based implementations of the
// domain observer interfaces for structured logging of deployment and
// delivery lifecycle events.
package observability

import (
	"context"
	"log/slog"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// FulfillmentObserver is a [domain.FulfillmentObserver] that logs
// fulfillment orchestration lifecycle events via [slog].
type FulfillmentObserver struct {
	domain.NoOpFulfillmentObserver
	logger *slog.Logger
}

// NewFulfillmentObserver returns a FulfillmentObserver that logs to logger.
func NewFulfillmentObserver(logger *slog.Logger) *FulfillmentObserver {
	return &FulfillmentObserver{logger: logger.With("component", "deployment")}
}

func (o *FulfillmentObserver) RunStarted(ctx context.Context, fulfillmentID domain.FulfillmentID) (context.Context, domain.FulfillmentRunProbe) {
	logger := o.logger.With(slog.String("fulfillment_id", string(fulfillmentID)))
	if logger.Enabled(ctx, slog.LevelInfo) {
		logger.LogAttrs(ctx, slog.LevelInfo, "deployment run started")
	}
	return ctx, &fulfillmentRunProbe{
		logger:         logger,
		ctx:            ctx,
		startTime:      time.Now(),
		fulfillmentID: fulfillmentID,
	}
}

type fulfillmentRunProbe struct {
	domain.NoOpFulfillmentRunProbe
	logger        *slog.Logger
	ctx           context.Context
	startTime     time.Time
	fulfillmentID domain.FulfillmentID
	err           error
}

func (p *fulfillmentRunProbe) EventReceived(event domain.FulfillmentEvent) {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "deployment event received",
		slog.String("event_kind", classifyFulfillmentEvent(event)),
	)
}

func (p *fulfillmentRunProbe) StateChanged(state domain.FulfillmentState) {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "deployment state changed",
		slog.String("state", string(state)),
	)
}

func (p *fulfillmentRunProbe) ManifestsFiltered(target domain.TargetInfo, total, accepted int) {
	if accepted == 0 {
		p.logger.LogAttrs(p.ctx, slog.LevelWarn, "all manifests filtered for target",
			slog.String("target_id", string(target.ID)),
			slog.String("target_type", string(target.Type)),
			slog.Int("total", total),
		)
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "manifests filtered for target",
		slog.String("target_id", string(target.ID)),
		slog.String("target_type", string(target.Type)),
		slog.Int("total", total),
		slog.Int("accepted", accepted),
	)
}

func (p *fulfillmentRunProbe) DeliveryOutputsProcessed(targets []domain.ProvisionedTarget, secrets int) {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	targetIDs := make([]string, len(targets))
	for i, t := range targets {
		targetIDs[i] = string(t.ID)
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "delivery outputs processed",
		slog.Int("targets_registered", len(targets)),
		slog.Int("secrets_stored", secrets),
		slog.Any("target_ids", targetIDs),
	)
}

func (p *fulfillmentRunProbe) Error(err error) {
	p.err = err
}

func (p *fulfillmentRunProbe) End() {
	duration := time.Since(p.startTime)
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "deployment run failed",
			slog.Duration("duration", duration),
			slog.String("error", p.err.Error()),
		)
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "deployment run completed",
		slog.Duration("duration", duration),
	)
}

func classifyFulfillmentEvent(event domain.FulfillmentEvent) string {
	switch {
	case event.DeliveryCompleted != nil:
		return "delivery_completed"
	default:
		return "unknown"
	}
}
