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

// DeploymentObserver is a [domain.DeploymentObserver] that logs
// deployment orchestration lifecycle events via [slog].
type DeploymentObserver struct {
	domain.NoOpDeploymentObserver
	logger *slog.Logger
}

// NewDeploymentObserver returns a DeploymentObserver that logs to logger.
func NewDeploymentObserver(logger *slog.Logger) *DeploymentObserver {
	return &DeploymentObserver{logger: logger.With("component", "deployment")}
}

func (o *DeploymentObserver) RunStarted(ctx context.Context, deploymentID domain.DeploymentID) (context.Context, domain.DeploymentRunProbe) {
	logger := o.logger.With(slog.String("deployment_id", string(deploymentID)))
	if logger.Enabled(ctx, slog.LevelInfo) {
		logger.LogAttrs(ctx, slog.LevelInfo, "deployment run started")
	}
	return ctx, &deploymentRunProbe{
		logger:       logger,
		ctx:          ctx,
		startTime:    time.Now(),
		deploymentID: deploymentID,
	}
}

type deploymentRunProbe struct {
	domain.NoOpDeploymentRunProbe
	logger       *slog.Logger
	ctx          context.Context
	startTime    time.Time
	deploymentID domain.DeploymentID
	err          error
}

func (p *deploymentRunProbe) EventReceived(event domain.DeploymentEvent) {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "deployment event received",
		slog.String("event_kind", classifyDeploymentEvent(event)),
	)
}

func (p *deploymentRunProbe) StateChanged(state domain.DeploymentState) {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "deployment state changed",
		slog.String("state", string(state)),
	)
}

func (p *deploymentRunProbe) ManifestsFiltered(target domain.TargetInfo, total, accepted int) {
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

func (p *deploymentRunProbe) DeliveryOutputsProcessed(targets []domain.ProvisionedTarget, secrets int) {
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

func (p *deploymentRunProbe) Error(err error) {
	p.err = err
}

func (p *deploymentRunProbe) End() {
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

func classifyDeploymentEvent(event domain.DeploymentEvent) string {
	switch {
	case event.Delete:
		return "delete"
	case event.SpecChanged:
		return "spec_changed"
	case event.ManifestInvalidated:
		return "manifest_invalidated"
	case event.PoolChange != nil:
		return "pool_change"
	case event.DeliveryCompleted != nil:
		return "delivery_completed"
	default:
		return "unknown"
	}
}
