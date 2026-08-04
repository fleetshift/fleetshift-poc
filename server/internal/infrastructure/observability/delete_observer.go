package observability

import (
	"context"
	"log/slog"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// DeleteObserver is a [domain.DeleteObserver] that logs delete and
// delete-cleanup workflow lifecycle events via [slog].
type DeleteObserver struct {
	domain.NoOpDeleteObserver
	logger *slog.Logger
}

// NewDeleteObserver returns a DeleteObserver that logs to logger.
func NewDeleteObserver(logger *slog.Logger) *DeleteObserver {
	return &DeleteObserver{logger: logger.With("component", "delete")}
}

// ---------------------------------------------------------------------------
// DeleteDeploymentStarted → deleteDeploymentProbe (workflow-level)
// ---------------------------------------------------------------------------

func (o *DeleteObserver) DeleteDeploymentStarted(ctx context.Context, name domain.ResourceName) (context.Context, domain.DeleteProbe) {
	logger := o.logger.With(slog.String("deployment_name", string(name)))
	return ctx, &deleteProbe{
		logger:    logger,
		ctx:       ctx,
		startTime: time.Now(),
		kind:      "deployment",
	}
}

// ---------------------------------------------------------------------------
// DeleteManagedResourceStarted → deleteManagedResourceProbe (workflow-level)
// ---------------------------------------------------------------------------

func (o *DeleteObserver) DeleteManagedResourceStarted(ctx context.Context, resourceType domain.ResourceType, name domain.ResourceName) (context.Context, domain.DeleteProbe) {
	logger := o.logger.With(
		slog.String("resource_type", string(resourceType)),
		slog.String("resource_name", string(name)),
	)
	return ctx, &deleteProbe{
		logger:    logger,
		ctx:       ctx,
		startTime: time.Now(),
		kind:      "managed resource",
	}
}

type deleteProbe struct {
	domain.NoOpDeleteProbe
	logger    *slog.Logger
	ctx       context.Context
	startTime time.Time
	kind      string // "deployment" or "managed resource"
	err       error
}

func (p *deleteProbe) Mutated(fulfillmentID domain.FulfillmentID, generation domain.Generation) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "fulfillment transitioned to deleting",
		slog.String("fulfillment_id", string(fulfillmentID)),
		slog.Int64("generation", int64(generation)),
	)
}

func (p *deleteProbe) CleanupStarted() {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "delete cleanup workflow started")
}

func (p *deleteProbe) Error(err error) {
	p.err = err
}

func (p *deleteProbe) End() {
	duration := time.Since(p.startTime)
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "delete "+p.kind+" failed",
			slog.Duration("duration", duration),
			slog.String("error", p.err.Error()),
		)
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "delete "+p.kind+" completed",
		slog.Duration("duration", duration),
	)
}

// ---------------------------------------------------------------------------
// DeploymentCleanupStarted / ManagedResourceCleanupStarted → deleteCleanupProbe
// ---------------------------------------------------------------------------

func (o *DeleteObserver) DeploymentCleanupStarted(ctx context.Context, input domain.DeleteDeploymentCleanupInput) (context.Context, domain.DeleteCleanupProbe) {
	logger := o.logger.With(
		slog.String("deployment_name", string(input.Name)),
		slog.String("fulfillment_id", string(input.FulfillmentID)),
	)
	return ctx, &deleteCleanupProbe{
		logger:    logger,
		ctx:       ctx,
		startTime: time.Now(),
	}
}

func (o *DeleteObserver) ManagedResourceCleanupStarted(ctx context.Context, input domain.DeleteManagedResourceCleanupInput) (context.Context, domain.DeleteCleanupProbe) {
	logger := o.logger.With(
		slog.String("resource_type", string(input.ResourceType)),
		slog.String("resource_name", string(input.Name)),
		slog.String("fulfillment_id", string(input.FulfillmentID)),
	)
	return ctx, &deleteCleanupProbe{
		logger:    logger,
		ctx:       ctx,
		startTime: time.Now(),
	}
}

type deleteCleanupProbe struct {
	domain.NoOpDeleteCleanupProbe
	logger    *slog.Logger
	ctx       context.Context
	startTime time.Time
	err       error
}

func (p *deleteCleanupProbe) SignalReceived() {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "delete cleanup signal received")
}

func (p *deleteCleanupProbe) RowsDeleted() {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "delete cleanup rows deleted")
}

func (p *deleteCleanupProbe) Error(err error) {
	p.err = err
}

func (p *deleteCleanupProbe) End() {
	duration := time.Since(p.startTime)
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "delete cleanup failed",
			slog.Duration("duration", duration),
			slog.String("error", p.err.Error()),
		)
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "delete cleanup completed",
		slog.Duration("duration", duration),
	)
}

// ---------------------------------------------------------------------------
// MutateDeploymentStarted → mutateDeploymentProbe (activity-level)
// ---------------------------------------------------------------------------

func (o *DeleteObserver) MutateDeploymentStarted(ctx context.Context, name domain.ResourceName) (context.Context, domain.MutateDeploymentProbe) {
	logger := o.logger.With(slog.String("deployment_name", string(name)))
	return ctx, &mutateDeploymentProbe{
		logger:    logger,
		ctx:       ctx,
		startTime: time.Now(),
	}
}

type mutateDeploymentProbe struct {
	domain.NoOpMutateDeploymentProbe
	logger    *slog.Logger
	ctx       context.Context
	startTime time.Time
	err       error
}

func (p *mutateDeploymentProbe) Error(err error) {
	p.err = err
}

func (p *mutateDeploymentProbe) End() {
	duration := time.Since(p.startTime)
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "mutate deployment to deleting failed",
			slog.Duration("duration", duration),
			slog.String("error", p.err.Error()),
		)
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "mutate deployment to deleting completed",
		slog.Duration("duration", duration),
	)
}

// ---------------------------------------------------------------------------
// MutateManagedResourceStarted → mutateManagedResourceProbe (activity-level)
// ---------------------------------------------------------------------------

func (o *DeleteObserver) MutateManagedResourceStarted(ctx context.Context, resourceType domain.ResourceType, name domain.ResourceName) (context.Context, domain.MutateManagedResourceProbe) {
	logger := o.logger.With(
		slog.String("resource_type", string(resourceType)),
		slog.String("resource_name", string(name)),
	)
	return ctx, &mutateManagedResourceProbe{
		logger:    logger,
		ctx:       ctx,
		startTime: time.Now(),
	}
}

type mutateManagedResourceProbe struct {
	domain.NoOpMutateManagedResourceProbe
	logger    *slog.Logger
	ctx       context.Context
	startTime time.Time
	err       error
}

func (p *mutateManagedResourceProbe) Error(err error) {
	p.err = err
}

func (p *mutateManagedResourceProbe) End() {
	duration := time.Since(p.startTime)
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "mutate managed resource to deleting failed",
			slog.Duration("duration", duration),
			slog.String("error", p.err.Error()),
		)
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "mutate managed resource to deleting completed",
		slog.Duration("duration", duration),
	)
}
