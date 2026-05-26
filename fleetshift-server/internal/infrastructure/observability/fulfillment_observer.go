// Package observability provides slog-based implementations of the
// domain observer interfaces for structured logging of fulfillment
// orchestration and delivery lifecycle events.
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
	return &FulfillmentObserver{logger: logger.With("component", "fulfillment")}
}

// ---------------------------------------------------------------------------
// RunStarted → fulfillmentRunProbe (workflow-level)
// ---------------------------------------------------------------------------

func (o *FulfillmentObserver) RunStarted(ctx context.Context, fulfillmentID domain.FulfillmentID) (context.Context, domain.FulfillmentRunProbe) {
	logger := o.logger.With(slog.String("fulfillment_id", string(fulfillmentID)))
	if logger.Enabled(ctx, slog.LevelInfo) {
		logger.LogAttrs(ctx, slog.LevelInfo, "fulfillment run started")
	}
	return ctx, &fulfillmentRunProbe{
		logger:        logger,
		ctx:           ctx,
		startTime:     time.Now(),
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

func (p *fulfillmentRunProbe) DispatchCycleStarted(deliveryCount int, expectedGen domain.Generation) domain.DispatchCycleProbe {
	if p.logger.Enabled(p.ctx, slog.LevelDebug) {
		p.logger.LogAttrs(p.ctx, slog.LevelDebug, "dispatch cycle started",
			slog.Int("delivery_count", deliveryCount),
			slog.Int64("expected_generation", int64(expectedGen)),
		)
	}
	return &dispatchCycleProbe{
		logger:      p.logger,
		ctx:         p.ctx,
		startTime:   time.Now(),
		expectedGen: expectedGen,
	}
}

func (p *fulfillmentRunProbe) StateChanged(state domain.FulfillmentState) {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "fulfillment state changed",
		slog.String("state", string(state)),
	)
}

func (p *fulfillmentRunProbe) RolloutStepStarted(stepIndex, stepCount int, isDeliver bool) {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	kind := "remove"
	if isDeliver {
		kind = "deliver"
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "rollout step started",
		slog.Int("step_index", stepIndex),
		slog.Int("step_count", stepCount),
		slog.String("step_kind", kind),
	)
}

func (p *fulfillmentRunProbe) GenerationAdvancedMidRollout(startGen, currentGen domain.Generation) {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "generation advanced mid-rollout",
		slog.Int64("start_generation", int64(startGen)),
		slog.Int64("current_generation", int64(currentGen)),
	)
}

func (p *fulfillmentRunProbe) ReconciliationRestarting(generation domain.Generation) {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "reconciliation restarting",
		slog.Int64("reconciled_generation", int64(generation)),
	)
}

func (p *fulfillmentRunProbe) ContinueAsNewTriggered() {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "continue-as-new triggered")
}

func (p *fulfillmentRunProbe) DeleteStarted(targetCount int) {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "delete pipeline started",
		slog.Int("target_count", targetCount),
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

func (p *fulfillmentRunProbe) Error(err error) {
	p.err = err
}

func (p *fulfillmentRunProbe) End() {
	duration := time.Since(p.startTime)
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "fulfillment run failed",
			slog.Duration("duration", duration),
			slog.String("error", p.err.Error()),
		)
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "fulfillment run completed",
		slog.Duration("duration", duration),
	)
}

// ---------------------------------------------------------------------------
// DispatchCycleProbe (workflow-level, spawned by RunProbe)
// ---------------------------------------------------------------------------

type dispatchCycleProbe struct {
	domain.NoOpDispatchCycleProbe
	logger      *slog.Logger
	ctx         context.Context
	startTime   time.Time
	expectedGen domain.Generation
	err         error
}

func (p *dispatchCycleProbe) Dispatched(deliveryID domain.DeliveryID, isRedispatch bool) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "delivery dispatched",
		slog.String("delivery_id", string(deliveryID)),
		slog.Bool("is_redispatch", isRedispatch),
	)
}

func (p *dispatchCycleProbe) Skipped(deliveryID domain.DeliveryID) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "delivery skipped",
		slog.String("delivery_id", string(deliveryID)),
	)
}

func (p *dispatchCycleProbe) AckReceived(deliveryID domain.DeliveryID) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "delivery ack received",
		slog.String("delivery_id", string(deliveryID)),
	)
}

func (p *dispatchCycleProbe) AckTimeout(unackedCount int) {
	p.logger.LogAttrs(p.ctx, slog.LevelWarn, "delivery ack timeout",
		slog.Int("unacked_count", unackedCount),
	)
}

func (p *dispatchCycleProbe) Completed(deliveryID domain.DeliveryID, state domain.DeliveryState) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "delivery completed",
		slog.String("delivery_id", string(deliveryID)),
		slog.String("state", string(state)),
	)
}

func (p *dispatchCycleProbe) StaleEventDiscarded(event domain.FulfillmentEvent, expectedGen domain.Generation) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "stale event discarded",
		slog.String("event_kind", classifyFulfillmentEvent(event)),
		slog.Int64("expected_generation", int64(expectedGen)),
	)
}

func (p *dispatchCycleProbe) Error(err error) {
	p.err = err
}

func (p *dispatchCycleProbe) End() {
	duration := time.Since(p.startTime)
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "dispatch cycle failed",
			slog.Duration("duration", duration),
			slog.String("error", p.err.Error()),
		)
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "dispatch cycle completed",
		slog.Duration("duration", duration),
	)
}

// ---------------------------------------------------------------------------
// AcquireLockStarted → acquireLockProbe (activity-level)
// ---------------------------------------------------------------------------

func (o *FulfillmentObserver) AcquireLockStarted(ctx context.Context, fulfillmentID domain.FulfillmentID) (context.Context, domain.AcquireLockProbe) {
	logger := o.logger.With(slog.String("fulfillment_id", string(fulfillmentID)))
	if logger.Enabled(ctx, slog.LevelDebug) {
		logger.LogAttrs(ctx, slog.LevelDebug, "acquire lock started")
	}
	return ctx, &acquireLockProbe{
		logger:    logger,
		ctx:       ctx,
		startTime: time.Now(),
	}
}

type acquireLockProbe struct {
	domain.NoOpAcquireLockProbe
	logger    *slog.Logger
	ctx       context.Context
	startTime time.Time
	err       error
}

func (p *acquireLockProbe) LockAcquired(newlyAcquired bool) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "orchestration lock acquired",
		slog.Bool("newly_acquired", newlyAcquired),
	)
}

func (p *acquireLockProbe) PoolLoaded(targetCount int) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "target pool loaded",
		slog.Int("target_count", targetCount),
	)
}

func (p *acquireLockProbe) EvidenceResolved(hasEvidence bool) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "attestation evidence resolved",
		slog.Bool("has_evidence", hasEvidence),
	)
}

func (p *acquireLockProbe) Error(err error) {
	p.err = err
}

func (p *acquireLockProbe) End() {
	duration := time.Since(p.startTime)
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "acquire lock failed",
			slog.Duration("duration", duration),
			slog.String("error", p.err.Error()),
		)
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "acquire lock completed",
		slog.Duration("duration", duration),
	)
}

// ---------------------------------------------------------------------------
// DeliverStarted → deliverProbe (activity-level)
// ---------------------------------------------------------------------------

func (o *FulfillmentObserver) DeliverStarted(ctx context.Context, input domain.DeliverInput) (context.Context, domain.DeliverProbe) {
	logger := o.logger.With(
		slog.String("fulfillment_id", string(input.FulfillmentID)),
		slog.String("delivery_id", string(input.DeliveryID)),
		slog.String("target_id", string(input.Target.ID)),
	)
	if logger.Enabled(ctx, slog.LevelDebug) {
		logger.LogAttrs(ctx, slog.LevelDebug, "deliver to target started")
	}
	return ctx, &deliverProbe{
		logger:    logger,
		ctx:       ctx,
		startTime: time.Now(),
	}
}

type deliverProbe struct {
	domain.NoOpDeliverProbe
	logger    *slog.Logger
	ctx       context.Context
	startTime time.Time
	err       error
}

func (p *deliverProbe) NewDelivery() {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "new delivery created")
}

func (p *deliverProbe) Redispatched(previousGen domain.Generation) {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "delivery redispatched",
		slog.Int64("previous_generation", int64(previousGen)),
	)
}

func (p *deliverProbe) Retried() {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "delivery retried")
}

func (p *deliverProbe) SkippedAlreadyAcked() {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "delivery skipped: already acked")
}

func (p *deliverProbe) Error(err error) {
	p.err = err
}

func (p *deliverProbe) End() {
	duration := time.Since(p.startTime)
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "deliver to target failed",
			slog.Duration("duration", duration),
			slog.String("error", p.err.Error()),
		)
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "deliver to target completed",
		slog.Duration("duration", duration),
	)
}

// ---------------------------------------------------------------------------
// RemoveStarted → removeProbe (activity-level)
// ---------------------------------------------------------------------------

func (o *FulfillmentObserver) RemoveStarted(ctx context.Context, input domain.RemoveInput) (context.Context, domain.RemoveProbe) {
	logger := o.logger.With(
		slog.String("fulfillment_id", string(input.FulfillmentID)),
		slog.String("delivery_id", string(input.DeliveryID)),
		slog.String("target_id", string(input.Target.ID)),
	)
	if logger.Enabled(ctx, slog.LevelDebug) {
		logger.LogAttrs(ctx, slog.LevelDebug, "remove from target started")
	}
	return ctx, &removeProbe{
		logger:    logger,
		ctx:       ctx,
		startTime: time.Now(),
	}
}

type removeProbe struct {
	domain.NoOpRemoveProbe
	logger    *slog.Logger
	ctx       context.Context
	startTime time.Time
	err       error
}

func (p *removeProbe) TargetNotFound() {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "remove skipped: no delivery record")
}

func (p *removeProbe) Withdrawn() {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "delivery withdrawn")
}

func (p *removeProbe) AlreadyPending() {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "remove skipped: already past pending")
}

func (p *removeProbe) Error(err error) {
	p.err = err
}

func (p *removeProbe) End() {
	duration := time.Since(p.startTime)
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "remove from target failed",
			slog.Duration("duration", duration),
			slog.String("error", p.err.Error()),
		)
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "remove from target completed",
		slog.Duration("duration", duration),
	)
}

// ---------------------------------------------------------------------------
// PersistReconciliationStarted → persistReconciliationProbe (activity-level)
// ---------------------------------------------------------------------------

func (o *FulfillmentObserver) PersistReconciliationStarted(ctx context.Context, fulfillmentID domain.FulfillmentID) (context.Context, domain.PersistReconciliationProbe) {
	logger := o.logger.With(slog.String("fulfillment_id", string(fulfillmentID)))
	if logger.Enabled(ctx, slog.LevelDebug) {
		logger.LogAttrs(ctx, slog.LevelDebug, "persist reconciliation started")
	}
	return ctx, &persistReconciliationProbe{
		logger:    logger,
		ctx:       ctx,
		startTime: time.Now(),
	}
}

type persistReconciliationProbe struct {
	domain.NoOpPersistReconciliationProbe
	logger    *slog.Logger
	ctx       context.Context
	startTime time.Time
	err       error
}

func (p *persistReconciliationProbe) Persisted(state domain.FulfillmentState, needsRestart bool) {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "reconciliation persisted",
		slog.String("state", string(state)),
		slog.Bool("needs_restart", needsRestart),
	)
}

func (p *persistReconciliationProbe) DeleteCleanupSignaled() {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "delete cleanup signaled")
}

func (p *persistReconciliationProbe) Error(err error) {
	p.err = err
}

func (p *persistReconciliationProbe) End() {
	duration := time.Since(p.startTime)
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "persist reconciliation failed",
			slog.Duration("duration", duration),
			slog.String("error", p.err.Error()),
		)
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "persist reconciliation completed",
		slog.Duration("duration", duration),
	)
}

// ---------------------------------------------------------------------------
// ProcessOutputsStarted → processOutputsProbe (activity-level)
// ---------------------------------------------------------------------------

func (o *FulfillmentObserver) ProcessOutputsStarted(ctx context.Context) (context.Context, domain.ProcessOutputsProbe) {
	logger := o.logger
	if logger.Enabled(ctx, slog.LevelDebug) {
		logger.LogAttrs(ctx, slog.LevelDebug, "process delivery outputs started")
	}
	return ctx, &processOutputsProbe{
		logger:    logger,
		ctx:       ctx,
		startTime: time.Now(),
	}
}

type processOutputsProbe struct {
	domain.NoOpProcessOutputsProbe
	logger    *slog.Logger
	ctx       context.Context
	startTime time.Time
	err       error
}

func (p *processOutputsProbe) SecretsStored(count int) {
	if count == 0 || !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "secrets stored",
		slog.Int("count", count),
	)
}

func (p *processOutputsProbe) TargetsRegistered(count int) {
	if count == 0 || !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "delivery outputs processed",
		slog.Int("targets_registered", count),
	)
}

func (p *processOutputsProbe) Skipped() {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "delivery outputs skipped: no outputs")
}

func (p *processOutputsProbe) Error(err error) {
	p.err = err
}

func (p *processOutputsProbe) End() {
	duration := time.Since(p.startTime)
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "process delivery outputs failed",
			slog.Duration("duration", duration),
			slog.String("error", p.err.Error()),
		)
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "process delivery outputs completed",
		slog.Duration("duration", duration),
	)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func classifyFulfillmentEvent(event domain.FulfillmentEvent) string {
	switch {
	case event.DeliveryAcked != nil:
		return "delivery_acked"
	case event.DeliveryCompleted != nil:
		return "delivery_completed"
	default:
		return "unknown"
	}
}
