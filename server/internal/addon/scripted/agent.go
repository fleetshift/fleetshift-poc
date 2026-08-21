package scripted

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Agent implements domain.DeliveryAgent for declaratively controlled
// test delivery. Behavior is derived entirely from the delivered
// resource spec; there is no imperative control API.
type Agent struct {
	reporter  domain.DeliveryReporter
	inventory domain.InventoryReporter
	codec     *Codec
	planner   *Planner
	appCtx    context.Context
	log       *slog.Logger

	// sleep is the cancellable sleep function used by the retry loop.
	// Tests inject a fake; production uses sleepCancellable.
	sleep func(ctx context.Context, d time.Duration) error

	mu      sync.Mutex
	slots   map[slotKey]*dispatchSlot
	wg      sync.WaitGroup
	closing bool
}

// slotKey identifies one in-flight operation for a managed resource.
type slotKey struct {
	uid        domain.ExtensionResourceUID
	generation int64
	operation  Operation
}

// dispatchSlot tracks the active delivery for one (UID, generation,
// operation) triple.
type dispatchSlot struct {
	deliveryID domain.DeliveryID
}

// AgentOption is a functional option for NewAgent.
type AgentOption func(*Agent)

// WithSleep injects a custom sleep function for testing. The default is sleepCancellable.
func WithSleep(fn func(context.Context, time.Duration) error) AgentOption {
	return func(a *Agent) { a.sleep = fn }
}

// NewAgent creates a scripted delivery agent. The codec must be
// pre-compiled (NewCodec). The appCtx is the application-owned context
// used to cancel in-flight work during shutdown.
func NewAgent(
	reporter domain.DeliveryReporter,
	inventory domain.InventoryReporter,
	codec *Codec,
	planner *Planner,
	appCtx context.Context,
	log *slog.Logger,
	opts ...AgentOption,
) *Agent {
	a := &Agent{
		reporter:  reporter,
		inventory: inventory,
		codec:     codec,
		planner:   planner,
		appCtx:    appCtx,
		log:       log,
		slots:     make(map[slotKey]*dispatchSlot),
	}
	// Set default sleep function (must be done before applying options).
	a.sleep = a.sleepCancellableFunc
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Deliver implements domain.DeliveryAgent.
func (a *Agent) Deliver(
	ctx context.Context,
	target domain.TargetInfo,
	deliveryID domain.DeliveryID,
	manifests []domain.Manifest,
	_ domain.DeliveryAuth,
	_ *domain.Attestation,
	generation domain.Generation,
) error {
	return a.dispatch(ctx, target, deliveryID, manifests, generation, OperationDeliver)
}

// Remove implements domain.DeliveryAgent.
func (a *Agent) Remove(
	ctx context.Context,
	target domain.TargetInfo,
	deliveryID domain.DeliveryID,
	manifests []domain.Manifest,
	_ domain.DeliveryAuth,
	_ *domain.Attestation,
	generation domain.Generation,
) error {
	return a.dispatch(ctx, target, deliveryID, manifests, generation, OperationRemove)
}

// Close marks the agent as closing and waits for in-flight work to
// finish. Cancellation of in-flight work is driven by appCtx, which is
// owned by the caller (bootstrap shutdown).
func (a *Agent) Close(_ context.Context) error {
	a.mu.Lock()
	a.closing = true
	a.mu.Unlock()
	// appCtx cancellation is owned by the caller (bootstrap shutdown);
	// we just wait for all in-flight work to join.
	a.wg.Wait()
	return nil
}

func (a *Agent) dispatch(
	ctx context.Context,
	target domain.TargetInfo,
	deliveryID domain.DeliveryID,
	manifests []domain.Manifest,
	generation domain.Generation,
	operation Operation,
) error {
	// Validate target.
	if target.ID() != TargetID {
		return fmt.Errorf("scripted: unexpected target %q, want %q", target.ID(), TargetID)
	}

	// Require exactly one manifest of the managed type.
	if len(manifests) != 1 {
		return fmt.Errorf("scripted: expected 1 manifest, got %d", len(manifests))
	}
	m := manifests[0]
	if m.ManifestType != ManagedManifestType {
		return fmt.Errorf("scripted: unexpected manifest type %q, want %q", m.ManifestType, ManagedManifestType)
	}

	// Unwrap the managed resource envelope.
	envelope, err := domain.UnwrapManagedResourceSpec(m.Raw)
	if err != nil {
		return fmt.Errorf("scripted: unwrap manifest: %w", err)
	}

	// Decode and validate the spec.
	spec, err := a.codec.Decode(envelope.Spec)
	if err != nil {
		return fmt.Errorf("%w: %v", domain.ErrInvalidArgument, err)
	}

	uid := envelope.UID
	gen := int64(generation)

	// Select the operation-specific behavior.
	var opSpec OperationSpec
	switch operation {
	case OperationDeliver:
		opSpec = spec.Delivery
	case OperationRemove:
		opSpec = spec.Removal
	default:
		return fmt.Errorf("scripted: unsupported operation %v", operation)
	}

	// Arbitrate by (UID, generation, operation). The mu lock also
	// gates the closing check and wg reservation to prevent a race
	// where Close sees wg at zero before we launch the goroutine.
	key := slotKey{uid: uid, generation: gen, operation: operation}
	a.mu.Lock()
	if a.closing {
		a.mu.Unlock()
		return fmt.Errorf("scripted: agent is closing")
	}
	existing, hasSlot := a.slots[key]
	if hasSlot {
		if existing.deliveryID == deliveryID {
			// Exact duplicate -- the ack already succeeded (the slot
			// wouldn't exist otherwise). Return nil immediately; the
			// single in-flight completion goroutine will report the
			// result via ReportResult.
			a.mu.Unlock()
			return nil
		}
		// Different delivery ID for the same (UID, gen, op) -- conflict.
		a.mu.Unlock()
		return domain.ErrInvalidArgument
	}
	// Reserve a WaitGroup slot under the lock so Close cannot
	// observe wg at zero between slot creation and goroutine launch.
	a.wg.Add(1)
	slot := &dispatchSlot{deliveryID: deliveryID}
	a.slots[key] = slot
	a.mu.Unlock()

	// Plan acknowledgement.
	ackKey := AttemptKey{
		InstanceKey: managedResourceInstanceKey(uid),
		Generation:  gen,
		Operation:   operation,
		Phase:       PhaseAcknowledgement,
	}
	ackDecision := a.planner.Decide(opSpec.Acknowledgement, ackKey)

	// Wait for ack latency.
	if err := a.sleepCancellable(ctx, ackDecision.Latency); err != nil {
		a.releaseSlot(key)
		a.wg.Done()
		return fmt.Errorf("scripted: ack wait cancelled: %w", err)
	}

	// Apply ack outcome.
	if ackDecision.Outcome == OutcomeFailure {
		a.releaseSlot(key)
		a.wg.Done()
		return fmt.Errorf("scripted %s acknowledgement failed", operation)
	}

	// Report acknowledgement progress event.
	ackMsg := fmt.Sprintf("scripted %s accepted", operation)
	if err := a.reporter.ReportEvent(ctx, deliveryID, generation, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   ackMsg,
	}); err != nil {
		a.releaseSlot(key)
		a.wg.Done()
		return fmt.Errorf("scripted: report ack event: %w", err)
	}

	// Start async completion. The wg slot was reserved above.
	go a.runCompletion(deliveryID, generation, uid, envelope.Name, gen, operation, opSpec, spec.Inventory, key)

	return nil
}

func (a *Agent) runCompletion(
	deliveryID domain.DeliveryID,
	generation domain.Generation,
	uid domain.ExtensionResourceUID,
	name domain.ResourceName,
	gen int64,
	operation Operation,
	opSpec OperationSpec,
	inv InventoryProjection,
	key slotKey,
) {
	defer a.wg.Done()
	defer a.releaseSlot(key)

	ctx := a.appCtx

	// Plan completion.
	compKey := AttemptKey{
		InstanceKey: managedResourceInstanceKey(uid),
		Generation:  gen,
		Operation:   operation,
		Phase:       PhaseCompletion,
	}
	compDecision := a.planner.Decide(opSpec.Completion, compKey)

	// Wait for completion latency.
	if err := a.sleepCancellable(ctx, compDecision.Latency); err != nil {
		return // cancelled -- agent is shutting down
	}

	if compDecision.Outcome == OutcomeFailure {
		failMsg := fmt.Sprintf("scripted %s completion failed", operation)
		if err := a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: failMsg,
		}); err != nil {
			a.log.Warn("scripted: report completion failure", "deliveryID", deliveryID, "error", err)
		}
		return
	}

	// On successful delivery, project inventory before reporting
	// delivered state (with retries on transient errors).
	if operation == OperationDeliver {
		if err := a.projectInventoryWithRetry(ctx, name, inv); err != nil {
			a.log.Warn("scripted: inventory projection failed after retries", "name", name, "error", err)
		}
	}

	// On successful removal, clear planner cursor state for this
	// resource so a same-name recreation starts fresh.
	if operation == OperationRemove {
		a.planner.Reset(managedResourceInstanceKey(uid))
	}

	if err := a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
		State: domain.DeliveryStateDelivered,
	}); err != nil {
		a.log.Warn("scripted: report completion success", "deliveryID", deliveryID, "error", err)
	}
}

// projectInventoryWithRetry idempotently replaces the managed resource's labels
// and observation, retrying transient errors with a deterministic backoff schedule.
// Empty projection values clear any prior inventory.
func (a *Agent) projectInventoryWithRetry(
	ctx context.Context,
	name domain.ResourceName,
	inv InventoryProjection,
) error {
	report := domain.InventoryDeltaReport{
		ResourceType:  ResourceType,
		Name:          name,
		ReplaceLabels: inv.Labels,
	}
	if inv.Observation != nil {
		obs := json.RawMessage(inv.Observation)
		report.Observation = &obs
	}

	var lastErr error
	for attempt := 0; ; attempt++ {
		err := a.inventory.ApplyDeltaBatch(ctx, domain.InventoryDeltaBatch{
			Reports: []domain.InventoryDeltaReport{report},
		})
		if err == nil {
			return nil
		}

		// Check for permanent errors.
		if isPermanentInventoryError(err) {
			return err
		}

		lastErr = err

		// Wait before retry.
		delay := inventoryRetryDelay(attempt)
		if err := a.sleep(ctx, delay); err != nil {
			return lastErr // context cancelled during wait
		}
	}
}

func (a *Agent) releaseSlot(key slotKey) {
	a.mu.Lock()
	delete(a.slots, key)
	a.mu.Unlock()
}

func (a *Agent) sleepCancellable(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-a.appCtx.Done():
		return a.appCtx.Err()
	}
}

func managedResourceInstanceKey(uid domain.ExtensionResourceUID) string {
	return "managed-resource:" + uid.String()
}

// inventoryRetrySchedule is the fixed backoff schedule: 100ms, 200ms, 400ms, 800ms, 1.6s.
// After exhaustion, every subsequent attempt uses inventoryRetryMaxDelay (2s).
var inventoryRetrySchedule = []time.Duration{
	100 * time.Millisecond,
	200 * time.Millisecond,
	400 * time.Millisecond,
	800 * time.Millisecond,
	1600 * time.Millisecond,
}

const inventoryRetryMaxDelay = 2 * time.Second

// inventoryRetryDelay returns the backoff duration for the given attempt number.
func inventoryRetryDelay(attempt int) time.Duration {
	if attempt < len(inventoryRetrySchedule) {
		return inventoryRetrySchedule[attempt]
	}
	return inventoryRetryMaxDelay
}

// isPermanentInventoryError classifies whether an error should stop retries.
// Terminal errors (domain.IsTerminal), semantic errors (domain.ErrInvalidArgument,
// domain.ErrUnimplemented, domain.ErrNotFound) are permanent.
// All other errors are transient/retryable.
func isPermanentInventoryError(err error) bool {
	if domain.IsTerminal(err) {
		return true
	}
	if errors.Is(err, domain.ErrInvalidArgument) {
		return true
	}
	if errors.Is(err, domain.ErrUnimplemented) {
		return true
	}
	if errors.Is(err, domain.ErrNotFound) {
		return true
	}
	return false
}

// sleepCancellableFunc is a wrapper around sleepCancellable that can be assigned as the sleep function.
func (a *Agent) sleepCancellableFunc(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-a.appCtx.Done():
		return a.appCtx.Err()
	}
}
