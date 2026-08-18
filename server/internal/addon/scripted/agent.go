package scripted

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
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

	mu      sync.Mutex
	slots   map[slotKey]*dispatchSlot
	wg      sync.WaitGroup
	closing atomic.Bool
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
	// completionDone is closed when async completion finishes or is
	// skipped. Nil when no completion has started.
	completionDone chan struct{}
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
) *Agent {
	return &Agent{
		reporter:  reporter,
		inventory: inventory,
		codec:     codec,
		planner:   planner,
		appCtx:    appCtx,
		slots:     make(map[slotKey]*dispatchSlot),
	}
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

// Close cancels all in-flight work and waits for it to finish.
func (a *Agent) Close(_ context.Context) error {
	a.closing.Store(true)
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
	if a.closing.Load() {
		return fmt.Errorf("scripted: agent is closing")
	}

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
	}

	// Arbitrate by (UID, generation, operation).
	key := slotKey{uid: uid, generation: gen, operation: operation}
	a.mu.Lock()
	existing, hasSlot := a.slots[key]
	if hasSlot {
		if existing.deliveryID == deliveryID {
			// Exact duplicate -- join in-flight result. The caller
			// will receive the same ack outcome.
			a.mu.Unlock()
			// For duplicates, we re-plan with the same key so the
			// cursor is already past this attempt.
			return nil
		}
		// Different delivery ID for the same (UID, gen, op) -- conflict.
		a.mu.Unlock()
		return domain.ErrInvalidArgument
	}
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
		return fmt.Errorf("scripted: ack wait cancelled: %w", err)
	}

	// Apply ack outcome.
	if ackDecision.Outcome == OutcomeFailure {
		a.releaseSlot(key)
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
		return fmt.Errorf("scripted: report ack event: %w", err)
	}

	// Start async completion.
	a.wg.Add(1)
	go a.runCompletion(deliveryID, generation, uid, gen, operation, opSpec, spec.Inventory, key)

	return nil
}

func (a *Agent) runCompletion(
	deliveryID domain.DeliveryID,
	generation domain.Generation,
	uid domain.ExtensionResourceUID,
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
		_ = a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: failMsg,
		})
		return
	}

	// On successful delivery, project inventory before reporting
	// delivered state.
	if operation == OperationDeliver {
		a.projectInventory(ctx, uid, generation, inv)
	}

	_ = a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
		State: domain.DeliveryStateDelivered,
	})
}

// projectInventory idempotently replaces the managed resource's labels
// and observation. Empty projection values clear any prior inventory.
func (a *Agent) projectInventory(
	ctx context.Context,
	uid domain.ExtensionResourceUID,
	generation domain.Generation,
	inv InventoryProjection,
) {
	report := domain.InventoryDeltaReport{
		ResourceType:  ResourceType,
		Name:          domain.ResourceName(uid.String()),
		ReplaceLabels: inv.Labels,
	}
	if inv.Observation != nil {
		obs := json.RawMessage(inv.Observation)
		report.Observation = &obs
	}
	_ = a.inventory.ApplyDeltaBatch(ctx, domain.InventoryDeltaBatch{
		Reports: []domain.InventoryDeltaReport{report},
	})
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
