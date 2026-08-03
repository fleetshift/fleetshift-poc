// Package fake provides a programmable [domain.DeliveryAgent].
// Behavior is driven through a typed [Controller]; delivery
// outcomes are reported asynchronously via the injected
// [domain.DeliveryReporter].
package fake

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// CallKind identifies a recorded agent method invocation.
type CallKind string

const (
	CallDeliver CallKind = "deliver"
	CallRemove  CallKind = "remove"
)

// Call is one ordered Deliver/Remove invocation observed by the agent.
type Call struct {
	Kind       CallKind
	TargetID   domain.TargetID
	DeliveryID domain.DeliveryID
	Generation domain.Generation
	At         time.Time
}

// ReportKind identifies a recorded reporter emission.
type ReportKind string

const (
	ReportResult ReportKind = "result"
	ReportEvent  ReportKind = "event"
)

// Report is one ordered ReportResult or ReportEvent emission.
type Report struct {
	Kind       ReportKind
	DeliveryID domain.DeliveryID
	Generation domain.Generation
	Result     *domain.DeliveryResult
	Event      *domain.DeliveryEvent
	At         time.Time
}

// Agent implements [domain.DeliveryAgent] with controllable outcomes.
// Construct via [New]; drive behavior through the returned [Controller].
type Agent struct {
	reporter domain.DeliveryReporter
	ctrl     *Controller
}

// New creates a fake delivery agent and its typed controller.
// Default behavior is queued success (Deliver/Remove report Delivered).
func New(reporter domain.DeliveryReporter) (*Agent, *Controller) {
	ctrl := &Controller{
		mode: modeSuccess,
		now:  time.Now,
	}
	a := &Agent{reporter: reporter, ctrl: ctrl}
	ctrl.agent = a
	return a, ctrl
}

// Controller is the typed delivery control surface. Delivery and
// inventory remain separate APIs; this controller never mutates inventory.
type Controller struct {
	mu sync.Mutex

	agent *Agent
	now   func() time.Time

	mode               mode
	transientRemaining int
	gate               *gateState

	calls   []Call
	reports []Report
}

// mode selects how the next Deliver/Remove calls behave.
type mode int

const (
	modeSuccess mode = iota
	modeGate
	modeTransient
)

// gateState holds wait/progress state while the controller is gated.
type gateState struct {
	releaseCh  chan struct{}
	pendingMsg []string
	inFlight   *inFlight
	released   bool
}

// inFlight identifies the gated Deliver or Remove currently waiting.
type inFlight struct {
	deliveryID domain.DeliveryID
	generation domain.Generation
}

// releaseGateLocked unblocks any goroutine waiting on the current gate
// before it is replaced or discarded. Callers must hold c.mu.
func (c *Controller) releaseGateLocked() {
	if c.gate != nil && !c.gate.released {
		c.gate.released = true
		close(c.gate.releaseCh)
	}
	c.gate = nil
}

// QueueSuccess sets the default success behavior for subsequent calls.
// This is the initial mode after [New]. Any in-flight gated Deliver or
// Remove is unblocked and completes successfully.
func (c *Controller) QueueSuccess() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.releaseGateLocked()
	c.mode = modeSuccess
	c.transientRemaining = 0
}

// Gate causes the next Deliver or Remove to wait until [Controller.Release].
// While gated, [Controller.ReportProgress] emits progress events for
// the in-flight call. Concurrent Gate while already gated fails.
func (c *Controller) Gate() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gate != nil && !c.gate.released {
		return errors.New("fake: already gated")
	}
	c.mode = modeGate
	c.gate = &gateState{releaseCh: make(chan struct{})}
	return nil
}

// ReportProgress emits a progress event for the gated in-flight
// Deliver or Remove, or queues the message until that call begins waiting.
func (c *Controller) ReportProgress(message string) error {
	c.mu.Lock()
	if c.gate == nil || c.gate.released {
		c.mu.Unlock()
		return errors.New("fake: not gated")
	}
	if c.gate.inFlight != nil {
		flight := *c.gate.inFlight
		timestamp := c.now()
		c.mu.Unlock()
		c.agent.emitEvent(flight.deliveryID, flight.generation, domain.DeliveryEvent{
			Timestamp: timestamp,
			Kind:      domain.DeliveryEventProgress,
			Message:   message,
		})
		return nil
	}
	c.gate.pendingMsg = append(c.gate.pendingMsg, message)
	c.mu.Unlock()
	return nil
}

// Release unblocks a gated Deliver or Remove so it can complete successfully.
func (c *Controller) Release() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gate == nil || c.gate.released {
		return errors.New("fake: not gated")
	}
	c.gate.released = true
	close(c.gate.releaseCh)
	return nil
}

// InjectTransientFailure causes the next n Deliver or Remove calls to
// return an agent error (the product may retry). After n failures,
// subsequent calls succeed. Any in-flight gated call is unblocked and
// completes successfully; the transient budget applies to later calls.
func (c *Controller) InjectTransientFailure(n int) error {
	if n < 1 {
		return fmt.Errorf("fake: transient failure count must be >= 1, got %d", n)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.releaseGateLocked()
	c.mode = modeTransient
	c.transientRemaining = n
	return nil
}

// Calls returns a snapshot of ordered Deliver/Remove invocations.
func (c *Controller) Calls() []Call {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Call, len(c.calls))
	copy(out, c.calls)
	return out
}

// Reports returns a snapshot of ordered result/event emissions.
func (c *Controller) Reports() []Report {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Report, len(c.reports))
	copy(out, c.reports)
	return out
}

// Deliver implements [domain.DeliveryAgent].
func (a *Agent) Deliver(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	return a.handle(ctx, CallDeliver, target, deliveryID, generation)
}

// Remove implements [domain.DeliveryAgent].
func (a *Agent) Remove(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	return a.handle(ctx, CallRemove, target, deliveryID, generation)
}

// handle records a Deliver/Remove call, applies the current controller
// mode (success, gate wait, or transient failure), and on success reports
// Delivered asynchronously via the injected reporter.
func (a *Agent) handle(ctx context.Context, kind CallKind, target domain.TargetInfo, deliveryID domain.DeliveryID, generation domain.Generation) error {
	c := a.ctrl
	c.mu.Lock()
	c.calls = append(c.calls, Call{
		Kind:       kind,
		TargetID:   target.ID(),
		DeliveryID: deliveryID,
		Generation: generation,
		At:         c.now(),
	})

	mode := c.mode
	var gate *gateState
	if mode == modeGate && c.gate != nil && !c.gate.released {
		gate = c.gate
	}
	transient := false
	if mode == modeTransient && c.transientRemaining > 0 {
		c.transientRemaining--
		transient = true
		if c.transientRemaining == 0 {
			c.mode = modeSuccess
		}
	}

	var pending []domain.DeliveryEvent
	var releaseCh <-chan struct{}
	if gate != nil {
		gate.inFlight = &inFlight{deliveryID: deliveryID, generation: generation}
		pending = make([]domain.DeliveryEvent, 0, len(gate.pendingMsg))
		for _, message := range gate.pendingMsg {
			pending = append(pending, domain.DeliveryEvent{
				Timestamp: c.now(),
				Kind:      domain.DeliveryEventProgress,
				Message:   message,
			})
		}
		gate.pendingMsg = nil
		releaseCh = gate.releaseCh
	}
	reporter := a.reporter
	c.mu.Unlock()

	if transient {
		return errors.New("fake: transient delivery failure")
	}

	for _, event := range pending {
		a.emitEvent(deliveryID, generation, event)
	}

	if releaseCh != nil {
		select {
		case <-releaseCh:
		case <-ctx.Done():
			c.mu.Lock()
			if c.gate != nil {
				c.gate.inFlight = nil
			}
			c.mu.Unlock()
			return ctx.Err()
		}
		c.mu.Lock()
		if c.gate != nil {
			c.gate.inFlight = nil
		}
		// Preserve mode if QueueSuccess / InjectTransientFailure already
		// switched away from the gate while this call was waiting.
		if c.mode == modeGate {
			c.mode = modeSuccess
		}
		c.mu.Unlock()
	}

	if reporter == nil {
		return nil
	}
	go func() {
		result := domain.DeliveryResult{State: domain.DeliveryStateDelivered}
		_ = reporter.ReportResult(context.Background(), deliveryID, generation, result)
		c.mu.Lock()
		c.reports = append(c.reports, Report{
			Kind:       ReportResult,
			DeliveryID: deliveryID,
			Generation: generation,
			Result:     &result,
			At:         c.now(),
		})
		c.mu.Unlock()
	}()
	return nil
}

// emitEvent reports event synchronously through the reporter (when set)
// and appends it to the controller's ordered report journal.
func (a *Agent) emitEvent(deliveryID domain.DeliveryID, generation domain.Generation, event domain.DeliveryEvent) {
	c := a.ctrl
	if a.reporter != nil {
		_ = a.reporter.ReportEvent(context.Background(), deliveryID, generation, event)
	}
	c.mu.Lock()
	ev := event
	c.reports = append(c.reports, Report{
		Kind:       ReportEvent,
		DeliveryID: deliveryID,
		Generation: generation,
		Event:      &ev,
		At:         c.now(),
	})
	c.mu.Unlock()
}
