// Package memworkflow provides a lightweight, in-memory [domain.Registry]
// that faithfully reproduces the concurrency and serialization semantics
// of a durable workflow engine:
//
//   - Activities are dispatched to goroutines with a fresh
//     [context.Background] context (not the workflow context).
//   - Activity inputs and outputs go through a JSON round-trip,
//     matching what durable engines do when persisting activity state.
//   - Signals are JSON-serialized on send and deserialized on receive,
//     matching go-workflows' signal channel behavior.
//
// No durable state is kept; there is no replay. This package is the
// recommended workflow backend for fast, high-fidelity tests.
package memworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// activityMaxAttempts controls how many times a failing activity is
// retried before the error propagates to the workflow. Set high enough
// to ride out transient failures (database contention, network blips)
// without permanently failing the reconciliation.
const activityMaxAttempts = 10

// Registry implements [domain.Registry] with in-memory execution.
// Workflow instances are tracked so that event signals can be delivered
// to the correct goroutine.
type Registry struct {
	mu               sync.Mutex
	instances        map[domain.FulfillmentID]*instance
	cleanupInstances map[domain.FulfillmentID]*instance
}

type instance struct {
	events chan []byte // JSON-serialized signal events
}

func (r *Registry) getInstance(id domain.FulfillmentID) *instance {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.instances == nil {
		r.instances = make(map[domain.FulfillmentID]*instance)
	}
	inst, ok := r.instances[id]
	if !ok {
		inst = &instance{events: make(chan []byte, 16)}
		r.instances[id] = inst
	}
	return inst
}

func (r *Registry) removeInstance(id domain.FulfillmentID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.instances, id)
}

func (r *Registry) getCleanupInstance(id domain.FulfillmentID) *instance {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cleanupInstances == nil {
		r.cleanupInstances = make(map[domain.FulfillmentID]*instance)
	}
	inst, ok := r.cleanupInstances[id]
	if !ok {
		inst = &instance{events: make(chan []byte, 16)}
		r.cleanupInstances[id] = inst
	}
	return inst
}

func (r *Registry) removeCleanupInstance(id domain.FulfillmentID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.cleanupInstances, id)
}

// SignalFulfillmentEvent JSON-serializes the event and delivers it to
// the workflow instance's signal channel, mirroring how durable engines
// persist signals before delivering them.
func (r *Registry) SignalFulfillmentEvent(ctx context.Context, id domain.FulfillmentID, event domain.FulfillmentEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("memworkflow: marshal signal: %w", err)
	}
	inst := r.getInstance(id)
	select {
	case inst.events <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SignalDeleteCleanupComplete JSON-serializes the event and delivers it
// to the cleanup workflow instance's signal channel.
func (r *Registry) SignalDeleteCleanupComplete(ctx context.Context, id domain.FulfillmentID, event domain.DeleteCleanupCompleteEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("memworkflow: marshal cleanup signal: %w", err)
	}
	inst := r.getCleanupInstance(id)
	select {
	case inst.events <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Registry) RegisterOrchestration(spec *domain.OrchestrationWorkflowSpec) (domain.OrchestrationWorkflow, error) {
	return &orchestrationWorkflow{registry: r, spec: spec}, nil
}

func (r *Registry) RegisterCreateDeployment(spec *domain.CreateDeploymentWorkflowSpec) (domain.CreateDeploymentWorkflow, error) {
	return &createDeploymentWorkflow{spec: spec}, nil
}

func (r *Registry) RegisterDeleteDeployment(spec *domain.DeleteDeploymentWorkflowSpec) (domain.DeleteDeploymentWorkflow, error) {
	return &deleteDeploymentWorkflow{registry: r, spec: spec}, nil
}

func (r *Registry) RegisterDeleteDeploymentCleanup(spec *domain.DeleteDeploymentCleanupWorkflowSpec) (domain.DeleteDeploymentCleanupWorkflow, error) {
	return &deleteDeploymentCleanupWorkflow{registry: r, spec: spec}, nil
}

func (r *Registry) RegisterResumeDeployment(spec *domain.ResumeDeploymentWorkflowSpec) (domain.ResumeDeploymentWorkflow, error) {
	return &resumeDeploymentWorkflow{registry: r, spec: spec}, nil
}

func (r *Registry) RegisterProvisionIdP(spec *domain.ProvisionIdPWorkflowSpec) (domain.ProvisionIdPWorkflow, error) {
	return &provisionIdPWorkflow{spec: spec}, nil
}

func (r *Registry) RegisterCreateManagedResource(spec *domain.CreateManagedResourceWorkflowSpec) (domain.CreateManagedResourceWorkflow, error) {
	return &createManagedResourceWorkflow{spec: spec}, nil
}

func (r *Registry) RegisterDeleteManagedResource(spec *domain.DeleteManagedResourceWorkflowSpec) (domain.DeleteManagedResourceWorkflow, error) {
	return &deleteManagedResourceWorkflow{registry: r, spec: spec}, nil
}

func (r *Registry) RegisterDeleteManagedResourceCleanup(spec *domain.DeleteManagedResourceCleanupWorkflowSpec) (domain.DeleteManagedResourceCleanupWorkflow, error) {
	return &deleteManagedResourceCleanupWorkflow{registry: r, spec: spec}, nil
}

func (r *Registry) RegisterResumeManagedResource(spec *domain.ResumeManagedResourceWorkflowSpec) (domain.ResumeManagedResourceWorkflow, error) {
	return &resumeManagedResourceWorkflow{registry: r, spec: spec}, nil
}

// --- Workflow execution helpers ---

type result[T any] struct {
	val T
	err error
}

type execution[T any] struct {
	id   string
	done <-chan result[T]
}

func (e *execution[T]) WorkflowID() string { return e.id }
func (e *execution[T]) AwaitResult(ctx context.Context) (T, error) {
	select {
	case r := <-e.done:
		return r.val, r.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// guard provides single-instance-per-ID deduplication, matching the
// instance-ID uniqueness enforced by durable workflow backends.
type guard struct {
	mu      sync.Mutex
	running map[string]struct{}
}

// signalBinding pairs a signal name with its raw channel and a
// type-erased unmarshaler. Use [bindSignal] to construct one.
type signalBinding struct {
	name        string
	ch          <-chan []byte
	unmarshaler func([]byte) (any, error)
}

// bindSignal creates a [signalBinding] that deserializes incoming
// JSON-encoded signals into the concrete event type E.
func bindSignal[E any](name string, ch <-chan []byte) signalBinding {
	return signalBinding{
		name: name,
		ch:   ch,
		unmarshaler: func(data []byte) (any, error) {
			var event E
			if err := json.Unmarshal(data, &event); err != nil {
				return nil, fmt.Errorf("memworkflow: unmarshal signal: %w", err)
			}
			return event, nil
		},
	}
}

// startWorkflow is the single entry point for launching any workflow.
// It handles the complete lifecycle:
//   - Guard check: returns [domain.ErrAlreadyRunning] if an instance
//     with the same ID is already active.
//   - Record construction: builds a [baseRecord] from instanceID, ctx,
//     and the optional signal bindings.
//   - ContinueAsNew: wraps the run call in a loop that restarts on
//     [domain.ContinueAsNewError] (harmless for specs that never
//     return it -- the loop executes exactly once).
//   - Atomic finish: under the guard's lock, removes the running
//     entry, calls the optional cleanup, and sends the result, so a
//     concurrent Start cannot see the slot as vacant before the
//     result is delivered.
//   - Panic recovery: surfaced as an error on the execution.
func startWorkflow[I, O any](
	g *guard,
	instanceID string,
	ctx context.Context,
	cleanup func(),
	run func(domain.Record, I) (O, error),
	input I,
	signals ...signalBinding,
) (*execution[O], error) {
	g.mu.Lock()
	if g.running == nil {
		g.running = make(map[string]struct{})
	}
	if _, active := g.running[instanceID]; active {
		g.mu.Unlock()
		return nil, domain.ErrAlreadyRunning
	}
	g.running[instanceID] = struct{}{}
	g.mu.Unlock()

	done := make(chan result[O], 1)

	go func() {
		finish := func(r result[O]) {
			g.mu.Lock()
			delete(g.running, instanceID)
			if cleanup != nil {
				cleanup()
			}
			done <- r
			g.mu.Unlock()
		}
		defer func() {
			if rec := recover(); rec != nil {
				finish(result[O]{err: fmt.Errorf("workflow panicked: %v", rec)})
			}
		}()

		for {
			record := newRecord(instanceID, ctx, signals)
			val, err := run(record, input)
			var can *domain.ContinueAsNewError
			if errors.As(err, &can) {
				input = can.Input.(I)
				continue
			}
			finish(result[O]{val: val, err: err})
			return
		}
	}()

	return &execution[O]{id: instanceID, done: done}, nil
}

// newRecord builds a [baseRecord] wired with the given signal
// bindings. Workflows without signals get a plain record.
func newRecord(id string, ctx context.Context, signals []signalBinding) *baseRecord {
	execCtx := context.Background()

	if len(signals) == 0 {
		return &baseRecord{id: id, ctx: ctx, execCtx: execCtx}
	}

	sigs := make(map[string]func() (any, error), len(signals))
	sigChans := make(map[string]<-chan []byte, len(signals))
	unmarshalers := make(map[string]func([]byte) (any, error), len(signals))

	for _, s := range signals {
		s := s
		sigs[s.name] = func() (any, error) {
			select {
			case data := <-s.ch:
				return s.unmarshaler(data)
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		sigChans[s.name] = s.ch
		unmarshalers[s.name] = s.unmarshaler
	}

	return &baseRecord{
		id:           id,
		ctx:          ctx,
		execCtx:      execCtx,
		signals:      sigs,
		signalChans:  sigChans,
		unmarshalers: unmarshalers,
	}
}

// --- OrchestrationWorkflow ---

type orchestrationWorkflow struct {
	registry *Registry
	spec     *domain.OrchestrationWorkflowSpec
	guard    guard
}

func (w *orchestrationWorkflow) Start(ctx context.Context, fulfillmentID domain.FulfillmentID) (domain.Execution[struct{}], error) {
	inst := w.registry.getInstance(fulfillmentID)
	return startWorkflow(&w.guard, string(fulfillmentID), ctx,
		func() { w.registry.removeInstance(fulfillmentID) },
		w.spec.Run, fulfillmentID,
		bindSignal[domain.FulfillmentEvent](domain.FulfillmentEventSignal.Name, inst.events),
	)
}

// --- CreateDeploymentWorkflow ---

type createDeploymentWorkflow struct {
	spec  *domain.CreateDeploymentWorkflowSpec
	guard guard
}

func (w *createDeploymentWorkflow) Start(ctx context.Context, input domain.CreateDeploymentInput) (domain.Execution[domain.DeploymentView], error) {
	return startWorkflow(&w.guard, "create-"+string(input.ID), ctx,
		nil, w.spec.Run, input,
	)
}

// --- ProvisionIdPWorkflow ---

type provisionIdPWorkflow struct {
	spec  *domain.ProvisionIdPWorkflowSpec
	guard guard
}

func (w *provisionIdPWorkflow) Start(ctx context.Context, input domain.ProvisionIdPInput) (domain.Execution[domain.AuthMethod], error) {
	return startWorkflow(&w.guard, "provision-idp-"+string(input.AuthMethodID), ctx,
		nil, w.spec.Run, input,
	)
}

// --- DeleteDeploymentWorkflow ---

type deleteDeploymentWorkflow struct {
	registry *Registry
	spec     *domain.DeleteDeploymentWorkflowSpec
	guard    guard
}

func (w *deleteDeploymentWorkflow) Start(ctx context.Context, deploymentID domain.DeploymentID, observedGen domain.Generation) (domain.Execution[domain.DeploymentView], error) {
	instanceID := fmt.Sprintf("delete-%s-gen-%d", deploymentID, observedGen)
	return startWorkflow(&w.guard, instanceID, ctx,
		nil, w.spec.Run, deploymentID,
	)
}

// --- ResumeDeploymentWorkflow ---

type resumeDeploymentWorkflow struct {
	registry *Registry
	spec     *domain.ResumeDeploymentWorkflowSpec
	guard    guard
}

func (w *resumeDeploymentWorkflow) Start(ctx context.Context, input domain.ResumeDeploymentInput, observedGen domain.Generation) (domain.Execution[domain.DeploymentView], error) {
	instanceID := fmt.Sprintf("resume-%s-gen-%d", input.ID, observedGen)
	return startWorkflow(&w.guard, instanceID, ctx,
		nil, w.spec.Run, input,
	)
}

// --- DeleteDeploymentCleanupWorkflow ---

type deleteDeploymentCleanupWorkflow struct {
	registry *Registry
	spec     *domain.DeleteDeploymentCleanupWorkflowSpec
	guard    guard
}

func (w *deleteDeploymentCleanupWorkflow) Start(ctx context.Context, input domain.DeleteDeploymentCleanupInput) (domain.Execution[struct{}], error) {
	instanceID := domain.DeleteCleanupWorkflowID(input.FulfillmentID)
	inst := w.registry.getCleanupInstance(input.FulfillmentID)
	return startWorkflow(&w.guard, instanceID, ctx,
		func() { w.registry.removeCleanupInstance(input.FulfillmentID) },
		w.spec.Run, input,
		bindSignal[domain.DeleteCleanupCompleteEvent](domain.DeleteCleanupCompleteSignal.Name, inst.events),
	)
}

// --- DeleteManagedResourceCleanupWorkflow ---

type deleteManagedResourceCleanupWorkflow struct {
	registry *Registry
	spec     *domain.DeleteManagedResourceCleanupWorkflowSpec
	guard    guard
}

func (w *deleteManagedResourceCleanupWorkflow) Start(ctx context.Context, input domain.DeleteManagedResourceCleanupInput) (domain.Execution[struct{}], error) {
	instanceID := domain.DeleteCleanupWorkflowID(input.FulfillmentID)
	inst := w.registry.getCleanupInstance(input.FulfillmentID)
	return startWorkflow(&w.guard, instanceID, ctx,
		func() { w.registry.removeCleanupInstance(input.FulfillmentID) },
		w.spec.Run, input,
		bindSignal[domain.DeleteCleanupCompleteEvent](domain.DeleteCleanupCompleteSignal.Name, inst.events),
	)
}

// --- CreateManagedResourceWorkflow ---

type createManagedResourceWorkflow struct {
	spec  *domain.CreateManagedResourceWorkflowSpec
	guard guard
}

func (w *createManagedResourceWorkflow) Start(ctx context.Context, input domain.CreateManagedResourceInput) (domain.Execution[domain.ManagedResourceView], error) {
	instanceID := domain.CreateManagedResourceWorkflowID(input.ResourceType, input.Name)
	return startWorkflow(&w.guard, instanceID, ctx,
		nil, w.spec.Run, input,
	)
}

// --- DeleteManagedResourceWorkflow ---

type deleteManagedResourceWorkflow struct {
	registry *Registry
	spec     *domain.DeleteManagedResourceWorkflowSpec
	guard    guard
}

func (w *deleteManagedResourceWorkflow) Start(ctx context.Context, input domain.DeleteManagedResourceInput) (domain.Execution[domain.ManagedResourceView], error) {
	instanceID := domain.DeleteManagedResourceWorkflowID(input.ResourceType, input.Name)
	return startWorkflow(&w.guard, instanceID, ctx,
		nil, w.spec.Run, input,
	)
}

// --- ResumeManagedResourceWorkflow ---

type resumeManagedResourceWorkflow struct {
	registry *Registry
	spec     *domain.ResumeManagedResourceWorkflowSpec
	guard    guard
}

func (w *resumeManagedResourceWorkflow) Start(ctx context.Context, input domain.ResumeManagedResourceInput, observedGen domain.Generation) (domain.Execution[domain.ManagedResourceView], error) {
	instanceID := fmt.Sprintf("resume-mr-%s-%s-gen-%d", input.ResourceType, input.Name, observedGen)
	return startWorkflow(&w.guard, instanceID, ctx,
		nil, w.spec.Run, input,
	)
}

// --- shared base Record ---

type baseRecord struct {
	id           string
	ctx          context.Context                      // lifecycle context for internal use (Sleep, runOnce)
	execCtx      context.Context                      // detached execution context returned by Context()
	signals      map[string]func() (any, error)       // per-signal-name blocking receivers
	signalChans  map[string]<-chan []byte             // raw channels for AwaitWithTimeout
	unmarshalers map[string]func([]byte) (any, error) // per-signal-name deserializers
}

func (r *baseRecord) ID() string               { return r.id }
func (r *baseRecord) Context() context.Context { return r.execCtx }

func (r *baseRecord) unmarshalSignal(name string, data []byte) (any, error) {
	unmarshal, ok := r.unmarshalers[name]
	if !ok {
		return nil, fmt.Errorf("memworkflow: no unmarshaler for signal %q", name)
	}
	return unmarshal(data)
}

// Run dispatches the activity to a goroutine with a fresh context and
// JSON round-trips both input and output. The workflow goroutine blocks
// until the activity completes. Failed activities are retried up to
// [activityMaxAttempts] times, matching go-workflows' default retry
// policy. Between retries a [runtime.Gosched] yields the processor so
// transient contention (e.g. SQLite SQLITE_LOCKED) can resolve.
func (r *baseRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	deserializedIn, err := jsonRoundTrip(in)
	if err != nil {
		return nil, fmt.Errorf("memworkflow: round-trip activity %q input: %w", activity.Name(), err)
	}

	var lastErr error
	for attempt := range activityMaxAttempts {
		if attempt > 0 {
			runtime.Gosched()
		}

		out, err := r.runOnce(activity, deserializedIn)
		if err != nil {
			lastErr = err
			if domain.IsTerminal(err) {
				break
			}
			continue
		}

		deserializedOut, err := jsonRoundTrip(out)
		if err != nil {
			return nil, fmt.Errorf("memworkflow: round-trip activity %q output: %w", activity.Name(), err)
		}
		return deserializedOut, nil
	}
	return nil, lastErr
}

// runOnce dispatches a single activity attempt in a goroutine with a
// fresh [context.Background] and blocks until it completes or the
// workflow context is cancelled.
func (r *baseRecord) runOnce(activity domain.Activity[any, any], in any) (any, error) {
	type actResult struct {
		out any
		err error
	}
	ch := make(chan actResult, 1)
	go func() {
		out, err := activity.Run(context.Background(), in)
		ch <- actResult{out, err}
	}()

	select {
	case res := <-ch:
		return res.out, res.err
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	}
}

// Sleep pauses the workflow for at least the given duration using a
// cancellable timer rather than bare [time.Sleep].
func (r *baseRecord) Sleep(d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
}

// Await blocks until the named signal arrives by calling the
// registered receiver for that signal name.
func (r *baseRecord) Await(signalName string) (any, error) {
	recv, ok := r.signals[signalName]
	if !ok {
		return nil, fmt.Errorf("memworkflow: no signal receiver registered for %q", signalName)
	}
	return recv()
}

// AwaitWithTimeout blocks until the named signal arrives or the timeout
// expires. A zero timeout is non-blocking. Returns [domain.ErrSignalTimeout]
// if no signal is received within the timeout.
func (r *baseRecord) AwaitWithTimeout(signalName string, timeout time.Duration) (any, error) {
	ch, ok := r.signalChans[signalName]
	if !ok {
		return nil, fmt.Errorf("memworkflow: no signal receiver registered for %q", signalName)
	}

	if timeout == 0 {
		select {
		case data := <-ch:
			return r.unmarshalSignal(signalName, data)
		default:
			return nil, domain.ErrSignalTimeout
		}
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case data := <-ch:
		return r.unmarshalSignal(signalName, data)
	case <-timer.C:
		return nil, domain.ErrSignalTimeout
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	}
}

// jsonRoundTrip marshals v to JSON and unmarshals into a new value of
// the same concrete type. This catches serialization issues that would
// be silent without a durable engine.
func jsonRoundTrip(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	t := reflect.TypeOf(v)
	ptr := reflect.New(t)
	if err := json.Unmarshal(data, ptr.Interface()); err != nil {
		return nil, err
	}
	return ptr.Elem().Interface(), nil
}

// Compile-time interface checks.
var (
	_ domain.Registry                             = (*Registry)(nil)
	_ domain.OrchestrationWorkflow                = (*orchestrationWorkflow)(nil)
	_ domain.CreateDeploymentWorkflow             = (*createDeploymentWorkflow)(nil)
	_ domain.DeleteDeploymentWorkflow             = (*deleteDeploymentWorkflow)(nil)
	_ domain.DeleteDeploymentCleanupWorkflow      = (*deleteDeploymentCleanupWorkflow)(nil)
	_ domain.ResumeDeploymentWorkflow             = (*resumeDeploymentWorkflow)(nil)
	_ domain.ProvisionIdPWorkflow                 = (*provisionIdPWorkflow)(nil)
	_ domain.CreateManagedResourceWorkflow        = (*createManagedResourceWorkflow)(nil)
	_ domain.DeleteManagedResourceWorkflow        = (*deleteManagedResourceWorkflow)(nil)
	_ domain.DeleteManagedResourceCleanupWorkflow = (*deleteManagedResourceCleanupWorkflow)(nil)
	_ domain.ResumeManagedResourceWorkflow        = (*resumeManagedResourceWorkflow)(nil)
)
