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
	mu        sync.Mutex
	instances map[domain.DeploymentID]*instance
}

type instance struct {
	events chan []byte // JSON-serialized DeploymentEvent
}

func (r *Registry) getInstance(id domain.DeploymentID) *instance {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.instances == nil {
		r.instances = make(map[domain.DeploymentID]*instance)
	}
	inst, ok := r.instances[id]
	if !ok {
		inst = &instance{events: make(chan []byte, 16)}
		r.instances[id] = inst
	}
	return inst
}

func (r *Registry) removeInstance(id domain.DeploymentID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.instances, id)
}

// SignalDeploymentEvent JSON-serializes the event and delivers it to
// the workflow instance's signal channel, mirroring how durable engines
// persist signals before delivering them.
func (r *Registry) SignalDeploymentEvent(ctx context.Context, id domain.DeploymentID, event domain.DeploymentEvent) error {
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

func (r *Registry) RegisterOrchestration(spec *domain.OrchestrationWorkflowSpec) (domain.OrchestrationWorkflow, error) {
	return &orchestrationWorkflow{registry: r, spec: spec}, nil
}

func (r *Registry) RegisterCreateDeployment(spec *domain.CreateDeploymentWorkflowSpec) (domain.CreateDeploymentWorkflow, error) {
	return &createDeploymentWorkflow{spec: spec}, nil
}

func (r *Registry) RegisterDeleteDeployment(spec *domain.DeleteDeploymentWorkflowSpec) (domain.DeleteDeploymentWorkflow, error) {
	return &deleteDeploymentWorkflow{registry: r, spec: spec}, nil
}

func (r *Registry) RegisterResumeDeployment(spec *domain.ResumeDeploymentWorkflowSpec) (domain.ResumeDeploymentWorkflow, error) {
	return &resumeDeploymentWorkflow{registry: r, spec: spec}, nil
}

func (r *Registry) RegisterProvisionIdP(spec *domain.ProvisionIdPWorkflowSpec) (domain.ProvisionIdPWorkflow, error) {
	return &provisionIdPWorkflow{spec: spec}, nil
}

// --- OrchestrationWorkflow ---

type orchestrationWorkflow struct {
	registry *Registry
	spec     *domain.OrchestrationWorkflowSpec
	mu       sync.Mutex
	running  map[domain.DeploymentID]struct{}
}

func (w *orchestrationWorkflow) Start(ctx context.Context, deploymentID domain.DeploymentID) (domain.Execution[struct{}], error) {
	w.mu.Lock()
	if w.running == nil {
		w.running = make(map[domain.DeploymentID]struct{})
	}
	if _, active := w.running[deploymentID]; active {
		w.mu.Unlock()
		return nil, domain.ErrAlreadyRunning
	}
	w.running[deploymentID] = struct{}{}
	w.mu.Unlock()

	inst := w.registry.getInstance(deploymentID)

	done := make(chan orchResult, 1)

	go func() {
		defer func() {
			w.registry.removeInstance(deploymentID)
			w.mu.Lock()
			delete(w.running, deploymentID)
			w.mu.Unlock()
			if r := recover(); r != nil {
				done <- orchResult{err: fmt.Errorf("workflow panicked: %v", r)}
			}
		}()

		record := &baseRecord{
			id:     string(deploymentID),
			ctx:    ctx,
			events: inst.events,
		}
		val, err := w.spec.Run(record, deploymentID)
		done <- orchResult{val: val, err: err}
	}()

	return &orchExecution{id: string(deploymentID), done: done}, nil
}

// --- CreateDeploymentWorkflow ---

type createDeploymentWorkflow struct {
	spec *domain.CreateDeploymentWorkflowSpec
}

func (w *createDeploymentWorkflow) Start(ctx context.Context, input domain.CreateDeploymentInput) (domain.Execution[domain.Deployment], error) {
	done := make(chan createResult, 1)

	go func() {
		record := &baseRecord{
			id:  "create-" + string(input.ID),
			ctx: ctx,
		}
		val, err := w.spec.Run(record, input)
		done <- createResult{val: val, err: err}
	}()

	return &createExecution{id: "create-" + string(input.ID), done: done}, nil
}

// --- ProvisionIdPWorkflow ---

type provisionIdPWorkflow struct {
	spec *domain.ProvisionIdPWorkflowSpec
}

func (w *provisionIdPWorkflow) Start(ctx context.Context, input domain.ProvisionIdPInput) (domain.Execution[domain.AuthMethod], error) {
	done := make(chan provisionResult, 1)

	go func() {
		record := &baseRecord{
			id:  "provision-idp-" + string(input.AuthMethodID),
			ctx: ctx,
		}
		val, err := w.spec.Run(record, input)
		done <- provisionResult{val: val, err: err}
	}()

	return &provisionExecution{id: "provision-idp-" + string(input.AuthMethodID), done: done}, nil
}

// --- DeleteDeploymentWorkflow ---

type deleteDeploymentWorkflow struct {
	registry *Registry
	spec     *domain.DeleteDeploymentWorkflowSpec
	mu       sync.Mutex
	running  map[string]struct{}
}

func (w *deleteDeploymentWorkflow) Start(ctx context.Context, deploymentID domain.DeploymentID, observedGen domain.Generation) (domain.Execution[domain.Deployment], error) {
	instanceID := fmt.Sprintf("delete-%s-gen-%d", deploymentID, observedGen)

	w.mu.Lock()
	if w.running == nil {
		w.running = make(map[string]struct{})
	}
	if _, active := w.running[instanceID]; active {
		w.mu.Unlock()
		return nil, domain.ErrConcurrentUpdate
	}
	w.running[instanceID] = struct{}{}
	w.mu.Unlock()

	done := make(chan deploymentResult, 1)

	go func() {
		defer func() {
			w.mu.Lock()
			delete(w.running, instanceID)
			w.mu.Unlock()
			if r := recover(); r != nil {
				done <- deploymentResult{err: fmt.Errorf("workflow panicked: %v", r)}
			}
		}()

		record := &baseRecord{
			id:  instanceID,
			ctx: ctx,
		}
		val, err := w.spec.Run(record, deploymentID)
		done <- deploymentResult{val: val, err: err}
	}()

	return &deploymentExecution{id: instanceID, done: done}, nil
}

// --- ResumeDeploymentWorkflow ---

type resumeDeploymentWorkflow struct {
	registry *Registry
	spec     *domain.ResumeDeploymentWorkflowSpec
	mu       sync.Mutex
	running  map[string]struct{}
}

func (w *resumeDeploymentWorkflow) Start(ctx context.Context, input domain.ResumeDeploymentInput, observedGen domain.Generation) (domain.Execution[domain.Deployment], error) {
	instanceID := fmt.Sprintf("resume-%s-gen-%d", input.ID, observedGen)

	w.mu.Lock()
	if w.running == nil {
		w.running = make(map[string]struct{})
	}
	if _, active := w.running[instanceID]; active {
		w.mu.Unlock()
		return nil, domain.ErrConcurrentUpdate
	}
	w.running[instanceID] = struct{}{}
	w.mu.Unlock()

	done := make(chan deploymentResult, 1)

	go func() {
		defer func() {
			w.mu.Lock()
			delete(w.running, instanceID)
			w.mu.Unlock()
			if r := recover(); r != nil {
				done <- deploymentResult{err: fmt.Errorf("workflow panicked: %v", r)}
			}
		}()

		record := &baseRecord{
			id:  instanceID,
			ctx: ctx,
		}
		val, err := w.spec.Run(record, input)
		done <- deploymentResult{val: val, err: err}
	}()

	return &deploymentExecution{id: instanceID, done: done}, nil
}

// --- shared base Record ---

type baseRecord struct {
	id     string
	ctx    context.Context
	events <-chan []byte // JSON-serialized signals
}

func (r *baseRecord) ID() string               { return r.id }
func (r *baseRecord) Context() context.Context { return r.ctx }

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

// Await blocks until a signal arrives on the channel, then
// JSON-deserializes it into [domain.DeploymentEvent].
func (r *baseRecord) Await(signalName string) (any, error) {
	select {
	case data := <-r.events:
		var event domain.DeploymentEvent
		if err := json.Unmarshal(data, &event); err != nil {
			return nil, fmt.Errorf("memworkflow: unmarshal signal %q: %w", signalName, err)
		}
		return event, nil
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

// --- Executions and result types ---

type orchResult struct {
	val struct{}
	err error
}

type orchExecution struct {
	id   string
	done <-chan orchResult
}

func (e *orchExecution) WorkflowID() string { return e.id }
func (e *orchExecution) AwaitResult(ctx context.Context) (struct{}, error) {
	select {
	case r := <-e.done:
		return r.val, r.err
	case <-ctx.Done():
		return struct{}{}, ctx.Err()
	}
}

type createResult struct {
	val domain.Deployment
	err error
}

type createExecution struct {
	id   string
	done <-chan createResult
}

func (e *createExecution) WorkflowID() string { return e.id }
func (e *createExecution) AwaitResult(ctx context.Context) (domain.Deployment, error) {
	select {
	case r := <-e.done:
		return r.val, r.err
	case <-ctx.Done():
		return domain.Deployment{}, ctx.Err()
	}
}

type deploymentResult struct {
	val domain.Deployment
	err error
}

type deploymentExecution struct {
	id   string
	done <-chan deploymentResult
}

func (e *deploymentExecution) WorkflowID() string { return e.id }
func (e *deploymentExecution) AwaitResult(ctx context.Context) (domain.Deployment, error) {
	select {
	case r := <-e.done:
		return r.val, r.err
	case <-ctx.Done():
		return domain.Deployment{}, ctx.Err()
	}
}

type provisionResult struct {
	val domain.AuthMethod
	err error
}

type provisionExecution struct {
	id   string
	done <-chan provisionResult
}

func (e *provisionExecution) WorkflowID() string { return e.id }
func (e *provisionExecution) AwaitResult(ctx context.Context) (domain.AuthMethod, error) {
	select {
	case r := <-e.done:
		return r.val, r.err
	case <-ctx.Done():
		return domain.AuthMethod{}, ctx.Err()
	}
}

// Compile-time interface checks.
var (
	_ domain.Registry                  = (*Registry)(nil)
	_ domain.OrchestrationWorkflow     = (*orchestrationWorkflow)(nil)
	_ domain.CreateDeploymentWorkflow  = (*createDeploymentWorkflow)(nil)
	_ domain.DeleteDeploymentWorkflow  = (*deleteDeploymentWorkflow)(nil)
	_ domain.ResumeDeploymentWorkflow  = (*resumeDeploymentWorkflow)(nil)
	_ domain.ProvisionIdPWorkflow      = (*provisionIdPWorkflow)(nil)
)
