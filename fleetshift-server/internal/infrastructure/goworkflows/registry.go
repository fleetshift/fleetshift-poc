// Package goworkflows implements [domain.Registry] using
// cschleiden/go-workflows for durable workflow execution.
package goworkflows

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/client"
	goregistry "github.com/cschleiden/go-workflows/registry"
	"github.com/cschleiden/go-workflows/worker"
	"github.com/cschleiden/go-workflows/workflow"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// defaultActivityOptions provides aggressive retries so that transient
// failures (network blips, database contention) do not permanently fail
// a reconciliation.
var defaultActivityOptions = workflow.ActivityOptions{
	RetryOptions: workflow.RetryOptions{
		MaxAttempts:        20,
		FirstRetryInterval: 500 * time.Millisecond,
		MaxRetryInterval:   30 * time.Second,
		BackoffCoefficient: 2,
	},
}

// activityInvoker calls an activity from the workflow context with the
// correct generic types. Created at construction time when concrete
// types are known.
type activityInvoker func(wfCtx workflow.Context, in any) (any, error)

// Registry implements [domain.Registry] backed by go-workflows.
type Registry struct {
	Worker          *worker.Worker
	Client          *client.Client
	Timeout         time.Duration
	ActivityOptions *workflow.ActivityOptions // nil uses defaultActivityOptions
}

func (r *Registry) activityOptions() workflow.ActivityOptions {
	if r.ActivityOptions != nil {
		return *r.ActivityOptions
	}
	return defaultActivityOptions
}

func (r *Registry) timeout() time.Duration {
	if r.Timeout > 0 {
		return r.Timeout
	}
	return 30 * time.Second
}

func (r *Registry) SignalFulfillmentEvent(ctx context.Context, fulfillmentID domain.FulfillmentID, event domain.FulfillmentEvent) error {
	return r.Client.SignalWorkflow(ctx, string(fulfillmentID), domain.FulfillmentEventSignal.Name, event)
}

func (r *Registry) SignalDeleteCleanupComplete(ctx context.Context, fulfillmentID domain.FulfillmentID, event domain.DeleteCleanupCompleteEvent) error {
	return r.Client.SignalWorkflow(ctx, domain.DeleteCleanupWorkflowID(fulfillmentID), domain.DeleteCleanupCompleteSignal.Name, event)
}

func (r *Registry) RegisterOrchestration(spec *domain.OrchestrationWorkflowSpec) (domain.OrchestrationWorkflow, error) {
	invokers := make(map[string]activityInvoker)
	opts := r.activityOptions()

	for _, reg := range []func() error{
		func() error { return registerActivity(r.Worker, invokers, spec.AcquireLockAndLoad(), opts) },
		func() error { return registerActivity(r.Worker, invokers, spec.ResolvePlacement(), opts) },
		func() error { return registerActivity(r.Worker, invokers, spec.PlanRollout(), opts) },
		func() error { return registerActivity(r.Worker, invokers, spec.GenerateManifests(), opts) },
		func() error { return registerActivity(r.Worker, invokers, spec.DeliverToTarget(), opts) },
		func() error { return registerActivity(r.Worker, invokers, spec.RemoveFromTarget(), opts) },
		func() error {
			return registerActivity(r.Worker, invokers, spec.PersistAndCompleteReconciliation(), opts)
		},
		func() error { return registerActivity(r.Worker, invokers, spec.ProcessDeliveryOutputs(), opts) },
		func() error { return registerActivity(r.Worker, invokers, spec.CheckGeneration(), opts) },
		func() error { return registerActivity(r.Worker, invokers, spec.CleanupDeliveryData(), opts) },
		func() error { return registerActivity(r.Worker, invokers, spec.ReleaseLock(), opts) },
	} {
		if err := reg(); err != nil {
			return nil, err
		}
	}

	ow := &orchestrationWorkflow{
		client:  r.Client,
		wfName:  spec.Name(),
		timeout: r.timeout(),
	}

	wfFunc := func(ctx workflow.Context, fulfillmentID domain.FulfillmentID) (struct{}, error) {
		ch := workflow.NewSignalChannel[domain.FulfillmentEvent](ctx, domain.FulfillmentEventSignal.Name)
		port := newSignalPort(ch)
		record := newBaseRecord(ctx, invokers)
		record.signals = map[string]func() (any, error){
			domain.FulfillmentEventSignal.Name: func() (any, error) {
				val, ok := ch.Receive(ctx)
				if !ok {
					return nil, fmt.Errorf("signal channel %q closed", domain.FulfillmentEventSignal.Name)
				}
				return val, nil
			},
		}
		record.signalPorts = map[string]*signalPort{
			domain.FulfillmentEventSignal.Name: port,
		}
		val, err := spec.Run(record, fulfillmentID)
		var can *domain.ContinueAsNewError
		if errors.As(err, &can) {
			return val, workflow.ContinueAsNew(ctx, can.Input.(domain.FulfillmentID))
		}
		return val, err
	}

	if err := r.Worker.RegisterWorkflow(wfFunc, goregistry.WithName(spec.Name())); err != nil {
		return nil, fmt.Errorf("register workflow %q: %w", spec.Name(), err)
	}

	return ow, nil
}

func (r *Registry) RegisterCreateDeployment(spec *domain.CreateDeploymentWorkflowSpec) (domain.CreateDeploymentWorkflow, error) {
	invokers := make(map[string]activityInvoker)
	opts := r.activityOptions()

	if err := registerActivity(r.Worker, invokers, spec.PersistDeployment(), opts); err != nil {
		return nil, err
	}
	if err := registerActivity(r.Worker, invokers, spec.StartOrchestration(), opts); err != nil {
		return nil, err
	}

	wfFunc := func(ctx workflow.Context, input domain.CreateDeploymentInput) (domain.DeploymentView, error) {
		return spec.Run(newBaseRecord(ctx, invokers), input)
	}

	if err := r.Worker.RegisterWorkflow(wfFunc, goregistry.WithName(spec.Name())); err != nil {
		return nil, fmt.Errorf("register workflow %q: %w", spec.Name(), err)
	}

	return &createDeploymentWorkflow{
		client:  r.Client,
		wfName:  spec.Name(),
		timeout: r.timeout(),
	}, nil
}

func (r *Registry) RegisterDeleteDeployment(spec *domain.DeleteDeploymentWorkflowSpec) (domain.DeleteDeploymentWorkflow, error) {
	invokers := make(map[string]activityInvoker)
	opts := r.activityOptions()

	if err := registerActivity(r.Worker, invokers, spec.MutateToDeleting(), opts); err != nil {
		return nil, err
	}
	if err := registerActivity(r.Worker, invokers, spec.LoadFulfillment(), opts); err != nil {
		return nil, err
	}
	if err := registerActivity(r.Worker, invokers, spec.StartCleanup(), opts); err != nil {
		return nil, err
	}

	wfFunc := func(ctx workflow.Context, input domain.DeleteDeploymentInput) (domain.DeploymentView, error) {
		return spec.Run(newBaseRecord(ctx, invokers), input)
	}

	if err := r.Worker.RegisterWorkflow(wfFunc, goregistry.WithName(spec.Name())); err != nil {
		return nil, fmt.Errorf("register workflow %q: %w", spec.Name(), err)
	}

	return &deleteDeploymentWorkflow{
		client:  r.Client,
		wfName:  spec.Name(),
		timeout: r.timeout(),
	}, nil
}

func (r *Registry) RegisterDeleteDeploymentCleanup(spec *domain.DeleteDeploymentCleanupWorkflowSpec) (domain.DeleteDeploymentCleanupWorkflow, error) {
	invokers := make(map[string]activityInvoker)
	opts := r.activityOptions()

	if err := registerActivity(r.Worker, invokers, spec.DeleteDeploymentAndFulfillment(), opts); err != nil {
		return nil, err
	}

	wfFunc := func(ctx workflow.Context, input domain.DeleteDeploymentCleanupInput) (struct{}, error) {
		ch := workflow.NewSignalChannel[domain.DeleteCleanupCompleteEvent](ctx, domain.DeleteCleanupCompleteSignal.Name)
		port := newSignalPort(ch)
		record := newBaseRecord(ctx, invokers)
		record.signals = map[string]func() (any, error){
			domain.DeleteCleanupCompleteSignal.Name: func() (any, error) {
				val, ok := ch.Receive(ctx)
				if !ok {
					return nil, fmt.Errorf("signal channel %q closed", domain.DeleteCleanupCompleteSignal.Name)
				}
				return val, nil
			},
		}
		record.signalPorts = map[string]*signalPort{
			domain.DeleteCleanupCompleteSignal.Name: port,
		}
		return spec.Run(record, input)
	}

	if err := r.Worker.RegisterWorkflow(wfFunc, goregistry.WithName(spec.Name())); err != nil {
		return nil, fmt.Errorf("register workflow %q: %w", spec.Name(), err)
	}

	return &deleteDeploymentCleanupWorkflow{
		client:  r.Client,
		wfName:  spec.Name(),
		timeout: r.timeout(),
	}, nil
}

func (r *Registry) RegisterDeleteManagedResourceCleanup(spec *domain.DeleteManagedResourceCleanupWorkflowSpec) (domain.DeleteManagedResourceCleanupWorkflow, error) {
	invokers := make(map[string]activityInvoker)
	opts := r.activityOptions()

	if err := registerActivity(r.Worker, invokers, spec.DeleteManagedResourceAndFulfillment(), opts); err != nil {
		return nil, err
	}

	wfFunc := func(ctx workflow.Context, input domain.DeleteManagedResourceCleanupInput) (struct{}, error) {
		ch := workflow.NewSignalChannel[domain.DeleteCleanupCompleteEvent](ctx, domain.DeleteCleanupCompleteSignal.Name)
		port := newSignalPort(ch)
		record := newBaseRecord(ctx, invokers)
		record.signals = map[string]func() (any, error){
			domain.DeleteCleanupCompleteSignal.Name: func() (any, error) {
				val, ok := ch.Receive(ctx)
				if !ok {
					return nil, fmt.Errorf("signal channel %q closed", domain.DeleteCleanupCompleteSignal.Name)
				}
				return val, nil
			},
		}
		record.signalPorts = map[string]*signalPort{
			domain.DeleteCleanupCompleteSignal.Name: port,
		}
		return spec.Run(record, input)
	}

	if err := r.Worker.RegisterWorkflow(wfFunc, goregistry.WithName(spec.Name())); err != nil {
		return nil, fmt.Errorf("register workflow %q: %w", spec.Name(), err)
	}

	return &deleteManagedResourceCleanupWorkflow{
		client:  r.Client,
		wfName:  spec.Name(),
		timeout: r.timeout(),
	}, nil
}

func (r *Registry) RegisterResumeManagedResource(spec *domain.ResumeManagedResourceWorkflowSpec) (domain.ResumeManagedResourceWorkflow, error) {
	invokers := make(map[string]activityInvoker)
	opts := r.activityOptions()

	if err := registerActivity(r.Worker, invokers, spec.MutateToResumed(), opts); err != nil {
		return nil, err
	}
	if err := registerActivity(r.Worker, invokers, spec.LoadFulfillment(), opts); err != nil {
		return nil, err
	}

	wfFunc := func(ctx workflow.Context, input domain.ResumeManagedResourceInput) (domain.ExtensionResourceView, error) {
		return spec.Run(newBaseRecord(ctx, invokers), input)
	}

	if err := r.Worker.RegisterWorkflow(wfFunc, goregistry.WithName(spec.Name())); err != nil {
		return nil, fmt.Errorf("register workflow %q: %w", spec.Name(), err)
	}

	return &resumeManagedResourceWorkflow{
		client:  r.Client,
		wfName:  spec.Name(),
		timeout: r.timeout(),
	}, nil
}

func (r *Registry) RegisterResumeDeployment(spec *domain.ResumeDeploymentWorkflowSpec) (domain.ResumeDeploymentWorkflow, error) {
	invokers := make(map[string]activityInvoker)
	opts := r.activityOptions()

	if err := registerActivity(r.Worker, invokers, spec.MutateToResumed(), opts); err != nil {
		return nil, err
	}
	if err := registerActivity(r.Worker, invokers, spec.LoadFulfillment(), opts); err != nil {
		return nil, err
	}

	wfFunc := func(ctx workflow.Context, input domain.ResumeDeploymentInput) (domain.DeploymentView, error) {
		return spec.Run(newBaseRecord(ctx, invokers), input)
	}

	if err := r.Worker.RegisterWorkflow(wfFunc, goregistry.WithName(spec.Name())); err != nil {
		return nil, fmt.Errorf("register workflow %q: %w", spec.Name(), err)
	}

	return &resumeDeploymentWorkflow{
		client:  r.Client,
		wfName:  spec.Name(),
		timeout: r.timeout(),
	}, nil
}

func (r *Registry) RegisterProvisionIdP(spec *domain.ProvisionIdPWorkflowSpec) (domain.ProvisionIdPWorkflow, error) {
	invokers := make(map[string]activityInvoker)
	opts := r.activityOptions()

	if err := registerActivity(r.Worker, invokers, spec.ResolveAndPersist(), opts); err != nil {
		return nil, err
	}
	if err := registerActivity(r.Worker, invokers, spec.DeployTrustBundle(), opts); err != nil {
		return nil, err
	}

	wfFunc := func(ctx workflow.Context, input domain.ProvisionIdPInput) (domain.AuthMethod, error) {
		return spec.Run(newBaseRecord(ctx, invokers), input)
	}

	if err := r.Worker.RegisterWorkflow(wfFunc, goregistry.WithName(spec.Name())); err != nil {
		return nil, fmt.Errorf("register workflow %q: %w", spec.Name(), err)
	}

	return &provisionIdPWorkflow{
		client:  r.Client,
		wfName:  spec.Name(),
		timeout: r.timeout(),
	}, nil
}

func (r *Registry) RegisterCreateManagedResource(spec *domain.CreateManagedResourceWorkflowSpec) (domain.CreateManagedResourceWorkflow, error) {
	invokers := make(map[string]activityInvoker)
	opts := r.activityOptions()

	if err := registerActivity(r.Worker, invokers, spec.PersistManagedResource(), opts); err != nil {
		return nil, err
	}
	if err := registerActivity(r.Worker, invokers, spec.StartOrchestration(), opts); err != nil {
		return nil, err
	}

	wfFunc := func(ctx workflow.Context, input domain.CreateManagedResourceInput) (domain.ExtensionResourceView, error) {
		return spec.Run(newBaseRecord(ctx, invokers), input)
	}

	if err := r.Worker.RegisterWorkflow(wfFunc, goregistry.WithName(spec.Name())); err != nil {
		return nil, fmt.Errorf("register workflow %q: %w", spec.Name(), err)
	}

	return &createManagedResourceWorkflow{
		client:  r.Client,
		wfName:  spec.Name(),
		timeout: r.timeout(),
	}, nil
}

func (r *Registry) RegisterDeleteManagedResource(spec *domain.DeleteManagedResourceWorkflowSpec) (domain.DeleteManagedResourceWorkflow, error) {
	invokers := make(map[string]activityInvoker)
	opts := r.activityOptions()

	if err := registerActivity(r.Worker, invokers, spec.MutateToDeleting(), opts); err != nil {
		return nil, err
	}
	if err := registerActivity(r.Worker, invokers, spec.StartCleanup(), opts); err != nil {
		return nil, err
	}
	if err := registerActivity(r.Worker, invokers, spec.LoadFulfillment(), opts); err != nil {
		return nil, err
	}

	wfFunc := func(ctx workflow.Context, input domain.DeleteManagedResourceInput) (domain.ExtensionResourceView, error) {
		return spec.Run(newBaseRecord(ctx, invokers), input)
	}

	if err := r.Worker.RegisterWorkflow(wfFunc, goregistry.WithName(spec.Name())); err != nil {
		return nil, fmt.Errorf("register workflow %q: %w", spec.Name(), err)
	}

	return &deleteManagedResourceWorkflow{
		client:  r.Client,
		wfName:  spec.Name(),
		timeout: r.timeout(),
	}, nil
}

// registerActivity registers a typed activity with go-workflows and
// creates a corresponding typed invoker.
func registerActivity[I, O any](
	w *worker.Worker,
	invokers map[string]activityInvoker,
	activity domain.Activity[I, O],
	opts workflow.ActivityOptions,
) error {
	name := activity.Name()

	activityFn := func(ctx context.Context, in I) (O, error) {
		out, err := activity.Run(ctx, in)
		if err != nil && (domain.IsTerminal(err) || domain.IsAuthExpired(err)) {
			return out, workflow.NewPermanentError(err)
		}
		return out, err
	}
	if err := w.RegisterActivity(activityFn, goregistry.WithName(name)); err != nil {
		return fmt.Errorf("register activity %q: %w", name, err)
	}

	invokers[name] = func(wfCtx workflow.Context, in any) (any, error) {
		result, err := workflow.ExecuteActivity[O](
			wfCtx, opts, name, in,
		).Get(wfCtx)
		return result, err
	}

	return nil
}

// --- shared base Record ---

// signalPort provides timeout-aware signal reception for a typed
// go-workflows channel. Closures capture the concrete channel type.
type signalPort struct {
	receive            func(workflow.Context) (any, bool)
	receiveNonBlocking func() (any, bool)
	addToSelect        func(workflow.Context, *[]workflow.SelectCase, *any)
}

type baseRecord struct {
	wfCtx       workflow.Context
	execCtx     context.Context
	invokers    map[string]activityInvoker
	signals     map[string]func() (any, error)
	signalPorts map[string]*signalPort
}

func newBaseRecord(wfCtx workflow.Context, invokers map[string]activityInvoker) *baseRecord {
	return &baseRecord{
		wfCtx:    wfCtx,
		execCtx:  context.Background(),
		invokers: invokers,
	}
}

func (r *baseRecord) ID() string {
	return workflow.WorkflowInstance(r.wfCtx).InstanceID
}

func (r *baseRecord) Context() context.Context {
	return r.execCtx
}

func (r *baseRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	invoke, ok := r.invokers[activity.Name()]
	if !ok {
		return nil, fmt.Errorf("activity %q not registered", activity.Name())
	}
	out, err := invoke(r.wfCtx, in)
	if err != nil && !workflow.CanRetry(err) {
		if domain.IsAuthExpired(err) {
			return out, fmt.Errorf("%w: %v", domain.ErrAuthExpired, err)
		}
		return out, domain.TerminalError(err)
	}
	return out, err
}

func (r *baseRecord) Await(signalName string) (any, error) {
	recv, ok := r.signals[signalName]
	if !ok {
		return nil, fmt.Errorf("signal %q not registered", signalName)
	}
	return recv()
}

func (r *baseRecord) AwaitWithTimeout(signalName string, timeout time.Duration) (any, error) {
	port, ok := r.signalPorts[signalName]
	if !ok {
		return nil, fmt.Errorf("signal %q not registered", signalName)
	}

	if timeout == 0 {
		val, ok := port.receiveNonBlocking()
		if !ok {
			return nil, domain.ErrSignalTimeout
		}
		return val, nil
	}

	var result any
	var timedOut bool
	tctx, cancel := workflow.WithCancel(r.wfCtx)
	var cases []workflow.SelectCase
	port.addToSelect(r.wfCtx, &cases, &result)
	cases = append(cases, workflow.Await(workflow.ScheduleTimer(tctx, timeout), func(_ workflow.Context, _ workflow.Future[any]) {
		timedOut = true
	}))
	workflow.Select(r.wfCtx, cases...)
	cancel()
	if timedOut {
		return nil, domain.ErrSignalTimeout
	}
	return result, nil
}

func (r *baseRecord) Sleep(d time.Duration) error {
	return workflow.Sleep(r.wfCtx, d)
}

// newSignalPort creates a [signalPort] from a typed go-workflows channel.
func newSignalPort[T any](ch workflow.Channel[T]) *signalPort {
	return &signalPort{
		receive: func(ctx workflow.Context) (any, bool) {
			val, ok := ch.Receive(ctx)
			return val, ok
		},
		receiveNonBlocking: func() (any, bool) {
			return ch.ReceiveNonBlocking()
		},
		addToSelect: func(ctx workflow.Context, cases *[]workflow.SelectCase, result *any) {
			*cases = append(*cases, workflow.Receive(ch, func(_ workflow.Context, v T, _ bool) {
				*result = v
			}))
		},
	}
}

// --- OrchestrationWorkflow ---

type orchestrationWorkflow struct {
	client  *client.Client
	wfName  string
	timeout time.Duration
}

func (w *orchestrationWorkflow) Start(ctx context.Context, fulfillmentID domain.FulfillmentID) (domain.Execution[struct{}], error) {
	instance, err := w.client.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: string(fulfillmentID),
	}, w.wfName, fulfillmentID)
	if errors.Is(err, backend.ErrInstanceAlreadyExists) {
		return nil, domain.ErrAlreadyRunning
	}
	if err != nil {
		return nil, fmt.Errorf("create workflow instance: %w", err)
	}

	return &execution[struct{}]{
		client:   w.client,
		instance: instance,
		timeout:  w.timeout,
	}, nil
}

// --- CreateDeploymentWorkflow ---

type createDeploymentWorkflow struct {
	client  *client.Client
	wfName  string
	timeout time.Duration
}

func (w *createDeploymentWorkflow) Start(ctx context.Context, input domain.CreateDeploymentInput) (domain.Execution[domain.DeploymentView], error) {
	instance, err := w.client.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: "create-" + string(input.Name),
	}, w.wfName, input)
	if err != nil {
		return nil, fmt.Errorf("create workflow instance: %w", err)
	}

	return &execution[domain.DeploymentView]{
		client:   w.client,
		instance: instance,
		timeout:  w.timeout,
	}, nil
}

// --- ProvisionIdPWorkflow ---

type provisionIdPWorkflow struct {
	client  *client.Client
	wfName  string
	timeout time.Duration
}

func (w *provisionIdPWorkflow) Start(ctx context.Context, input domain.ProvisionIdPInput) (domain.Execution[domain.AuthMethod], error) {
	instance, err := w.client.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: "provision-idp-" + string(input.AuthMethodID),
	}, w.wfName, input)
	if err != nil {
		return nil, fmt.Errorf("create workflow instance: %w", err)
	}

	return &execution[domain.AuthMethod]{
		client:   w.client,
		instance: instance,
		timeout:  w.timeout,
	}, nil
}

// --- DeleteDeploymentWorkflow ---

type deleteDeploymentWorkflow struct {
	client  *client.Client
	wfName  string
	timeout time.Duration
}

func (w *deleteDeploymentWorkflow) Start(ctx context.Context, input domain.DeleteDeploymentInput, observedGen domain.Generation) (domain.Execution[domain.DeploymentView], error) {
	instanceID := fmt.Sprintf("delete-%s-gen-%d", input.Name, observedGen)
	instance, err := w.client.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: instanceID,
	}, w.wfName, input)
	if errors.Is(err, backend.ErrInstanceAlreadyExists) {
		return nil, domain.ErrAlreadyRunning
	}
	if err != nil {
		return nil, fmt.Errorf("create workflow instance: %w", err)
	}

	return &execution[domain.DeploymentView]{
		client:   w.client,
		instance: instance,
		timeout:  w.timeout,
	}, nil
}

// --- DeleteDeploymentCleanupWorkflow ---

type deleteDeploymentCleanupWorkflow struct {
	client  *client.Client
	wfName  string
	timeout time.Duration
}

func (w *deleteDeploymentCleanupWorkflow) Start(ctx context.Context, input domain.DeleteDeploymentCleanupInput) (domain.Execution[struct{}], error) {
	instanceID := domain.DeleteCleanupWorkflowID(input.FulfillmentID)
	instance, err := w.client.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: instanceID,
	}, w.wfName, input)
	if errors.Is(err, backend.ErrInstanceAlreadyExists) {
		return nil, domain.ErrAlreadyRunning
	}
	if err != nil {
		return nil, fmt.Errorf("create workflow instance: %w", err)
	}

	return &execution[struct{}]{
		client:   w.client,
		instance: instance,
		timeout:  w.timeout,
	}, nil
}

// --- DeleteManagedResourceCleanupWorkflow ---

type deleteManagedResourceCleanupWorkflow struct {
	client  *client.Client
	wfName  string
	timeout time.Duration
}

func (w *deleteManagedResourceCleanupWorkflow) Start(ctx context.Context, input domain.DeleteManagedResourceCleanupInput) (domain.Execution[struct{}], error) {
	instanceID := domain.DeleteCleanupWorkflowID(input.FulfillmentID)
	instance, err := w.client.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: instanceID,
	}, w.wfName, input)
	if errors.Is(err, backend.ErrInstanceAlreadyExists) {
		return nil, domain.ErrAlreadyRunning
	}
	if err != nil {
		return nil, fmt.Errorf("create workflow instance: %w", err)
	}

	return &execution[struct{}]{
		client:   w.client,
		instance: instance,
		timeout:  w.timeout,
	}, nil
}

// --- ResumeDeploymentWorkflow ---

type resumeDeploymentWorkflow struct {
	client  *client.Client
	wfName  string
	timeout time.Duration
}

func (w *resumeDeploymentWorkflow) Start(ctx context.Context, input domain.ResumeDeploymentInput, observedGen domain.Generation) (domain.Execution[domain.DeploymentView], error) {
	instanceID := fmt.Sprintf("resume-%s-gen-%d", input.Name, observedGen)
	instance, err := w.client.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: instanceID,
	}, w.wfName, input)
	if errors.Is(err, backend.ErrInstanceAlreadyExists) {
		return nil, domain.ErrAlreadyRunning
	}
	if err != nil {
		return nil, fmt.Errorf("create workflow instance: %w", err)
	}

	return &execution[domain.DeploymentView]{
		client:   w.client,
		instance: instance,
		timeout:  w.timeout,
	}, nil
}

// --- Execution ---

type execution[O any] struct {
	client   *client.Client
	instance *workflow.Instance
	timeout  time.Duration
}

func (e *execution[O]) WorkflowID() string {
	return e.instance.InstanceID
}

func (e *execution[O]) AwaitResult(ctx context.Context) (O, error) {
	return client.GetWorkflowResult[O](ctx, e.client, e.instance, e.timeout)
}

// --- CreateManagedResourceWorkflow ---

type createManagedResourceWorkflow struct {
	client  *client.Client
	wfName  string
	timeout time.Duration
}

func (w *createManagedResourceWorkflow) Start(ctx context.Context, input domain.CreateManagedResourceInput) (domain.Execution[domain.ExtensionResourceView], error) {
	fullName := input.ResourceType.FullName(input.Name)
	instanceID := domain.CreateManagedResourceWorkflowID(fullName)
	instance, err := w.client.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: instanceID,
	}, w.wfName, input)
	if err != nil {
		return nil, fmt.Errorf("create workflow instance: %w", err)
	}

	return &execution[domain.ExtensionResourceView]{
		client:   w.client,
		instance: instance,
		timeout:  w.timeout,
	}, nil
}

// --- DeleteManagedResourceWorkflow ---

type deleteManagedResourceWorkflow struct {
	client  *client.Client
	wfName  string
	timeout time.Duration
}

func (w *deleteManagedResourceWorkflow) Start(ctx context.Context, input domain.DeleteManagedResourceInput) (domain.Execution[domain.ExtensionResourceView], error) {
	fullName := input.ResourceType.FullName(input.Name)
	instanceID := domain.DeleteManagedResourceWorkflowID(fullName)
	instance, err := w.client.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: instanceID,
	}, w.wfName, input)
	if err != nil {
		return nil, fmt.Errorf("create workflow instance: %w", err)
	}

	return &execution[domain.ExtensionResourceView]{
		client:   w.client,
		instance: instance,
		timeout:  w.timeout,
	}, nil
}

// --- ResumeManagedResourceWorkflow ---

type resumeManagedResourceWorkflow struct {
	client  *client.Client
	wfName  string
	timeout time.Duration
}

func (w *resumeManagedResourceWorkflow) Start(ctx context.Context, input domain.ResumeManagedResourceInput, observedGen domain.Generation) (domain.Execution[domain.ExtensionResourceView], error) {
	instanceID := fmt.Sprintf("resume-mr-%s-%s-gen-%d", input.ResourceType, input.Name, observedGen)
	instance, err := w.client.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: instanceID,
	}, w.wfName, input)
	if errors.Is(err, backend.ErrInstanceAlreadyExists) {
		return nil, domain.ErrAlreadyRunning
	}
	if err != nil {
		return nil, fmt.Errorf("create workflow instance: %w", err)
	}

	return &execution[domain.ExtensionResourceView]{
		client:   w.client,
		instance: instance,
		timeout:  w.timeout,
	}, nil
}

// Compile-time interface checks.
var (
	_ domain.Registry                             = (*Registry)(nil)
	_ domain.OrchestrationWorkflow                = (*orchestrationWorkflow)(nil)
	_ domain.CreateDeploymentWorkflow             = (*createDeploymentWorkflow)(nil)
	_ domain.DeleteDeploymentWorkflow             = (*deleteDeploymentWorkflow)(nil)
	_ domain.DeleteDeploymentCleanupWorkflow      = (*deleteDeploymentCleanupWorkflow)(nil)
	_ domain.DeleteManagedResourceCleanupWorkflow = (*deleteManagedResourceCleanupWorkflow)(nil)
	_ domain.ResumeDeploymentWorkflow             = (*resumeDeploymentWorkflow)(nil)
	_ domain.ProvisionIdPWorkflow                 = (*provisionIdPWorkflow)(nil)
	_ domain.CreateManagedResourceWorkflow        = (*createManagedResourceWorkflow)(nil)
	_ domain.DeleteManagedResourceWorkflow        = (*deleteManagedResourceWorkflow)(nil)
	_ domain.ResumeManagedResourceWorkflow        = (*resumeManagedResourceWorkflow)(nil)
)
