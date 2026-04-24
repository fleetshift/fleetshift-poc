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

// activityOptions overrides go-workflows' DefaultActivityOptions with
// aggressive retries so that transient failures (network blips,
// database contention) do not permanently fail a reconciliation.
var activityOptions = workflow.ActivityOptions{
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
	Worker  *worker.Worker
	Client  *client.Client
	Timeout time.Duration
}

func (r *Registry) timeout() time.Duration {
	if r.Timeout > 0 {
		return r.Timeout
	}
	return 30 * time.Second
}

func (r *Registry) SignalDeploymentEvent(ctx context.Context, deploymentID domain.DeploymentID, event domain.DeploymentEvent) error {
	return r.Client.SignalWorkflow(ctx, string(deploymentID), domain.DeploymentEventSignal.Name, event)
}

func (r *Registry) RegisterOrchestration(spec *domain.OrchestrationWorkflowSpec) (domain.OrchestrationWorkflow, error) {
	invokers := make(map[string]activityInvoker)

	for _, reg := range []func() error{
		func() error { return registerActivity(r.Worker, invokers, spec.LoadDeploymentAndPool()) },
		func() error { return registerActivity(r.Worker, invokers, spec.ResolvePlacement()) },
		func() error { return registerActivity(r.Worker, invokers, spec.PlanRollout()) },
		func() error { return registerActivity(r.Worker, invokers, spec.GenerateManifests()) },
		func() error { return registerActivity(r.Worker, invokers, spec.DeliverToTarget()) },
		func() error { return registerActivity(r.Worker, invokers, spec.RemoveFromTarget()) },
		func() error { return registerActivity(r.Worker, invokers, spec.PersistReconciliationResult()) },
		func() error { return registerActivity(r.Worker, invokers, spec.ProcessDeliveryOutputs()) },
		func() error { return registerActivity(r.Worker, invokers, spec.CheckGeneration()) },
		func() error { return registerActivity(r.Worker, invokers, spec.CompleteReconciliation()) },
		func() error { return registerActivity(r.Worker, invokers, spec.DeleteDeploymentRecord()) },
		func() error { return registerActivity(r.Worker, invokers, spec.CleanupProvisionedTargets()) },
		func() error { return registerActivity(r.Worker, invokers, spec.AcquireLock()) },
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

	wfFunc := func(ctx workflow.Context, deploymentID domain.DeploymentID) (struct{}, error) {
		ch := workflow.NewSignalChannel[domain.DeploymentEvent](ctx, domain.DeploymentEventSignal.Name)
		record := &baseRecord{
			wfCtx:    ctx,
			invokers: invokers,
			signals: map[string]func() (any, error){
				domain.DeploymentEventSignal.Name: func() (any, error) {
					val, ok := ch.Receive(ctx)
					if !ok {
						return nil, fmt.Errorf("signal channel %q closed", domain.DeploymentEventSignal.Name)
					}
					return val, nil
				},
			},
		}
		return spec.Run(record, deploymentID)
	}

	if err := r.Worker.RegisterWorkflow(wfFunc, goregistry.WithName(spec.Name())); err != nil {
		return nil, fmt.Errorf("register workflow %q: %w", spec.Name(), err)
	}

	return ow, nil
}

func (r *Registry) RegisterCreateDeployment(spec *domain.CreateDeploymentWorkflowSpec) (domain.CreateDeploymentWorkflow, error) {
	invokers := make(map[string]activityInvoker)

	if err := registerActivity(r.Worker, invokers, spec.PersistDeployment()); err != nil {
		return nil, err
	}
	if err := registerActivity(r.Worker, invokers, spec.StartOrchestration()); err != nil {
		return nil, err
	}

	wfFunc := func(ctx workflow.Context, input domain.CreateDeploymentInput) (domain.Deployment, error) {
		record := &baseRecord{wfCtx: ctx, invokers: invokers, signals: nil}
		return spec.Run(record, input)
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

	if err := registerActivity(r.Worker, invokers, spec.MutateToDeleting()); err != nil {
		return nil, err
	}
	if err := registerActivity(r.Worker, invokers, spec.LoadDeployment()); err != nil {
		return nil, err
	}

	wfFunc := func(ctx workflow.Context, deploymentID domain.DeploymentID) (domain.Deployment, error) {
		record := &baseRecord{wfCtx: ctx, invokers: invokers, signals: nil}
		return spec.Run(record, deploymentID)
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

func (r *Registry) RegisterResumeDeployment(spec *domain.ResumeDeploymentWorkflowSpec) (domain.ResumeDeploymentWorkflow, error) {
	invokers := make(map[string]activityInvoker)

	if err := registerActivity(r.Worker, invokers, spec.MutateToResumed()); err != nil {
		return nil, err
	}
	if err := registerActivity(r.Worker, invokers, spec.LoadDeployment()); err != nil {
		return nil, err
	}

	wfFunc := func(ctx workflow.Context, input domain.ResumeDeploymentInput) (domain.Deployment, error) {
		record := &baseRecord{wfCtx: ctx, invokers: invokers, signals: nil}
		return spec.Run(record, input)
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

	if err := registerActivity(r.Worker, invokers, spec.ResolveAndPersist()); err != nil {
		return nil, err
	}
	if err := registerActivity(r.Worker, invokers, spec.DeployTrustBundle()); err != nil {
		return nil, err
	}

	wfFunc := func(ctx workflow.Context, input domain.ProvisionIdPInput) (domain.AuthMethod, error) {
		record := &baseRecord{wfCtx: ctx, invokers: invokers, signals: nil}
		return spec.Run(record, input)
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

// registerActivity registers a typed activity with go-workflows and
// creates a corresponding typed invoker.
func registerActivity[I, O any](
	w *worker.Worker,
	invokers map[string]activityInvoker,
	activity domain.Activity[I, O],
) error {
	name := activity.Name()

	activityFn := func(ctx context.Context, in I) (O, error) {
		return activity.Run(ctx, in)
	}
	if err := w.RegisterActivity(activityFn, goregistry.WithName(name)); err != nil {
		return fmt.Errorf("register activity %q: %w", name, err)
	}

	invokers[name] = func(wfCtx workflow.Context, in any) (any, error) {
		result, err := workflow.ExecuteActivity[O](
			wfCtx, activityOptions, name, in,
		).Get(wfCtx)
		return result, err
	}

	return nil
}

// --- shared base Record ---

type baseRecord struct {
	wfCtx    workflow.Context
	invokers map[string]activityInvoker
	signals  map[string]func() (any, error)
}

func (r *baseRecord) ID() string {
	return workflow.WorkflowInstance(r.wfCtx).InstanceID
}

func (r *baseRecord) Context() context.Context {
	return context.Background()
}

func (r *baseRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	invoke, ok := r.invokers[activity.Name()]
	if !ok {
		return nil, fmt.Errorf("activity %q not registered", activity.Name())
	}
	return invoke(r.wfCtx, in)
}

func (r *baseRecord) Await(signalName string) (any, error) {
	recv, ok := r.signals[signalName]
	if !ok {
		return nil, fmt.Errorf("signal %q not registered", signalName)
	}
	return recv()
}

func (r *baseRecord) Sleep(d time.Duration) error {
	return workflow.Sleep(r.wfCtx, d)
}

// --- OrchestrationWorkflow ---

type orchestrationWorkflow struct {
	client  *client.Client
	wfName  string
	timeout time.Duration
}

func (w *orchestrationWorkflow) Start(ctx context.Context, deploymentID domain.DeploymentID) (domain.Execution[struct{}], error) {
	instance, err := w.client.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: string(deploymentID),
	}, w.wfName, deploymentID)
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

func (w *createDeploymentWorkflow) Start(ctx context.Context, input domain.CreateDeploymentInput) (domain.Execution[domain.Deployment], error) {
	instance, err := w.client.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: "create-" + string(input.ID),
	}, w.wfName, input)
	if err != nil {
		return nil, fmt.Errorf("create workflow instance: %w", err)
	}

	return &execution[domain.Deployment]{
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

func (w *deleteDeploymentWorkflow) Start(ctx context.Context, deploymentID domain.DeploymentID, observedGen domain.Generation) (domain.Execution[domain.Deployment], error) {
	instanceID := fmt.Sprintf("delete-%s-gen-%d", deploymentID, observedGen)
	instance, err := w.client.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: instanceID,
	}, w.wfName, deploymentID)
	if errors.Is(err, backend.ErrInstanceAlreadyExists) {
		return nil, domain.ErrConcurrentUpdate
	}
	if err != nil {
		return nil, fmt.Errorf("create workflow instance: %w", err)
	}

	return &execution[domain.Deployment]{
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

func (w *resumeDeploymentWorkflow) Start(ctx context.Context, input domain.ResumeDeploymentInput, observedGen domain.Generation) (domain.Execution[domain.Deployment], error) {
	instanceID := fmt.Sprintf("resume-%s-gen-%d", input.ID, observedGen)
	instance, err := w.client.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: instanceID,
	}, w.wfName, input)
	if errors.Is(err, backend.ErrInstanceAlreadyExists) {
		return nil, domain.ErrConcurrentUpdate
	}
	if err != nil {
		return nil, fmt.Errorf("create workflow instance: %w", err)
	}

	return &execution[domain.Deployment]{
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

// Compile-time interface checks.
var (
	_ domain.Registry                  = (*Registry)(nil)
	_ domain.OrchestrationWorkflow     = (*orchestrationWorkflow)(nil)
	_ domain.CreateDeploymentWorkflow  = (*createDeploymentWorkflow)(nil)
	_ domain.DeleteDeploymentWorkflow  = (*deleteDeploymentWorkflow)(nil)
	_ domain.ResumeDeploymentWorkflow  = (*resumeDeploymentWorkflow)(nil)
	_ domain.ProvisionIdPWorkflow      = (*provisionIdPWorkflow)(nil)
)
