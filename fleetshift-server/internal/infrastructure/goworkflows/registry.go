// Package goworkflows implements [domain.WorkflowEngine] using
// cschleiden/go-workflows for durable workflow execution.
package goworkflows

import (
	"context"
	"fmt"
	"time"

	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/registry"
	"github.com/cschleiden/go-workflows/worker"
	"github.com/cschleiden/go-workflows/workflow"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const deploymentEventSignal = "deployment-event"

// activityInvoker calls an activity from the workflow context with the
// correct generic types. Created at construction time when concrete
// types are known.
type activityInvoker func(wfCtx workflow.Context, in any) (any, error)

// Engine implements [domain.WorkflowEngine] backed by go-workflows.
type Engine struct {
	Worker  *worker.Worker
	Client  *client.Client
	Timeout time.Duration
}

func (e *Engine) timeout() time.Duration {
	if e.Timeout > 0 {
		return e.Timeout
	}
	return 30 * time.Second
}

func (e *Engine) Register(owf *domain.OrchestrationWorkflow, cwf *domain.CreateDeploymentWorkflow) (domain.WorkflowRunners, error) {
	// --- set workflow-specific engine capabilities ---

	owf.SignalDeploymentEvent = func(_ context.Context, deploymentID domain.DeploymentID, event domain.DeploymentEvent) error {
		go e.Client.SignalWorkflow(context.Background(), string(deploymentID), deploymentEventSignal, event)
		return nil
	}

	// go-workflows requires all sub-workflow futures to be awaited
	// before the parent completes, so StartOrchestration is an activity
	// that uses the client to start orchestration as a top-level workflow.
	orchWfName := owf.Name()
	cwf.StartOrchestration = domain.NewActivity("start-orchestration", func(ctx context.Context, deploymentID domain.DeploymentID) (struct{}, error) {
		_, err := e.Client.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
			InstanceID: string(deploymentID),
		}, orchWfName, deploymentID)
		return struct{}{}, err
	})

	// --- orchestration activities & workflow ---

	orchInvokers := make(map[string]activityInvoker)

	for _, reg := range []func() error{
		func() error { return registerActivity(e.Worker, orchInvokers, owf.LoadDeploymentAndPool()) },
		func() error { return registerActivity(e.Worker, orchInvokers, owf.ResolvePlacement()) },
		func() error { return registerActivity(e.Worker, orchInvokers, owf.PlanRollout()) },
		func() error { return registerActivity(e.Worker, orchInvokers, owf.GenerateManifests()) },
		func() error { return registerActivity(e.Worker, orchInvokers, owf.DeliverToTarget()) },
		func() error { return registerActivity(e.Worker, orchInvokers, owf.RemoveFromTarget()) },
		func() error { return registerActivity(e.Worker, orchInvokers, owf.UpdateDeployment()) },
	} {
		if err := reg(); err != nil {
			return domain.WorkflowRunners{}, err
		}
	}

	wfFunc := func(ctx workflow.Context, deploymentID domain.DeploymentID) (struct{}, error) {
		ch := workflow.NewSignalChannel[domain.DeploymentEvent](ctx, deploymentEventSignal)
		journal := &baseJournal{wfCtx: ctx, invokers: orchInvokers}
		awaitEvent := func() (domain.DeploymentEvent, error) {
			event, ok := ch.Receive(ctx)
			if !ok {
				return domain.DeploymentEvent{}, fmt.Errorf("signal channel closed")
			}
			return event, nil
		}
		return owf.Run(journal, awaitEvent, deploymentID)
	}

	if err := e.Worker.RegisterWorkflow(wfFunc, registry.WithName(owf.Name())); err != nil {
		return domain.WorkflowRunners{}, fmt.Errorf("register workflow %q: %w", owf.Name(), err)
	}

	// --- create-deployment activities & workflow ---

	createInvokers := make(map[string]activityInvoker)
	if err := registerActivity(e.Worker, createInvokers, cwf.PersistDeployment()); err != nil {
		return domain.WorkflowRunners{}, err
	}
	if err := registerActivity(e.Worker, createInvokers, cwf.StartOrchestration); err != nil {
		return domain.WorkflowRunners{}, err
	}

	createWfFunc := func(ctx workflow.Context, input domain.CreateDeploymentInput) (domain.Deployment, error) {
		journal := &baseJournal{wfCtx: ctx, invokers: createInvokers}
		return cwf.Run(journal, input)
	}

	if err := e.Worker.RegisterWorkflow(createWfFunc, registry.WithName(cwf.Name())); err != nil {
		return domain.WorkflowRunners{}, fmt.Errorf("register workflow %q: %w", cwf.Name(), err)
	}

	// --- build runners ---

	return domain.WorkflowRunners{
		Orchestration: &orchestrationRunner{
			client:  e.Client,
			wfName:  owf.Name(),
			timeout: e.timeout(),
		},
		CreateDeployment: &createDeploymentRunner{
			client:  e.Client,
			wfName:  cwf.Name(),
			timeout: e.timeout(),
		},
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
	if err := w.RegisterActivity(activityFn, registry.WithName(name)); err != nil {
		return fmt.Errorf("register activity %q: %w", name, err)
	}

	invokers[name] = func(wfCtx workflow.Context, in any) (any, error) {
		result, err := workflow.ExecuteActivity[O](
			wfCtx, workflow.DefaultActivityOptions, name, in,
		).Get(wfCtx)
		return result, err
	}

	return nil
}

// --- shared base Journal ---

type baseJournal struct {
	wfCtx    workflow.Context
	invokers map[string]activityInvoker
}

func (j *baseJournal) ID() string {
	return workflow.WorkflowInstance(j.wfCtx).InstanceID
}

func (j *baseJournal) Context() context.Context {
	return context.Background()
}

func (j *baseJournal) Run(activity domain.Activity[any, any], in any) (any, error) {
	invoke, ok := j.invokers[activity.Name()]
	if !ok {
		return nil, fmt.Errorf("activity %q not registered", activity.Name())
	}
	return invoke(j.wfCtx, in)
}

// --- OrchestrationRunner (app-facing) ---

type orchestrationRunner struct {
	client  *client.Client
	wfName  string
	timeout time.Duration
}

func (r *orchestrationRunner) Run(ctx context.Context, deploymentID domain.DeploymentID) (domain.WorkflowHandle[struct{}], error) {
	instance, err := r.client.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: string(deploymentID),
	}, r.wfName, deploymentID)
	if err != nil {
		return nil, fmt.Errorf("create workflow instance: %w", err)
	}

	return &workflowHandle[struct{}]{
		client:   r.client,
		instance: instance,
		timeout:  r.timeout,
	}, nil
}

// --- CreateDeploymentRunner (app-facing) ---

type createDeploymentRunner struct {
	client  *client.Client
	wfName  string
	timeout time.Duration
}

func (r *createDeploymentRunner) Run(ctx context.Context, input domain.CreateDeploymentInput) (domain.WorkflowHandle[domain.Deployment], error) {
	instance, err := r.client.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: "create-" + string(input.ID),
	}, r.wfName, input)
	if err != nil {
		return nil, fmt.Errorf("create workflow instance: %w", err)
	}

	return &workflowHandle[domain.Deployment]{
		client:   r.client,
		instance: instance,
		timeout:  r.timeout,
	}, nil
}

// --- WorkflowHandle ---

type workflowHandle[O any] struct {
	client   *client.Client
	instance *workflow.Instance
	timeout  time.Duration
}

func (h *workflowHandle[O]) WorkflowID() string {
	return h.instance.InstanceID
}

func (h *workflowHandle[O]) AwaitResult(ctx context.Context) (O, error) {
	return client.GetWorkflowResult[O](ctx, h.client, h.instance, h.timeout)
}
