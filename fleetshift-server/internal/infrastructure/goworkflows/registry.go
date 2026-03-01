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
	"github.com/google/uuid"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

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

func (e *Engine) OrchestrationRunner(wf *domain.OrchestrationWorkflow) (domain.OrchestrationRunner, error) {
	invokers := make(map[string]activityInvoker)

	if err := registerActivity(e.Worker, invokers, wf.LoadDeployment()); err != nil {
		return nil, err
	}
	if err := registerActivity(e.Worker, invokers, wf.LoadTargetPool()); err != nil {
		return nil, err
	}
	if err := registerActivity(e.Worker, invokers, wf.ResolvePlacement()); err != nil {
		return nil, err
	}
	if err := registerActivity(e.Worker, invokers, wf.PlanRollout()); err != nil {
		return nil, err
	}
	if err := registerActivity(e.Worker, invokers, wf.GenerateManifests()); err != nil {
		return nil, err
	}
	if err := registerActivity(e.Worker, invokers, wf.DeliverToTarget()); err != nil {
		return nil, err
	}
	if err := registerActivity(e.Worker, invokers, wf.RemoveFromTarget()); err != nil {
		return nil, err
	}
	if err := registerActivity(e.Worker, invokers, wf.UpdateDeployment()); err != nil {
		return nil, err
	}

	wfFunc := func(ctx workflow.Context, deploymentID domain.DeploymentID) (struct{}, error) {
		runner := &durableRunner{wfCtx: ctx, invokers: invokers}
		return wf.Run(runner, deploymentID)
	}

	if err := e.Worker.RegisterWorkflow(wfFunc, registry.WithName(wf.Name())); err != nil {
		return nil, fmt.Errorf("register workflow %q: %w", wf.Name(), err)
	}

	return &orchestrationRunner{
		client:  e.Client,
		wfName:  wf.Name(),
		timeout: e.timeout(),
	}, nil
}

// registerActivity registers a typed activity with go-workflows and
// creates a corresponding typed invoker.
func registerActivity[I, O any](
	w *worker.Worker,
	invokers map[string]activityInvoker,
	activity domain.Activity[I, O],
) error {
	activityFn := func(ctx context.Context, in I) (O, error) {
		return activity.Run(ctx, in)
	}

	if err := w.RegisterActivity(activityFn, registry.WithName(activity.Name())); err != nil {
		return fmt.Errorf("register activity %q: %w", activity.Name(), err)
	}

	invokers[activity.Name()] = func(wfCtx workflow.Context, in any) (any, error) {
		result, err := workflow.ExecuteActivity[O](
			wfCtx, workflow.DefaultActivityOptions, activity.Name(), in,
		).Get(wfCtx)
		return result, err
	}

	return nil
}

type durableRunner struct {
	wfCtx    workflow.Context
	invokers map[string]activityInvoker
}

func (r *durableRunner) ID() string {
	return workflow.WorkflowInstance(r.wfCtx).InstanceID
}

func (r *durableRunner) Context() context.Context {
	return context.Background()
}

func (r *durableRunner) Run(activity domain.Activity[any, any], in any) (any, error) {
	invoke, ok := r.invokers[activity.Name()]
	if !ok {
		return nil, fmt.Errorf("activity %q not registered", activity.Name())
	}
	return invoke(r.wfCtx, in)
}

type orchestrationRunner struct {
	client  *client.Client
	wfName  string
	timeout time.Duration
}

func (r *orchestrationRunner) Run(ctx context.Context, deploymentID domain.DeploymentID) (domain.WorkflowHandle[struct{}], error) {
	instance, err := r.client.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: uuid.NewString(),
	}, r.wfName, deploymentID)
	if err != nil {
		return nil, fmt.Errorf("create workflow instance: %w", err)
	}

	return &workflowHandle{
		client:   r.client,
		instance: instance,
		timeout:  r.timeout,
	}, nil
}

type workflowHandle struct {
	client   *client.Client
	instance *workflow.Instance
	timeout  time.Duration
}

func (h *workflowHandle) WorkflowID() string {
	return h.instance.InstanceID
}

func (h *workflowHandle) AwaitResult(ctx context.Context) (struct{}, error) {
	return client.GetWorkflowResult[struct{}](ctx, h.client, h.instance, h.timeout)
}
