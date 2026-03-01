// Package dbosworkflows implements [domain.WorkflowEngine] using
// the DBOS Transact Go SDK.
package dbosworkflows

import (
	"context"
	"fmt"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// activityInvoker calls RunAsStep with the correct concrete output type.
// Created at construction time when concrete types are known.
type activityInvoker func(ctx dbos.DBOSContext, in any) (any, error)

// Engine implements [domain.WorkflowEngine] backed by DBOS.
//
// The caller must call [dbos.Launch] after creating runners and before
// invoking them.
type Engine struct {
	DBOSCtx dbos.DBOSContext
}

func (e *Engine) OrchestrationRunner(wf *domain.OrchestrationWorkflow) (domain.OrchestrationRunner, error) {
	invokers := make(map[string]activityInvoker)

	registerActivity(invokers, wf.LoadDeployment())
	registerActivity(invokers, wf.LoadTargetPool())
	registerActivity(invokers, wf.ResolvePlacement())
	registerActivity(invokers, wf.PlanRollout())
	registerActivity(invokers, wf.GenerateManifests())
	registerActivity(invokers, wf.DeliverToTarget())
	registerActivity(invokers, wf.RemoveFromTarget())
	registerActivity(invokers, wf.UpdateDeployment())

	wfFunc := func(ctx dbos.DBOSContext, deploymentID domain.DeploymentID) (struct{}, error) {
		runner := &durableRunner{ctx: ctx, invokers: invokers}
		return wf.Run(runner, deploymentID)
	}

	dbos.RegisterWorkflow(e.DBOSCtx, wfFunc, dbos.WithWorkflowName(wf.Name()))

	return &orchestrationRunner{
		dbosCtx: e.DBOSCtx,
		wfFunc:  wfFunc,
	}, nil
}

// registerActivity creates a typed invoker that calls [dbos.RunAsStep]
// with the concrete output type O, ensuring correct JSON deserialization
// during workflow replay.
func registerActivity[I, O any](invokers map[string]activityInvoker, activity domain.Activity[I, O]) {
	invokers[activity.Name()] = func(ctx dbos.DBOSContext, in any) (any, error) {
		return dbos.RunAsStep(ctx, func(stepCtx context.Context) (O, error) {
			return activity.Run(stepCtx, in.(I))
		}, dbos.WithStepName(activity.Name()))
	}
}

type durableRunner struct {
	ctx      dbos.DBOSContext
	invokers map[string]activityInvoker
}

func (r *durableRunner) ID() string {
	id, _ := dbos.GetWorkflowID(r.ctx)
	return id
}

func (r *durableRunner) Context() context.Context {
	return r.ctx
}

func (r *durableRunner) Run(activity domain.Activity[any, any], in any) (any, error) {
	invoke, ok := r.invokers[activity.Name()]
	if !ok {
		return nil, fmt.Errorf("activity %q not registered", activity.Name())
	}
	return invoke(r.ctx, in)
}

type orchestrationRunner struct {
	dbosCtx dbos.DBOSContext
	wfFunc  dbos.Workflow[domain.DeploymentID, struct{}]
}

func (r *orchestrationRunner) Run(ctx context.Context, deploymentID domain.DeploymentID) (domain.WorkflowHandle[struct{}], error) {
	handle, err := dbos.RunWorkflow(r.dbosCtx, r.wfFunc, deploymentID)
	if err != nil {
		return nil, fmt.Errorf("run DBOS workflow: %w", err)
	}
	return &workflowHandle{handle: handle}, nil
}

type workflowHandle struct {
	handle dbos.WorkflowHandle[struct{}]
}

func (h *workflowHandle) WorkflowID() string {
	return h.handle.GetWorkflowID()
}

func (h *workflowHandle) AwaitResult(_ context.Context) (struct{}, error) {
	return h.handle.GetResult()
}
