// Package dbosworkflows implements [domain.WorkflowEngine] using
// the DBOS Transact Go SDK.
package dbosworkflows

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const deploymentEventTopic = "deployment-event"

// activityInvoker calls RunAsStep with the correct concrete output type.
// Created at construction time when concrete types are known.
type activityInvoker func(ctx dbos.DBOSContext, in any) (any, error)

// Engine implements [domain.WorkflowEngine] backed by DBOS.
//
// Launch runs once on first [Register]; the caller must not call
// [dbos.Launch] themselves when using Engine through the interface.
type Engine struct {
	DBOSCtx    dbos.DBOSContext
	launchOnce sync.Once
}

func (e *Engine) Register(owf *domain.OrchestrationWorkflow, cwf *domain.CreateDeploymentWorkflow) (domain.WorkflowRunners, error) {
	// --- orchestration activities & workflow ---

	orchInvokers := make(map[string]activityInvoker)

	registerActivity(orchInvokers, owf.LoadDeploymentAndPool())
	registerActivity(orchInvokers, owf.ResolvePlacement())
	registerActivity(orchInvokers, owf.PlanRollout())
	registerActivity(orchInvokers, owf.GenerateManifests())
	registerActivity(orchInvokers, owf.DeliverToTarget())
	registerActivity(orchInvokers, owf.RemoveFromTarget())
	registerActivity(orchInvokers, owf.UpdateDeployment())

	dbosCtx := e.DBOSCtx
	orchWfFunc := func(ctx dbos.DBOSContext, deploymentID domain.DeploymentID) (struct{}, error) {
		journal := &orchestrationJournal{
			baseJournal: baseJournal{ctx: ctx, invokers: orchInvokers},
			dbosCtx:     dbosCtx,
		}
		return owf.Run(journal, deploymentID)
	}

	dbos.RegisterWorkflow(e.DBOSCtx, orchWfFunc, dbos.WithWorkflowName(owf.Name()))

	// --- create-deployment activities & workflow ---

	createInvokers := make(map[string]activityInvoker)
	registerActivity(createInvokers, cwf.PersistDeployment())

	createWfFunc := func(ctx dbos.DBOSContext, input domain.CreateDeploymentInput) (domain.Deployment, error) {
		journal := &createDeploymentJournal{
			baseJournal: baseJournal{ctx: ctx, invokers: createInvokers},
			orchWfFunc:  orchWfFunc,
		}
		return cwf.Run(journal, input)
	}

	dbos.RegisterWorkflow(e.DBOSCtx, createWfFunc, dbos.WithWorkflowName(cwf.Name()))

	e.launchOnce.Do(func() {
		if err := dbos.Launch(e.DBOSCtx); err != nil {
			panic(err) // Register already succeeded; Launch failure is fatal
		}
	})

	// --- build runners ---

	return domain.WorkflowRunners{
		Orchestration: &orchestrationRunner{
			dbosCtx:    e.DBOSCtx,
			orchWfFunc: orchWfFunc,
		},
		CreateDeployment: &createDeploymentRunner{
			dbosCtx: e.DBOSCtx,
			wfFunc:  createWfFunc,
		},
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

// --- shared base Journal ---

type baseJournal struct {
	ctx      dbos.DBOSContext
	invokers map[string]activityInvoker
}

func (j *baseJournal) ID() string {
	id, _ := dbos.GetWorkflowID(j.ctx)
	return id
}

func (j *baseJournal) Context() context.Context {
	return j.ctx
}

func (j *baseJournal) Run(activity domain.Activity[any, any], in any) (any, error) {
	invoke, ok := j.invokers[activity.Name()]
	if !ok {
		return nil, fmt.Errorf("activity %q not registered", activity.Name())
	}
	return invoke(j.ctx, in)
}

// --- OrchestrationJournal ---

type orchestrationJournal struct {
	baseJournal
	dbosCtx dbos.DBOSContext
}

// AwaitDeploymentEvent receives the next deployment event. It is called
// directly from the workflow body (not inside an activity). DBOS Recv is
// still durable: the runtime checkpoints Recv's result so that on replay
// the workflow gets the same event from the journal instead of re-reading
// the queue. So after a crash we do not lose the event or receive it
// twice. "Exactly once" means the message is consumed when Recv is
// committed, and replay uses the stored result.
func (j *orchestrationJournal) AwaitDeploymentEvent() (domain.DeploymentEvent, error) {
	for {
		event, err := dbos.Recv[domain.DeploymentEvent](j.ctx, deploymentEventTopic, 24*time.Hour)
		if err != nil {
			return domain.DeploymentEvent{}, fmt.Errorf("recv deployment event: %w", err)
		}
		if event != (domain.DeploymentEvent{}) {
			return event, nil
		}
	}
}

func (j *orchestrationJournal) SignalDeploymentEvent(_ context.Context, deploymentID domain.DeploymentID, event domain.DeploymentEvent) error {
	return dbos.Send(j.dbosCtx, string(deploymentID), event, deploymentEventTopic)
}

// --- CreateDeploymentJournal ---

type createDeploymentJournal struct {
	baseJournal
	orchWfFunc dbos.Workflow[domain.DeploymentID, struct{}]
}

func (j *createDeploymentJournal) StartOrchestration(deploymentID domain.DeploymentID) error {
	_, err := dbos.RunWorkflow(j.ctx, j.orchWfFunc, deploymentID, dbos.WithWorkflowID(string(deploymentID)))
	return err
}

// --- OrchestrationRunner (app-facing) ---

type orchestrationRunner struct {
	dbosCtx    dbos.DBOSContext
	orchWfFunc dbos.Workflow[domain.DeploymentID, struct{}]
}

func (r *orchestrationRunner) Run(ctx context.Context, deploymentID domain.DeploymentID) (domain.WorkflowHandle[struct{}], error) {
	handle, err := dbos.RunWorkflow(r.dbosCtx, r.orchWfFunc, deploymentID, dbos.WithWorkflowID(string(deploymentID)))
	if err != nil {
		return nil, fmt.Errorf("run DBOS workflow: %w", err)
	}
	return &orchWorkflowHandle{handle: handle}, nil
}

// --- CreateDeploymentRunner (app-facing) ---

type createDeploymentRunner struct {
	dbosCtx dbos.DBOSContext
	wfFunc  dbos.Workflow[domain.CreateDeploymentInput, domain.Deployment]
}

func (r *createDeploymentRunner) Run(_ context.Context, input domain.CreateDeploymentInput) (domain.WorkflowHandle[domain.Deployment], error) {
	handle, err := dbos.RunWorkflow(r.dbosCtx, r.wfFunc, input, dbos.WithWorkflowID("create-"+string(input.ID)))
	if err != nil {
		return nil, fmt.Errorf("run DBOS create-deployment workflow: %w", err)
	}
	return &createWorkflowHandle{handle: handle}, nil
}

// --- Workflow handles ---

type orchWorkflowHandle struct {
	handle dbos.WorkflowHandle[struct{}]
}

func (h *orchWorkflowHandle) WorkflowID() string { return h.handle.GetWorkflowID() }
func (h *orchWorkflowHandle) AwaitResult(_ context.Context) (struct{}, error) {
	return h.handle.GetResult()
}

type createWorkflowHandle struct {
	handle dbos.WorkflowHandle[domain.Deployment]
}

func (h *createWorkflowHandle) WorkflowID() string { return h.handle.GetWorkflowID() }
func (h *createWorkflowHandle) AwaitResult(_ context.Context) (domain.Deployment, error) {
	return h.handle.GetResult()
}
