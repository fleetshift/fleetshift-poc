// Package dbosworkflows implements [domain.Registry] using
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

// activityInvoker calls RunAsStep with the correct concrete output type.
// Created at construction time when concrete types are known.
type activityInvoker func(ctx dbos.DBOSContext, in any) (any, error)

// Registry implements [domain.Registry] backed by DBOS.
//
// Launch runs once on first registration; the caller must not call
// [dbos.Launch] themselves when using Registry through the interface.
type Registry struct {
	DBOSCtx    dbos.DBOSContext
	launchOnce sync.Once

	orchWfFunc dbos.Workflow[domain.DeploymentID, struct{}]
}

func (r *Registry) SignalDeploymentEvent(ctx context.Context, deploymentID domain.DeploymentID, event domain.DeploymentEvent) error {
	return dbos.Send(r.DBOSCtx, string(deploymentID), event, "deployment-event")
}

func (r *Registry) RegisterOrchestration(spec *domain.OrchestrationWorkflowSpec) (domain.OrchestrationWorkflow, error) {
	invokers := make(map[string]activityInvoker)

	registerActivity(invokers, spec.LoadDeploymentAndPool())
	registerActivity(invokers, spec.ResolvePlacement())
	registerActivity(invokers, spec.PlanRollout())
	registerActivity(invokers, spec.GenerateManifests())
	registerActivity(invokers, spec.DeliverToTarget())
	registerActivity(invokers, spec.RemoveFromTarget())
	registerActivity(invokers, spec.PersistReconciliationResult())
	registerActivity(invokers, spec.ProcessDeliveryOutputs())
	registerActivity(invokers, spec.CheckGeneration())
	registerActivity(invokers, spec.CompleteReconciliation())
	registerActivity(invokers, spec.DeleteDeploymentRecord())
	registerActivity(invokers, spec.CleanupProvisionedTargets())

	orchWfFunc := func(ctx dbos.DBOSContext, deploymentID domain.DeploymentID) (struct{}, error) {
		record := &baseRecord{
			ctx:      ctx,
			invokers: invokers,
			signals: map[string]func() (any, error){
				domain.DeploymentEventSignal.Name: func() (any, error) {
					for {
						event, err := dbos.Recv[domain.DeploymentEvent](ctx, domain.DeploymentEventSignal.Name, 24*time.Hour)
						if err != nil {
							return nil, fmt.Errorf("recv %q: %w", domain.DeploymentEventSignal.Name, err)
						}
						if event != (domain.DeploymentEvent{}) {
							return event, nil
						}
					}
				},
			},
		}
		return spec.Run(record, deploymentID)
	}
	r.orchWfFunc = orchWfFunc

	dbos.RegisterWorkflow(r.DBOSCtx, orchWfFunc, dbos.WithWorkflowName(spec.Name()), dbos.WithMaxRetries(-1))

	return &orchestrationWorkflow{
		dbosCtx:    r.DBOSCtx,
		orchWfFunc: orchWfFunc,
	}, nil
}

func (r *Registry) RegisterCreateDeployment(spec *domain.CreateDeploymentWorkflowSpec) (domain.CreateDeploymentWorkflow, error) {
	invokers := make(map[string]activityInvoker)
	registerActivity(invokers, spec.PersistDeployment())
	registerActivity(invokers, spec.StartOrchestration())

	createWfFunc := func(ctx dbos.DBOSContext, input domain.CreateDeploymentInput) (domain.Deployment, error) {
		record := &baseRecord{ctx: ctx, invokers: invokers, signals: nil}
		return spec.Run(record, input)
	}

	dbos.RegisterWorkflow(r.DBOSCtx, createWfFunc, dbos.WithWorkflowName(spec.Name()))

	return &createDeploymentWorkflow{
		dbosCtx: r.DBOSCtx,
		wfFunc:  createWfFunc,
	}, nil
}

func (r *Registry) RegisterProvisionIdP(spec *domain.ProvisionIdPWorkflowSpec) (domain.ProvisionIdPWorkflow, error) {
	invokers := make(map[string]activityInvoker)
	registerActivity(invokers, spec.ResolveAndPersist())
	registerActivity(invokers, spec.DeployTrustBundle())

	provWfFunc := func(ctx dbos.DBOSContext, input domain.ProvisionIdPInput) (domain.AuthMethod, error) {
		record := &baseRecord{ctx: ctx, invokers: invokers, signals: nil}
		return spec.Run(record, input)
	}

	dbos.RegisterWorkflow(r.DBOSCtx, provWfFunc, dbos.WithWorkflowName(spec.Name()))

	r.launchOnce.Do(func() {
		if err := dbos.Launch(r.DBOSCtx); err != nil {
			panic(err)
		}
	})

	return &provisionIdPWorkflow{
		dbosCtx: r.DBOSCtx,
		wfFunc:  provWfFunc,
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

// --- shared base Record ---

type baseRecord struct {
	ctx      dbos.DBOSContext
	invokers map[string]activityInvoker
	signals  map[string]func() (any, error)
}

func (r *baseRecord) ID() string {
	id, _ := dbos.GetWorkflowID(r.ctx)
	return id
}

func (r *baseRecord) Context() context.Context {
	return r.ctx
}

func (r *baseRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	invoke, ok := r.invokers[activity.Name()]
	if !ok {
		return nil, fmt.Errorf("activity %q not registered", activity.Name())
	}
	return invoke(r.ctx, in)
}

func (r *baseRecord) Await(signalName string) (any, error) {
	recv, ok := r.signals[signalName]
	if !ok {
		return nil, fmt.Errorf("signal %q not registered", signalName)
	}
	return recv()
}

// --- OrchestrationWorkflow ---

type orchestrationWorkflow struct {
	dbosCtx    dbos.DBOSContext
	orchWfFunc dbos.Workflow[domain.DeploymentID, struct{}]
}

func (w *orchestrationWorkflow) Start(ctx context.Context, deploymentID domain.DeploymentID) (domain.Execution[struct{}], error) {
	// DBOS uses the workflow ID for idempotency: calling RunWorkflow
	// with an already-active ID returns a handle to the existing
	// instance rather than starting a second one. This means Start
	// returns (handle, nil) instead of ErrAlreadyRunning, which the
	// service layer's Reconcile handles correctly.
	handle, err := dbos.RunWorkflow(w.dbosCtx, w.orchWfFunc, deploymentID, dbos.WithWorkflowID(string(deploymentID)))
	if err != nil {
		return nil, fmt.Errorf("run DBOS workflow: %w", err)
	}
	return &orchExecution{handle: handle}, nil
}

// --- CreateDeploymentWorkflow ---

type createDeploymentWorkflow struct {
	dbosCtx dbos.DBOSContext
	wfFunc  dbos.Workflow[domain.CreateDeploymentInput, domain.Deployment]
}

func (w *createDeploymentWorkflow) Start(ctx context.Context, input domain.CreateDeploymentInput) (domain.Execution[domain.Deployment], error) {
	handle, err := dbos.RunWorkflow(w.dbosCtx, w.wfFunc, input, dbos.WithWorkflowID("create-"+string(input.ID)))
	if err != nil {
		return nil, fmt.Errorf("run DBOS create-deployment workflow: %w", err)
	}
	return &createExecution{handle: handle}, nil
}

// --- ProvisionIdPWorkflow ---

type provisionIdPWorkflow struct {
	dbosCtx dbos.DBOSContext
	wfFunc  dbos.Workflow[domain.ProvisionIdPInput, domain.AuthMethod]
}

func (w *provisionIdPWorkflow) Start(ctx context.Context, input domain.ProvisionIdPInput) (domain.Execution[domain.AuthMethod], error) {
	handle, err := dbos.RunWorkflow(w.dbosCtx, w.wfFunc, input, dbos.WithWorkflowID("provision-idp-"+string(input.AuthMethodID)))
	if err != nil {
		return nil, fmt.Errorf("run DBOS provision-idp workflow: %w", err)
	}
	return &provisionExecution{handle: handle}, nil
}

// --- Execution types ---

type orchExecution struct {
	handle dbos.WorkflowHandle[struct{}]
}

func (e *orchExecution) WorkflowID() string { return e.handle.GetWorkflowID() }
func (e *orchExecution) AwaitResult(_ context.Context) (struct{}, error) {
	return e.handle.GetResult()
}

type createExecution struct {
	handle dbos.WorkflowHandle[domain.Deployment]
}

func (e *createExecution) WorkflowID() string { return e.handle.GetWorkflowID() }
func (e *createExecution) AwaitResult(_ context.Context) (domain.Deployment, error) {
	return e.handle.GetResult()
}

type provisionExecution struct {
	handle dbos.WorkflowHandle[domain.AuthMethod]
}

func (e *provisionExecution) WorkflowID() string { return e.handle.GetWorkflowID() }
func (e *provisionExecution) AwaitResult(_ context.Context) (domain.AuthMethod, error) {
	return e.handle.GetResult()
}

// Compile-time interface checks.
var (
	_ domain.Registry                 = (*Registry)(nil)
	_ domain.OrchestrationWorkflow    = (*orchestrationWorkflow)(nil)
	_ domain.CreateDeploymentWorkflow = (*createDeploymentWorkflow)(nil)
	_ domain.ProvisionIdPWorkflow     = (*provisionIdPWorkflow)(nil)
)
