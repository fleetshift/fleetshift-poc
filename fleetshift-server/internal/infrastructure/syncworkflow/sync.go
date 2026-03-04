// Package syncworkflow provides a synchronous, in-process [domain.WorkflowEngine].
// Activities execute inline with no persistence or replay. The workflow runs
// in a goroutine and receives [domain.DeploymentEvent] values through a
// buffered channel, so callers must coordinate start and signal.
package syncworkflow

import (
	"context"
	"sync"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Engine implements [domain.WorkflowEngine] with synchronous, in-process
// execution. No durable state is kept. Workflow instances are tracked so
// that event signals can be delivered to the correct goroutine.
type Engine struct {
	mu        sync.Mutex
	instances map[domain.DeploymentID]*instance
}

type instance struct {
	events chan domain.DeploymentEvent
}

func (e *Engine) getInstance(id domain.DeploymentID) *instance {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.instances == nil {
		e.instances = make(map[domain.DeploymentID]*instance)
	}
	inst, ok := e.instances[id]
	if !ok {
		inst = &instance{events: make(chan domain.DeploymentEvent, 16)}
		e.instances[id] = inst
	}
	return inst
}

func (e *Engine) removeInstance(id domain.DeploymentID) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.instances, id)
}

func (e *Engine) Register(owf *domain.OrchestrationWorkflow, cwf *domain.CreateDeploymentWorkflow) (domain.WorkflowRunners, error) {
	orchRunner := &orchestrationRunner{engine: e, wf: owf}

	owf.SignalDeploymentEvent = func(ctx context.Context, id domain.DeploymentID, event domain.DeploymentEvent) error {
		inst := e.getInstance(id)
		select {
		case inst.events <- event:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	cwf.StartOrchestration = domain.NewActivity("start-orchestration", func(ctx context.Context, id domain.DeploymentID) (struct{}, error) {
		_, err := orchRunner.Run(ctx, id)
		return struct{}{}, err
	})

	createRunner := &createDeploymentRunner{
		cwf: cwf,
	}
	return domain.WorkflowRunners{
		Orchestration:    orchRunner,
		CreateDeployment: createRunner,
	}, nil
}

type orchestrationRunner struct {
	engine *Engine
	wf     *domain.OrchestrationWorkflow
}

func (r *orchestrationRunner) Run(ctx context.Context, deploymentID domain.DeploymentID) (domain.WorkflowHandle[struct{}], error) {
	inst := r.engine.getInstance(deploymentID)

	done := make(chan orchResult, 1)

	go func() {
		journal := &baseJournal{id: string(deploymentID), ctx: ctx}
		awaitEvent := func() (domain.DeploymentEvent, error) {
			select {
			case event := <-inst.events:
				return event, nil
			case <-ctx.Done():
				return domain.DeploymentEvent{}, ctx.Err()
			}
		}
		val, err := r.wf.Run(journal, awaitEvent, deploymentID)
		r.engine.removeInstance(deploymentID)
		done <- orchResult{val: val, err: err}
	}()

	return &orchHandle{id: string(deploymentID), done: done}, nil
}

type createDeploymentRunner struct {
	cwf *domain.CreateDeploymentWorkflow
}

func (r *createDeploymentRunner) Run(ctx context.Context, input domain.CreateDeploymentInput) (domain.WorkflowHandle[domain.Deployment], error) {
	done := make(chan createResult, 1)

	go func() {
		journal := &baseJournal{id: "create-" + string(input.ID), ctx: ctx}
		val, err := r.cwf.Run(journal, input)
		done <- createResult{val: val, err: err}
	}()

	return &createHandle{id: "create-" + string(input.ID), done: done}, nil
}

// --- shared base Journal ---

type baseJournal struct {
	id  string
	ctx context.Context
}

func (j *baseJournal) ID() string              { return j.id }
func (j *baseJournal) Context() context.Context { return j.ctx }
func (j *baseJournal) Run(activity domain.Activity[any, any], in any) (any, error) {
	return activity.Run(j.ctx, in)
}

// --- Handles and result types ---

type orchResult struct {
	val struct{}
	err error
}

type orchHandle struct {
	id   string
	done <-chan orchResult
}

func (h *orchHandle) WorkflowID() string { return h.id }
func (h *orchHandle) AwaitResult(ctx context.Context) (struct{}, error) {
	select {
	case r := <-h.done:
		return r.val, r.err
	case <-ctx.Done():
		return struct{}{}, ctx.Err()
	}
}

type createResult struct {
	val domain.Deployment
	err error
}

type createHandle struct {
	id   string
	done <-chan createResult
}

func (h *createHandle) WorkflowID() string { return h.id }
func (h *createHandle) AwaitResult(ctx context.Context) (domain.Deployment, error) {
	select {
	case r := <-h.done:
		return r.val, r.err
	case <-ctx.Done():
		return domain.Deployment{}, ctx.Err()
	}
}
