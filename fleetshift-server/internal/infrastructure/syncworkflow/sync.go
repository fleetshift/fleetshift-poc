// Package syncworkflow provides a synchronous, in-process implementation
// of the durable workflow port. Steps execute inline with no persistence
// or replay. This is the starting backend; go-workflows replaces it later.
package syncworkflow

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

var runCounter atomic.Int64

// Registry implements [domain.WorkflowRegistry] with synchronous execution.
type Registry struct{}

func (r *Registry) RegisterWorkflow(wf domain.WorkflowDefinition[any, any]) (domain.RegisteredWorkflow[any, any], error) {
	return &registeredWorkflow{wf: wf}, nil
}

type registeredWorkflow struct {
	wf domain.WorkflowDefinition[any, any]
}

func (rw *registeredWorkflow) Name() string { return rw.wf.Name() }

func (rw *registeredWorkflow) Run(ctx context.Context, in any) (domain.WorkflowHandle[any], error) {
	id := runCounter.Add(1)
	run := &syncRun{id: id, ctx: ctx}
	result, err := rw.wf.Run(run, in)
	return &syncHandle{id: id, result: result, err: err}, nil
}

type syncRun struct {
	id  int64
	ctx context.Context
}

func (r *syncRun) RunID() string {
	return fmt.Sprintf("sync-%d", r.id)
}

func (r *syncRun) ExecuteStep(ctx context.Context, step domain.WorkflowStep[any, any], in any) (any, error) {
	return step.Run(ctx, in)
}

type syncHandle struct {
	id     int64
	result any
	err    error
}

func (h *syncHandle) WorkflowID() string {
	return fmt.Sprintf("sync-%d", h.id)
}

func (h *syncHandle) Get(_ context.Context) (any, error) {
	return h.result, h.err
}
