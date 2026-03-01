// Package syncworkflow provides a synchronous, in-process [domain.WorkflowEngine].
// Activities execute inline with no persistence or replay.
package syncworkflow

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

var runCounter atomic.Int64

// Engine implements [domain.WorkflowEngine] with synchronous, in-process
// execution. No durable state is kept.
type Engine struct{}

func (e *Engine) OrchestrationRunner(wf *domain.OrchestrationWorkflow) (domain.OrchestrationRunner, error) {
	return &runner{wf: wf}, nil
}

type runner struct {
	wf *domain.OrchestrationWorkflow
}

func (r *runner) Run(ctx context.Context, deploymentID domain.DeploymentID) (domain.WorkflowHandle[struct{}], error) {
	id := runCounter.Add(1)
	dr := &syncRunner{id: id, ctx: ctx}
	result, err := r.wf.Run(dr, deploymentID)
	return &handle{id: id, result: result, err: err}, nil
}

type syncRunner struct {
	id  int64
	ctx context.Context
}

func (r *syncRunner) ID() string              { return fmt.Sprintf("sync-%d", r.id) }
func (r *syncRunner) Context() context.Context { return r.ctx }
func (r *syncRunner) Run(activity domain.Activity[any, any], in any) (any, error) {
	return activity.Run(r.ctx, in)
}

type handle struct {
	id     int64
	result struct{}
	err    error
}

func (h *handle) WorkflowID() string                      { return fmt.Sprintf("sync-%d", h.id) }
func (h *handle) AwaitResult(_ context.Context) (struct{}, error) { return h.result, h.err }
