package domain

import "context"

// Workflow types are organized as follows:
//
//   - workflow.go: shared primitives (Activity, Journal, WorkflowHandle,
//     RunActivity, NewActivity) and the engine contract (WorkflowEngine,
//     WorkflowRunners). No workflow-specific types.
//
//   - Per-workflow file (orchestration.go, create_deployment.go): the workflow
//     struct and its runner. Naming:
//     • XRunner = app-facing starter; Run(ctx, input) returns WorkflowHandle.
//
// Journal is a single universal type used by all workflows. Workflow-specific
// engine capabilities (e.g. signaling, awaiting events, starting child
// workflows) are injected as function fields or Activity fields on the
// workflow struct, or as parameters to Run — not overloaded onto the journal.

// Activity is a named, typed, idempotent operation. Implementations must
// be safe for at-least-once invocation.
type Activity[I any, O any] interface {
	Name() string
	Run(ctx context.Context, in I) (O, error)
}

// Journal is the durable execution journal provided to a running
// workflow. It records activity invocations and their results so
// the engine can replay the workflow deterministically after a crash.
type Journal interface {
	ID() string

	// Context returns the workflow execution context. In a durable
	// engine this is the deterministic replay context; in the
	// synchronous backend it is the caller's context.
	Context() context.Context

	// Run durably runs an activity. The engine provides the activity's
	// context internally; callers should use [RunActivity] for type safety.
	Run(activity Activity[any, any], in any) (any, error)
}

// RunActivity provides type-safe durable activity execution from within
// a workflow body. It is a thin wrapper around [Journal.Run].
func RunActivity[I any, O any](journal Journal, activity Activity[I, O], in I) (O, error) {
	result, err := journal.Run(&activityAdapter[I, O]{activity: activity}, in)
	if err != nil {
		var zero O
		return zero, err
	}
	return result.(O), nil
}

// WorkflowHandle is a handle to a running or completed workflow execution.
type WorkflowHandle[O any] interface {
	WorkflowID() string
	AwaitResult(ctx context.Context) (O, error)
}

// WorkflowRunners holds the runners produced by [WorkflowEngine.Register].
type WorkflowRunners struct {
	Orchestration    OrchestrationRunner
	CreateDeployment CreateDeploymentRunner
}

// WorkflowEngine registers domain workflows with an execution engine
// and returns the runners needed by the application layer.
type WorkflowEngine interface {
	Register(owf *OrchestrationWorkflow, cwf *CreateDeploymentWorkflow) (WorkflowRunners, error)
}

// NewActivity creates an [Activity] from a stable name and a function.
// Workflow types use this to define their activities as methods.
func NewActivity[I, O any](name string, fn func(context.Context, I) (O, error)) Activity[I, O] {
	return &activityFunc[I, O]{name: name, fn: fn}
}

type activityFunc[I, O any] struct {
	name string
	fn   func(context.Context, I) (O, error)
}

func (a *activityFunc[I, O]) Name() string                             { return a.name }
func (a *activityFunc[I, O]) Run(ctx context.Context, in I) (O, error) { return a.fn(ctx, in) }

// activityAdapter bridges a typed [Activity] to the any-typed
// [Journal.Run] interface.
type activityAdapter[I any, O any] struct{ activity Activity[I, O] }

func (a *activityAdapter[I, O]) Name() string { return a.activity.Name() }
func (a *activityAdapter[I, O]) Run(ctx context.Context, in any) (any, error) {
	return a.activity.Run(ctx, in.(I))
}
