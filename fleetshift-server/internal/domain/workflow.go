package domain

import "context"

// Activity is a named, typed, idempotent operation. Implementations must
// be safe for at-least-once invocation.
type Activity[I any, O any] interface {
	Name() string
	Run(ctx context.Context, in I) (O, error)
}

// DurableRunner is the capability object provided to a running workflow.
// It durably runs activities and provides a context for pure operations
// that need cancellation propagation.
type DurableRunner interface {
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
// a workflow body. It is a thin wrapper around [DurableRunner.Run].
func RunActivity[I any, O any](runner DurableRunner, activity Activity[I, O], in I) (O, error) {
	result, err := runner.Run(&activityAdapter[I, O]{activity: activity}, in)
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

// OrchestrationRunner starts and awaits orchestration workflows.
type OrchestrationRunner interface {
	Run(ctx context.Context, deploymentID DeploymentID) (WorkflowHandle[struct{}], error)
}

// WorkflowEngine creates runners for the workflow types known to the
// domain. Infrastructure packages provide engine-specific implementations.
type WorkflowEngine interface {
	OrchestrationRunner(wf *OrchestrationWorkflow) (OrchestrationRunner, error)
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
// [DurableRunner.Run] interface.
type activityAdapter[I any, O any] struct{ activity Activity[I, O] }

func (a *activityAdapter[I, O]) Name() string { return a.activity.Name() }
func (a *activityAdapter[I, O]) Run(ctx context.Context, in any) (any, error) {
	return a.activity.Run(ctx, in.(I))
}
