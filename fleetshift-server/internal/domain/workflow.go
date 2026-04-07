package domain

import "context"

// Workflow types are organized as follows:
//
//   - workflow.go: shared primitives (Activity, Record, Execution, Signal,
//     RunActivity, AwaitSignal, NewActivity) and the engine contract
//     (Registry, per-workflow interfaces).
//
//   - Per-workflow file (orchestration.go, create_deployment.go): the
//     workflow spec struct. Naming:
//     • XWorkflowSpec = workflow definition with dependencies and Run body.
//     • XWorkflow     = registered workflow (returned by Registry); Start
//       creates an Execution.

// Activity is a named, typed, idempotent operation. Implementations must
// be safe for at-least-once invocation.
type Activity[I any, O any] interface {
	Name() string
	Run(ctx context.Context, in I) (O, error)
}

// Record is the durable execution record provided to a running
// workflow. It records activity invocations and their results so
// the engine can replay the workflow deterministically after a crash.
type Record interface {
	ID() string

	// Context returns the workflow execution context. In a durable
	// engine this is the deterministic replay context; in the
	// synchronous backend it is the caller's context.
	Context() context.Context

	// Run durably runs an activity. The engine provides the activity's
	// context internally; callers should use [RunActivity] for type safety.
	Run(activity Activity[any, any], in any) (any, error)

	// Await blocks until the named signal arrives. It uses the engine's
	// internal execution context (workflow.Context, dbos.DBOSContext, etc.)
	// rather than a context.Context parameter. This avoids accidentally
	// carrying request-scoped cancellation into a durable await — the
	// engine controls the lifecycle, not the caller.
	Await(signalName string) (any, error)
}

// Signal is a named, typed channel for cross-workflow communication.
// Created once as a package-level variable and shared between send
// ([Registry.SignalDeploymentEvent]) and receive ([AwaitSignal]) sides.
type Signal[T any] struct {
	Name string
}

// DeploymentEventSignal is the signal used for delivery-completion
// and lifecycle events sent to orchestration workflows.
var DeploymentEventSignal = Signal[DeploymentEvent]{Name: "deployment-event"}

// RunActivity provides type-safe durable activity execution from within
// a workflow body. It is a thin wrapper around [Record.Run].
func RunActivity[I any, O any](record Record, activity Activity[I, O], in I) (O, error) {
	result, err := record.Run(&activityAdapter[I, O]{activity: activity}, in)
	if err != nil {
		var zero O
		return zero, err
	}
	return result.(O), nil
}

// AwaitSignal provides type-safe signal reception from within a
// workflow body. It is a thin wrapper around [Record.Await],
// mirroring how [RunActivity] wraps [Record.Run].
func AwaitSignal[T any](record Record, sig Signal[T]) (T, error) {
	val, err := record.Await(sig.Name)
	if err != nil {
		var zero T
		return zero, err
	}
	return val.(T), nil
}

// Execution is a handle to a running or completed workflow instance.
type Execution[T any] interface {
	WorkflowID() string
	AwaitResult(ctx context.Context) (T, error)
}

// Registry registers workflow specs and provides cross-workflow
// signaling. Workflow specs receive it at construction so engine
// capabilities are available without lazy field assignment.
type Registry interface {
	RegisterOrchestration(spec *OrchestrationWorkflowSpec) (OrchestrationWorkflow, error)
	RegisterCreateDeployment(spec *CreateDeploymentWorkflowSpec) (CreateDeploymentWorkflow, error)
	RegisterProvisionIdP(spec *ProvisionIdPWorkflowSpec) (ProvisionIdPWorkflow, error)
	SignalDeploymentEvent(ctx context.Context, deploymentID DeploymentID, event DeploymentEvent) error
}

// OrchestrationWorkflow is a registered orchestration workflow that
// can start new instances. Returned by [Registry.RegisterOrchestration].
//
// If a workflow for the given deployment is already active
// the engine may return an [Execution] handle for the running workflow,
// or an [ErrAlreadyRunning] error.
type OrchestrationWorkflow interface {
	Start(ctx context.Context, deploymentID DeploymentID) (Execution[struct{}], error)
}

// CreateDeploymentWorkflow is a registered create-deployment workflow
// that can start new instances. Returned by [Registry.RegisterCreateDeployment].
type CreateDeploymentWorkflow interface {
	Start(ctx context.Context, input CreateDeploymentInput) (Execution[Deployment], error)
}

// ProvisionIdPWorkflow is a registered provision-idp workflow that can
// start new instances. Returned by [Registry.RegisterProvisionIdP].
type ProvisionIdPWorkflow interface {
	Start(ctx context.Context, input ProvisionIdPInput) (Execution[AuthMethod], error)
}

// NewActivity creates an [Activity] from a stable name and a function.
// Workflow spec types use this to define their activities as methods.
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
// [Record.Run] interface.
type activityAdapter[I any, O any] struct{ activity Activity[I, O] }

func (a *activityAdapter[I, O]) Name() string { return a.activity.Name() }
func (a *activityAdapter[I, O]) Run(ctx context.Context, in any) (any, error) {
	return a.activity.Run(ctx, in.(I))
}
