package domain

import (
	"context"
	"fmt"
	"time"
)

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
	// internal execution context (e.g. workflow.Context in go-workflows)
	// rather than a context.Context parameter. This avoids accidentally
	// carrying request-scoped cancellation into a durable await — the
	// engine controls the lifecycle, not the caller.
	Await(signalName string) (any, error)

	// Sleep durably pauses the workflow for at least the given duration.
	// After replay the engine fast-forwards past completed sleeps.
	Sleep(d time.Duration) error
}

// Signal is a named, typed channel for cross-workflow communication.
// Created once as a package-level variable and shared between send
// ([Registry.SignalFulfillmentEvent]) and receive ([AwaitSignal]) sides.
type Signal[T any] struct {
	Name string
}

// FulfillmentEventSignal is the signal used for delivery-completion
// and lifecycle events sent to orchestration workflows.
var FulfillmentEventSignal = Signal[FulfillmentEvent]{Name: "fulfillment-event"}

// DeleteCleanupCompleteSignal is sent by orchestration to a
// fulfillment-scoped cleanup workflow after delivery data has been
// cleaned up, indicating the cleanup workflow may hard-delete its
// abstraction-specific rows.
var DeleteCleanupCompleteSignal = Signal[DeleteCleanupCompleteEvent]{Name: "delete-cleanup-complete"}

// DeleteCleanupCompleteEvent carries the fulfillment ID whose delivery
// data has been cleaned up by orchestration.
type DeleteCleanupCompleteEvent struct {
	FulfillmentID FulfillmentID
}

// DeleteDeploymentCleanupInput identifies the deployment and
// fulfillment rows that the [DeleteDeploymentCleanupWorkflow] will
// hard-delete after receiving a [DeleteCleanupCompleteSignal].
type DeleteDeploymentCleanupInput struct {
	DeploymentID  DeploymentID
	FulfillmentID FulfillmentID
}

// DeleteCleanupWorkflowID returns the deterministic workflow instance
// ID shared by abstraction-specific cleanup workflows for a single
// fulfillment. Only one cleanup workflow should exist for a given
// fulfillment at a time.
func DeleteCleanupWorkflowID(fulfillmentID FulfillmentID) string {
	return "cleanup-" + string(fulfillmentID)
}

// DeleteManagedResourceCleanupInput identifies the fulfillment row
// that the [DeleteManagedResourceCleanupWorkflow] will hard-delete
// after the managed resource HEAD record has already been removed.
type DeleteManagedResourceCleanupInput struct {
	FulfillmentID FulfillmentID
}

// CreateManagedResourceWorkflowID returns the deterministic workflow
// instance ID for creating a managed resource. It includes the
// resource type so different types may reuse the same leaf name.
func CreateManagedResourceWorkflowID(resourceType ResourceType, name ResourceName) string {
	return fmt.Sprintf("create-mr-%s-%s", resourceType, name)
}

// DeleteManagedResourceWorkflowID returns the deterministic workflow
// instance ID for deleting a managed resource.
func DeleteManagedResourceWorkflowID(resourceType ResourceType, name ResourceName) string {
	return fmt.Sprintf("delete-mr-%s-%s", resourceType, name)
}

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
	RegisterDeleteDeployment(spec *DeleteDeploymentWorkflowSpec) (DeleteDeploymentWorkflow, error)
	RegisterDeleteDeploymentCleanup(spec *DeleteDeploymentCleanupWorkflowSpec) (DeleteDeploymentCleanupWorkflow, error)
	RegisterResumeDeployment(spec *ResumeDeploymentWorkflowSpec) (ResumeDeploymentWorkflow, error)
	RegisterProvisionIdP(spec *ProvisionIdPWorkflowSpec) (ProvisionIdPWorkflow, error)
	RegisterCreateManagedResource(spec *CreateManagedResourceWorkflowSpec) (CreateManagedResourceWorkflow, error)
	RegisterDeleteManagedResource(spec *DeleteManagedResourceWorkflowSpec) (DeleteManagedResourceWorkflow, error)
	RegisterDeleteManagedResourceCleanup(spec *DeleteManagedResourceCleanupWorkflowSpec) (DeleteManagedResourceCleanupWorkflow, error)
	SignalFulfillmentEvent(ctx context.Context, fulfillmentID FulfillmentID, event FulfillmentEvent) error
	SignalDeleteCleanupComplete(ctx context.Context, fulfillmentID FulfillmentID, event DeleteCleanupCompleteEvent) error
}

// OrchestrationWorkflow is a registered orchestration workflow that
// can start new instances. Returned by [Registry.RegisterOrchestration].
//
// If a workflow for the given fulfillment is already active the engine
// may return an [Execution] handle for the running workflow, or an
// [ErrAlreadyRunning] error.
type OrchestrationWorkflow interface {
	Start(ctx context.Context, fulfillmentID FulfillmentID) (Execution[struct{}], error)
}

// CreateDeploymentWorkflow is a registered create-deployment workflow
// that can start new instances. Returned by [Registry.RegisterCreateDeployment].
type CreateDeploymentWorkflow interface {
	Start(ctx context.Context, input CreateDeploymentInput) (Execution[DeploymentView], error)
}

// ProvisionIdPWorkflow is a registered provision-idp workflow that can
// start new instances. Returned by [Registry.RegisterProvisionIdP].
type ProvisionIdPWorkflow interface {
	Start(ctx context.Context, input ProvisionIdPInput) (Execution[AuthMethod], error)
}

// DeleteDeploymentWorkflow is a registered delete-deployment workflow.
// Returned by [Registry.RegisterDeleteDeployment]. The observedGen
// parameter is used by the adapter to derive a generation-qualified
// instance ID for same-type dedup.
type DeleteDeploymentWorkflow interface {
	Start(ctx context.Context, deploymentID DeploymentID, observedGen Generation) (Execution[DeploymentView], error)
}

// DeleteDeploymentCleanupWorkflow is a registered deployment cleanup
// workflow. Returned by [Registry.RegisterDeleteDeploymentCleanup].
// It runs in the background, awaiting a
// [DeleteCleanupCompleteSignal] from orchestration before
// hard-deleting the deployment row and the fulfillment row. The
// instance ID is deterministic: cleanup-{fulfillmentID}.
type DeleteDeploymentCleanupWorkflow interface {
	Start(ctx context.Context, input DeleteDeploymentCleanupInput) (Execution[struct{}], error)
}

// ResumeDeploymentWorkflow is a registered resume-deployment workflow.
// Returned by [Registry.RegisterResumeDeployment]. The observedGen
// parameter is used by the adapter to derive a generation-qualified
// instance ID for same-type dedup.
type ResumeDeploymentWorkflow interface {
	Start(ctx context.Context, input ResumeDeploymentInput, observedGen Generation) (Execution[DeploymentView], error)
}

// CreateManagedResourceWorkflow is a registered create-managed-resource
// workflow. Returned by [Registry.RegisterCreateManagedResource].
type CreateManagedResourceWorkflow interface {
	Start(ctx context.Context, input CreateManagedResourceInput) (Execution[ManagedResourceView], error)
}

// DeleteManagedResourceWorkflow is a registered delete-managed-resource
// workflow. Returned by [Registry.RegisterDeleteManagedResource].
type DeleteManagedResourceWorkflow interface {
	Start(ctx context.Context, input DeleteManagedResourceInput) (Execution[ManagedResourceView], error)
}

// DeleteManagedResourceCleanupWorkflow is a registered managed resource
// cleanup workflow. Returned by
// [Registry.RegisterDeleteManagedResourceCleanup]. It runs in the
// background, awaiting a [DeleteCleanupCompleteSignal] from
// orchestration before hard-deleting the fulfillment row for a
// managed resource whose HEAD record has already been removed. The
// instance ID is deterministic: cleanup-{fulfillmentID}.
type DeleteManagedResourceCleanupWorkflow interface {
	Start(ctx context.Context, input DeleteManagedResourceCleanupInput) (Execution[struct{}], error)
}

// ContinueAsNewError is returned by a workflow body to request that
// the engine restart the workflow with a fresh history and the given
// input. This keeps history bounded for long-running or retrying
// workflows while preserving the same logical workflow instance.
type ContinueAsNewError struct {
	Input any
}

func (e *ContinueAsNewError) Error() string { return "continue as new" }

// ContinueAsNew returns a [ContinueAsNewError] that workflow adapters
// intercept to restart the workflow with the given input.
func ContinueAsNew(input any) error {
	return &ContinueAsNewError{Input: input}
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
