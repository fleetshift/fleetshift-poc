package domain

import "context"

// DeleteObserver is called at key points during delete and
// delete-cleanup workflows. Each method corresponds to a workflow or
// activity entry point and returns a short-lived probe for that
// operation.
// Implementations should embed [NoOpDeleteObserver] for forward
// compatibility with new methods added to this interface.
//
// The observer serves two layers of probes:
//   - Workflow-level probes ([DeleteProbe], [DeleteCleanupProbe]) are
//     created in the deterministic workflow body and observe control flow.
//   - Activity-level probes ([MutateDeploymentProbe],
//     [MutateManagedResourceProbe]) are created inside activity closures
//     and observe I/O-bearing work. They cannot cross the
//     workflow/activity serialization boundary.
type DeleteObserver interface {
	// DeleteDeploymentStarted is called when a
	// [DeleteDeploymentWorkflowSpec] run begins.
	DeleteDeploymentStarted(ctx context.Context, name ResourceName) (context.Context, DeleteProbe)

	// DeleteManagedResourceStarted is called when a
	// [DeleteManagedResourceWorkflowSpec] run begins.
	DeleteManagedResourceStarted(ctx context.Context, resourceType ResourceType, name ResourceName) (context.Context, DeleteProbe)

	// DeploymentCleanupStarted is called when a
	// [DeleteDeploymentCleanupWorkflowSpec] run begins.
	DeploymentCleanupStarted(ctx context.Context, input DeleteDeploymentCleanupInput) (context.Context, DeleteCleanupProbe)

	// ManagedResourceCleanupStarted is called when a
	// [DeleteManagedResourceCleanupWorkflowSpec] run begins.
	ManagedResourceCleanupStarted(ctx context.Context, input DeleteManagedResourceCleanupInput) (context.Context, DeleteCleanupProbe)

	// MutateDeploymentStarted is called at the start of the deployment
	// mutate-to-deleting activity.
	MutateDeploymentStarted(ctx context.Context, name ResourceName) (context.Context, MutateDeploymentProbe)

	// MutateManagedResourceStarted is called at the start of the
	// managed-resource mutate-to-deleting activity.
	MutateManagedResourceStarted(ctx context.Context, resourceType ResourceType, name ResourceName) (context.Context, MutateManagedResourceProbe)
}

// ---------------------------------------------------------------------------
// Workflow-level probes
// ---------------------------------------------------------------------------

// DeleteProbe tracks a single delete-deployment or
// delete-managed-resource workflow run. Implementations should embed
// [NoOpDeleteProbe] for forward compatibility.
type DeleteProbe interface {
	// Mutated is called after the fulfillment is successfully
	// transitioned to [FulfillmentStateDeleting].
	Mutated(fulfillmentID FulfillmentID, generation Generation)

	// CleanupStarted is called after the background cleanup workflow is
	// started.
	CleanupStarted()

	// Error is called when an error occurs.
	Error(err error)

	// End signals the operation is complete (for timing). Called via defer.
	End()
}

// DeleteCleanupProbe tracks a single deployment or managed-resource
// cleanup workflow run. Implementations should embed
// [NoOpDeleteCleanupProbe] for forward compatibility.
type DeleteCleanupProbe interface {
	// SignalReceived is called after the [DeleteCleanupCompleteSignal]
	// arrives from orchestration.
	SignalReceived()

	// RowsDeleted is called after the abstraction-specific rows and
	// fulfillment row are hard-deleted.
	RowsDeleted()

	// Error is called when an error occurs.
	Error(err error)

	// End signals the operation is complete (for timing). Called via defer.
	End()
}

// ---------------------------------------------------------------------------
// Activity-level probes
// ---------------------------------------------------------------------------

// MutateDeploymentProbe tracks the deployment mutate-to-deleting
// activity. Implementations should embed [NoOpMutateDeploymentProbe]
// for forward compatibility.
type MutateDeploymentProbe interface {
	// Error is called when a fatal error occurs.
	Error(err error)

	// End signals the activity is complete. Called via defer.
	End()
}

// MutateManagedResourceProbe tracks the managed-resource
// mutate-to-deleting activity. Implementations should embed
// [NoOpMutateManagedResourceProbe] for forward compatibility.
type MutateManagedResourceProbe interface {
	// Error is called when a fatal error occurs.
	Error(err error)

	// End signals the activity is complete. Called via defer.
	End()
}

// ---------------------------------------------------------------------------
// NoOp implementations
// ---------------------------------------------------------------------------

// NoOpDeleteObserver is a [DeleteObserver] that returns no-op probes.
type NoOpDeleteObserver struct{}

func (NoOpDeleteObserver) DeleteDeploymentStarted(ctx context.Context, _ ResourceName) (context.Context, DeleteProbe) {
	return ctx, NoOpDeleteProbe{}
}

func (NoOpDeleteObserver) DeleteManagedResourceStarted(ctx context.Context, _ ResourceType, _ ResourceName) (context.Context, DeleteProbe) {
	return ctx, NoOpDeleteProbe{}
}

func (NoOpDeleteObserver) DeploymentCleanupStarted(ctx context.Context, _ DeleteDeploymentCleanupInput) (context.Context, DeleteCleanupProbe) {
	return ctx, NoOpDeleteCleanupProbe{}
}

func (NoOpDeleteObserver) ManagedResourceCleanupStarted(ctx context.Context, _ DeleteManagedResourceCleanupInput) (context.Context, DeleteCleanupProbe) {
	return ctx, NoOpDeleteCleanupProbe{}
}

func (NoOpDeleteObserver) MutateDeploymentStarted(ctx context.Context, _ ResourceName) (context.Context, MutateDeploymentProbe) {
	return ctx, NoOpMutateDeploymentProbe{}
}

func (NoOpDeleteObserver) MutateManagedResourceStarted(ctx context.Context, _ ResourceType, _ ResourceName) (context.Context, MutateManagedResourceProbe) {
	return ctx, NoOpMutateManagedResourceProbe{}
}

// NoOpDeleteProbe is a [DeleteProbe] that discards all calls.
type NoOpDeleteProbe struct{}

func (NoOpDeleteProbe) Mutated(FulfillmentID, Generation) {}
func (NoOpDeleteProbe) CleanupStarted()                   {}
func (NoOpDeleteProbe) Error(error)                       {}
func (NoOpDeleteProbe) End()                              {}

// NoOpDeleteCleanupProbe is a [DeleteCleanupProbe] that discards all calls.
type NoOpDeleteCleanupProbe struct{}

func (NoOpDeleteCleanupProbe) SignalReceived() {}
func (NoOpDeleteCleanupProbe) RowsDeleted()    {}
func (NoOpDeleteCleanupProbe) Error(error)     {}
func (NoOpDeleteCleanupProbe) End()            {}

// NoOpMutateDeploymentProbe is a [MutateDeploymentProbe] that discards
// all calls.
type NoOpMutateDeploymentProbe struct{}

func (NoOpMutateDeploymentProbe) Error(error) {}
func (NoOpMutateDeploymentProbe) End()        {}

// NoOpMutateManagedResourceProbe is a [MutateManagedResourceProbe] that
// discards all calls.
type NoOpMutateManagedResourceProbe struct{}

func (NoOpMutateManagedResourceProbe) Error(error) {}
func (NoOpMutateManagedResourceProbe) End()        {}
