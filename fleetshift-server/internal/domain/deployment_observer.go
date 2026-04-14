package domain

import "context"

// DeploymentObserver is called at key points during deployment
// orchestration. Implementations should embed [NoOpDeploymentObserver]
// for forward compatibility with new methods added to this interface.
type DeploymentObserver interface {
	// RunStarted is called when the orchestration workflow begins
	// processing a deployment. Returns a potentially modified context
	// and a probe to track the run.
	RunStarted(ctx context.Context, deploymentID DeploymentID) (context.Context, DeploymentRunProbe)
}

// DeploymentRunProbe tracks a single orchestration run.
// Implementations should embed [NoOpDeploymentRunProbe] for forward
// compatibility.
type DeploymentRunProbe interface {
	// EventReceived is called when the workflow receives a deployment event.
	EventReceived(event DeploymentEvent)

	// StateChanged is called when the deployment transitions to a new state.
	StateChanged(state DeploymentState)

	// ManifestsFiltered is called after [FilterAcceptedManifests] runs for
	// a target. total is the pre-filter count; accepted is the post-filter
	// count. When accepted is zero the target receives no delivery.
	ManifestsFiltered(target TargetInfo, total, accepted int)

	// DeliveryOutputsProcessed is called after [ProcessDeliveryOutputs]
	// registers provisioned targets and stores secrets from a delivery
	// result.
	DeliveryOutputsProcessed(targets []ProvisionedTarget, secrets int)

	// Error is called when an error occurs during the run.
	Error(err error)

	// End signals the run is complete (for timing). Called via defer.
	End()
}

// NoOpDeploymentObserver is a [DeploymentObserver] that returns a no-op probe.
type NoOpDeploymentObserver struct{}

func (NoOpDeploymentObserver) RunStarted(ctx context.Context, _ DeploymentID) (context.Context, DeploymentRunProbe) {
	return ctx, NoOpDeploymentRunProbe{}
}

// NoOpDeploymentRunProbe is a [DeploymentRunProbe] that discards all events.
type NoOpDeploymentRunProbe struct{}

func (NoOpDeploymentRunProbe) EventReceived(DeploymentEvent)                     {}
func (NoOpDeploymentRunProbe) StateChanged(DeploymentState)                      {}
func (NoOpDeploymentRunProbe) ManifestsFiltered(TargetInfo, int, int)            {}
func (NoOpDeploymentRunProbe) DeliveryOutputsProcessed([]ProvisionedTarget, int) {}
func (NoOpDeploymentRunProbe) Error(error)                                       {}
func (NoOpDeploymentRunProbe) End()                                              {}
