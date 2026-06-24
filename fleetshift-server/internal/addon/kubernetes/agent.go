package kubernetes

import (
	"context"
	"log/slog"

	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Agent is the unified per-target agent that holds shared K8s
// clients and delegates to a delivery delegate and an indexer delegate.
type Agent struct {
	targetID   domain.TargetID
	restConfig *rest.Config
	dynClient  dynamic.Interface
	discClient discovery.DiscoveryInterface
	logger     *slog.Logger

	delivery *deliveryDelegate
	indexer  *indexerDelegate

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// NewAgent creates an Agent with the given clients and delegates.
// The Agent owns its own lifecycle via a context derived from the parent.
func NewAgent(
	ctx context.Context,
	targetID domain.TargetID,
	restConfig *rest.Config,
	dynClient dynamic.Interface,
	discClient discovery.DiscoveryInterface,
	delivery *deliveryDelegate,
	indexer *indexerDelegate,
	logger *slog.Logger,
) *Agent {
	agentCtx, cancel := context.WithCancel(ctx)
	return &Agent{
		targetID:   targetID,
		restConfig: restConfig,
		dynClient:  dynClient,
		discClient: discClient,
		logger:     logger,
		delivery:   delivery,
		indexer:    indexer,
		ctx:        agentCtx,
		cancel:     cancel,
		done:       make(chan struct{}),
	}
}

// TargetID returns the agent's target identifier.
func (a *Agent) TargetID() domain.TargetID { return a.targetID }

// DynClient returns the dynamic Kubernetes client.
func (a *Agent) DynClient() dynamic.Interface { return a.dynClient }

// DiscClient returns the discovery client.
func (a *Agent) DiscClient() discovery.DiscoveryInterface { return a.discClient }

// APIServer returns the API server URL from the rest config.
func (a *Agent) APIServer() string { return a.restConfig.Host }

// CAData returns the CA certificate data from the rest config.
func (a *Agent) CAData() []byte { return a.restConfig.TLSClientConfig.CAData }

// Done returns a channel that is closed when the agent has stopped.
func (a *Agent) Done() <-chan struct{} { return a.done }

// Deliver forwards to the delivery delegate.
func (a *Agent) Deliver(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, generation domain.Generation) error {
	return a.delivery.deliver(ctx, a.restConfig, target, deliveryID, manifests, auth, att, generation)
}

// Remove forwards to the delivery delegate.
func (a *Agent) Remove(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, generation domain.Generation) error {
	return a.delivery.remove(ctx, a.restConfig, target, deliveryID, manifests, auth, att, generation)
}

// start runs the indexer delegate until the agent's context is cancelled.
// If no indexer is configured (delivery-only mode), it closes done immediately.
func (a *Agent) start() {
	defer close(a.done)
	if a.indexer != nil {
		a.indexer.start(a.ctx)
	}
}

// Stop cancels the agent's context and waits for it to finish.
func (a *Agent) Stop() {
	a.cancel()
	<-a.done
}
