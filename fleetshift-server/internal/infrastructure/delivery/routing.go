// Package delivery provides the [RoutingDeliveryService] that
// implements [domain.DeliveryService] by dispatching to per-target-type
// [domain.DeliveryAgent] implementations.
package delivery

import (
	"context"
	"fmt"
	"sync"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// RoutingDeliveryService implements [domain.DeliveryService] by routing
// each delivery to the [domain.DeliveryAgent] registered for the
// target's [domain.TargetType]. Registration is thread-safe to support
// dynamic addon connect/disconnect.
type RoutingDeliveryService struct {
	mu     sync.RWMutex
	agents map[domain.TargetType]domain.DeliveryAgent
}

// NewRoutingDeliveryService returns a ready-to-use router with no agents
// registered.
func NewRoutingDeliveryService() *RoutingDeliveryService {
	return &RoutingDeliveryService{
		agents: make(map[domain.TargetType]domain.DeliveryAgent),
	}
}

// Register associates a [domain.DeliveryAgent] with a [domain.TargetType].
// Calling Register for an already-registered type replaces the previous agent.
func (r *RoutingDeliveryService) Register(targetType domain.TargetType, agent domain.DeliveryAgent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.agents[targetType] = agent
}

// Deliver routes to the agent registered for target.Type.
func (r *RoutingDeliveryService) Deliver(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, attestation *domain.Attestation, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	agent, err := r.agentFor(target.Type)
	if err != nil {
		return domain.DeliveryResult{}, err
	}
	return agent.Deliver(ctx, target, deliveryID, manifests, auth, attestation, signaler)
}

// Remove routes to the agent registered for target.Type.
func (r *RoutingDeliveryService) Remove(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, signaler *domain.DeliverySignaler) error {
	agent, err := r.agentFor(target.Type)
	if err != nil {
		return err
	}
	return agent.Remove(ctx, target, deliveryID, manifests, auth, signaler)
}

func (r *RoutingDeliveryService) agentFor(tt domain.TargetType) (domain.DeliveryAgent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	agent, ok := r.agents[tt]
	if !ok {
		return nil, fmt.Errorf("%w: no delivery agent registered for target type %q", domain.ErrInvalidArgument, tt)
	}
	return agent, nil
}
