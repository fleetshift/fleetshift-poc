// Package kind implements a [domain.DeliveryAgent] for managing kind
// (Kubernetes-in-Docker) clusters. Manifests are interpreted as kind
// cluster specifications; delivery creates or updates clusters, and
// removal deletes them.
package kind

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"sigs.k8s.io/kind/pkg/cluster"
	"sigs.k8s.io/kind/pkg/log"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// TargetType is the [domain.TargetType] for kind-managed targets.
const TargetType domain.TargetType = "kind"

// ClusterResourceType is the [domain.ResourceType] for kind cluster
// specifications.
const ClusterResourceType domain.ResourceType = "api.kind.cluster"

// ClusterSpec is the manifest payload accepted by the kind delivery
// agent. Name identifies the kind cluster; Config holds the raw kind
// cluster configuration YAML/JSON (the same format accepted by
// kind create cluster --config).
type ClusterSpec struct {
	Name   string          `json:"name"`
	Config json.RawMessage `json:"config,omitempty"`
}

// ClusterProvider abstracts the kind cluster operations needed by the
// delivery agent. [cluster.Provider] satisfies this interface.
type ClusterProvider interface {
	Create(name string, options ...cluster.CreateOption) error
	Delete(name, kubeconfig string) error
	List() ([]string, error)
}

// ClusterProviderFactory creates a [ClusterProvider] with the given
// logger wired in. Each delivery creates its own provider so that
// kind's log output is captured per-delivery via the
// [domain.DeliverySignaler].
type ClusterProviderFactory func(logger log.Logger) ClusterProvider

// Agent implements [domain.DeliveryAgent] for kind clusters.
type Agent struct {
	providerFactory ClusterProviderFactory
}

// NewAgent returns an Agent that creates providers via the given factory.
func NewAgent(factory ClusterProviderFactory) *Agent {
	return &Agent{providerFactory: factory}
}

// Deliver validates all manifests synchronously. If validation passes,
// it returns [domain.DeliveryStateAccepted] immediately and performs
// the actual cluster creation in a background goroutine. Kind's own
// log output flows through the [domain.DeliverySignaler] via the
// [observerLogger] adapter.
func (a *Agent) Deliver(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, manifests []domain.Manifest, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	specs, err := a.validateManifests(manifests)
	if err != nil {
		return domain.DeliveryResult{State: domain.DeliveryStateFailed}, err
	}

	provider := a.providerFactory(NewObserverLogger(ctx, signaler, time.Now))

	go a.deliverAsync(ctx, provider, specs, signaler)

	return domain.DeliveryResult{State: domain.DeliveryStateAccepted}, nil
}

func (a *Agent) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ *domain.DeliverySignaler) error {
	return nil
}

func (a *Agent) validateManifests(manifests []domain.Manifest) ([]ClusterSpec, error) {
	specs := make([]ClusterSpec, len(manifests))
	for i, m := range manifests {
		if err := json.Unmarshal(m.Raw, &specs[i]); err != nil {
			return nil, fmt.Errorf("unmarshal kind cluster spec: %w", err)
		}
		if specs[i].Name == "" {
			return nil, fmt.Errorf("%w: kind cluster spec requires a name", domain.ErrInvalidArgument)
		}
	}
	return specs, nil
}

func (a *Agent) deliverAsync(ctx context.Context, provider ClusterProvider, specs []ClusterSpec, signaler *domain.DeliverySignaler) {
	for _, spec := range specs {
		if a.clusterExists(provider, spec.Name) {
			signaler.Emit(ctx, domain.DeliveryEvent{
				Kind:    domain.DeliveryEventProgress,
				Message: fmt.Sprintf("Deleting existing cluster %q for recreate", spec.Name),
			})
			if err := provider.Delete(spec.Name, ""); err != nil {
				signaler.Emit(ctx, domain.DeliveryEvent{
					Kind:    domain.DeliveryEventError,
					Message: fmt.Sprintf("delete existing kind cluster %q for recreate: %v", spec.Name, err),
				})
				signaler.Done(ctx, domain.DeliveryResult{
					State:   domain.DeliveryStateFailed,
					Message: fmt.Sprintf("delete existing kind cluster %q for recreate: %v", spec.Name, err),
				})
				return
			}
		}

		var opts []cluster.CreateOption
		if len(spec.Config) > 0 {
			opts = append(opts, cluster.CreateWithRawConfig(spec.Config))
		}

		if err := provider.Create(spec.Name, opts...); err != nil {
			signaler.Emit(ctx, domain.DeliveryEvent{
				Kind:    domain.DeliveryEventError,
				Message: fmt.Sprintf("create kind cluster %q: %v", spec.Name, err),
			})
			signaler.Done(ctx, domain.DeliveryResult{
				State:   domain.DeliveryStateFailed,
				Message: fmt.Sprintf("create kind cluster %q: %v", spec.Name, err),
			})
			return
		}
	}

	signaler.Done(ctx, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
}

func (a *Agent) clusterExists(provider ClusterProvider, name string) bool {
	clusters, err := provider.List()
	if err != nil {
		return false
	}
	for _, c := range clusters {
		if c == name {
			return true
		}
	}
	return false
}
