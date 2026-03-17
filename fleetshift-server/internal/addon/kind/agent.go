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
// kind create cluster --config). OIDC optionally configures the API
// server's OIDC authentication; it is mutually exclusive with Config.
type ClusterSpec struct {
	Name   string          `json:"name"`
	Config json.RawMessage `json:"config,omitempty"`
	OIDC   *OIDCSpec       `json:"oidc,omitempty"`
}

// ClusterProvider abstracts the kind cluster operations needed by the
// delivery agent. [cluster.Provider] satisfies this interface.
type ClusterProvider interface {
	Create(name string, options ...cluster.CreateOption) error
	Delete(name, kubeconfig string) error
	List() ([]string, error)
	KubeConfig(name string, internal bool) (string, error)
}

// ClusterProviderFactory creates a [ClusterProvider] with the given
// logger wired in. Each delivery creates its own provider so that
// kind's log output is captured per-delivery via the
// [domain.DeliverySignaler].
type ClusterProviderFactory func(logger log.Logger) ClusterProvider

// Agent implements [domain.DeliveryAgent] for kind clusters.
type Agent struct {
	providerFactory ClusterProviderFactory
	observer        AgentObserver
	tempDir         string
	oidcCABundle    []byte
}

// AgentOption configures an [Agent].
type AgentOption func(*Agent)

// WithObserver sets the [AgentObserver] for delivery lifecycle events.
func WithObserver(o AgentObserver) AgentOption {
	return func(a *Agent) { a.observer = o }
}

// WithTempDir sets the directory for temporary files (e.g., CA certs)
// that must be visible to the container runtime. If unset, [os.TempDir]
// is used. Container runtimes like Podman only mount specific host
// paths into the VM, so callers should set this to a path the runtime
// can see.
func WithTempDir(dir string) AgentOption {
	return func(a *Agent) { a.tempDir = dir }
}

// WithOIDCCABundle sets a PEM-encoded CA certificate for trusting the
// OIDC issuer's TLS. When set, the agent mounts it into kind nodes and
// configures --oidc-ca-file. When empty, the API server uses its system
// trust store.
func WithOIDCCABundle(pem []byte) AgentOption {
	return func(a *Agent) { a.oidcCABundle = pem }
}

// NewAgent returns an Agent that creates providers via the given factory.
func NewAgent(factory ClusterProviderFactory, opts ...AgentOption) *Agent {
	a := &Agent{providerFactory: factory}
	for _, o := range opts {
		o(a)
	}
	return a
}

func (a *Agent) agentObserver() AgentObserver {
	if a.observer != nil {
		return a.observer
	}
	return NoOpAgentObserver{}
}

// Deliver validates all manifests synchronously. If validation passes,
// it returns [domain.DeliveryStateAccepted] immediately and performs
// the actual cluster creation in a background goroutine. Kind's own
// log output flows through the [domain.DeliverySignaler] via the
// [observerLogger] adapter.
func (a *Agent) Deliver(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	specs, err := a.validateManifests(manifests, auth)
	if err != nil {
		return domain.DeliveryResult{State: domain.DeliveryStateFailed}, err
	}

	provider := a.providerFactory(NewObserverLogger(ctx, signaler, time.Now))

	go a.deliverAsync(ctx, provider, specs, auth, signaler)

	return domain.DeliveryResult{State: domain.DeliveryStateAccepted}, nil
}

func (a *Agent) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ *domain.DeliverySignaler) error {
	return nil
}

func (a *Agent) validateManifests(manifests []domain.Manifest, auth domain.DeliveryAuth) ([]ClusterSpec, error) {
	specs := make([]ClusterSpec, len(manifests))
	for i, m := range manifests {
		if err := json.Unmarshal(m.Raw, &specs[i]); err != nil {
			return nil, fmt.Errorf("unmarshal kind cluster spec: %w", err)
		}
		if specs[i].Name == "" {
			return nil, fmt.Errorf("%w: kind cluster spec requires a name", domain.ErrInvalidArgument)
		}
		if specs[i].OIDC != nil && len(specs[i].Config) > 0 {
			return nil, fmt.Errorf("%w: kind cluster spec cannot have both config and oidc", domain.ErrInvalidArgument)
		}
		if len(specs[i].Config) > 0 && auth.Caller != nil {
			return nil, fmt.Errorf("%w: kind cluster spec cannot have both config and an authenticated caller", domain.ErrInvalidArgument)
		}
	}
	return specs, nil
}

func (a *Agent) deliverAsync(ctx context.Context, provider ClusterProvider, specs []ClusterSpec, auth domain.DeliveryAuth, signaler *domain.DeliverySignaler) {
	var outputs []ClusterOutput

	for _, spec := range specs {
		out, ok := a.deliverCluster(ctx, provider, spec, auth, signaler)
		if !ok {
			return
		}
		if out != nil {
			outputs = append(outputs, *out)
		}
	}

	result := domain.DeliveryResult{State: domain.DeliveryStateDelivered}
	for _, out := range outputs {
		result.ProvisionedTargets = append(result.ProvisionedTargets, out.Target())
	}
	signaler.Done(ctx, result)
}

// deliverCluster handles a single cluster spec. Returns the output on
// success and true to continue, or nil and false if the delivery failed
// (signaler.Done already called).
func (a *Agent) deliverCluster(ctx context.Context, provider ClusterProvider, spec ClusterSpec, auth domain.DeliveryAuth, signaler *domain.DeliverySignaler) (*ClusterOutput, bool) {
	ctx, probe := a.agentObserver().ClusterDeliverStarted(ctx, spec.Name)
	defer probe.End()

	if a.clusterExists(provider, spec.Name) {
		signaler.Emit(ctx, domain.DeliveryEvent{
			Kind:    domain.DeliveryEventProgress,
			Message: fmt.Sprintf("Deleting existing cluster %q for recreate", spec.Name),
		})
		if err := provider.Delete(spec.Name, ""); err != nil {
			probe.Error(err)
			failDelivery(ctx, signaler, "delete existing kind cluster %q for recreate: %v", spec.Name, err)
			return nil, false
		}
	}

	rawConfig, source, err := a.resolveConfig(spec, auth)
	if err != nil {
		probe.Error(err)
		failDelivery(ctx, signaler, "resolve config for kind cluster %q: %v", spec.Name, err)
		return nil, false
	}

	var issuer domain.IssuerURL
	var aud domain.Audience
	if source == ConfigSourceOIDC {
		issuer = auth.Caller.Issuer
		aud = auth.Audience[0]
	}
	probe.ConfigResolved(source, issuer, aud)

	var opts []cluster.CreateOption
	if rawConfig != nil {
		opts = append(opts, cluster.CreateWithRawConfig(rawConfig))
	}

	if err := provider.Create(spec.Name, opts...); err != nil {
		probe.Error(err)
		failDelivery(ctx, signaler, "create kind cluster %q: %v", spec.Name, err)
		return nil, false
	}

	kc, err := provider.KubeConfig(spec.Name, false)
	if err != nil {
		signaler.Emit(ctx, domain.DeliveryEvent{
			Kind:    domain.DeliveryEventWarning,
			Message: fmt.Sprintf("get kubeconfig for %q: %v", spec.Name, err),
		})
		return nil, true
	}

	if auth.Caller != nil {
		signaler.Emit(ctx, domain.DeliveryEvent{
			Kind:    domain.DeliveryEventProgress,
			Message: fmt.Sprintf("Bootstrapping RBAC for %s on %q", auth.Caller.ID, spec.Name),
		})
		username := string(auth.Caller.Issuer) + "#" + string(auth.Caller.ID)
		if err := bootstrapRBAC(ctx, []byte(kc), auth.Caller.Issuer, auth.Caller); err != nil {
			probe.Error(err)
			failDelivery(ctx, signaler, "bootstrap RBAC on %q: %v", spec.Name, err)
			return nil, false
		}
		probe.RBACBootstrapped(auth.Caller.ID, username)
	}

	apiServer, caCert, err := ExtractClusterConnInfo([]byte(kc))
	if err != nil {
		probe.Error(err)
		failDelivery(ctx, signaler, "extract connection info for %q: %v", spec.Name, err)
		return nil, false
	}

	out := ClusterOutput{
		TargetID:  domain.TargetID("k8s-" + spec.Name),
		Name:      spec.Name,
		APIServer: apiServer,
		CACert:    caCert,
	}
	return &out, true
}

func failDelivery(ctx context.Context, signaler *domain.DeliverySignaler, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	signaler.Emit(ctx, domain.DeliveryEvent{
		Kind:    domain.DeliveryEventError,
		Message: msg,
	})
	signaler.Done(ctx, domain.DeliveryResult{
		State:   domain.DeliveryStateFailed,
		Message: msg,
	})
}

// resolveConfig returns the raw kind config bytes and the
// [ConfigSource] for a ClusterSpec. When an authenticated caller is
// present, the config includes OIDC API server flags derived from the
// caller's identity, with an optional CA cert mount. When Config is
// set (no caller), it is returned as-is. Returns nil when neither
// applies (default kind config).
func (a *Agent) resolveConfig(spec ClusterSpec, auth domain.DeliveryAuth) ([]byte, ConfigSource, error) {
	if auth.Caller != nil {
		var caCertHostPath string
		if len(a.oidcCABundle) > 0 {
			path, err := writeCABundle(a.oidcCABundle, a.tempDir)
			if err != nil {
				return nil, "", err
			}
			caCertHostPath = path
		}
		oidcSpec := spec.OIDC
		if oidcSpec == nil {
			oidcSpec = &OIDCSpec{}
		}
		// TODO: audience policy -- for now we use the first audience from
		// the caller's token. This couples the cluster's oidc-client-id to
		// whatever audience the platform validated the user against.
		cfg, err := BuildKindOIDCConfig(auth.Caller.Issuer, auth.Audience[0], oidcSpec, caCertHostPath)
		return cfg, ConfigSourceOIDC, err
	}
	if len(spec.Config) > 0 {
		return spec.Config, ConfigSourceCustom, nil
	}
	return nil, ConfigSourceDefault, nil
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
