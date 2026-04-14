// Package kind implements a [domain.DeliveryAgent] for managing kind
// (Kubernetes-in-Docker) clusters. Manifests are interpreted as kind
// cluster specifications; delivery creates or updates clusters, and
// removal deletes them.
package kind

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"
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
	tokenVerifier   domain.OIDCTokenVerifier
	oidcConfig      *domain.OIDCConfig
	containerHost   string // hostname containers use to reach the host machine (replaces localhost)
	oidcHTTPSPort   string // when set, rewrite HTTP issuer URLs to HTTPS with this port (e.g. "8443")

	trustMu      sync.RWMutex
	trustBundles []domain.TrustBundleEntry
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

// WithContainerHost sets the hostname that containers use to reach
// the host machine. When set, localhost and 127.0.0.1 in OIDC issuer
// URLs are rewritten to this value before injecting into the kubeadm
// config. Typical value: "host.docker.internal" (Docker Desktop).
// Falls back to the original URL when empty.
func WithContainerHost(host string) AgentOption {
	return func(a *Agent) { a.containerHost = host }
}

// WithOIDCHTTPSPort enables HTTP→HTTPS upgrade for OIDC issuer URLs
// passed to kube-apiserver (which requires HTTPS). The port is the
// HTTPS port of the OIDC provider (e.g. "8443"). When unset, the
// issuer URL is passed through unchanged.
func WithOIDCHTTPSPort(port string) AgentOption {
	return func(a *Agent) { a.oidcHTTPSPort = port }
}

// WithTokenVerifier configures the agent to verify the caller's JWT
// before acting on a delivery. This simulates the provenance check
// that a remote delivery agent would perform: verifying that the
// token is valid (signature, expiry, issuer, audience) before
// creating infrastructure on behalf of the caller.
func WithTokenVerifier(v domain.OIDCTokenVerifier, cfg domain.OIDCConfig) AgentOption {
	return func(a *Agent) {
		a.tokenVerifier = v
		a.oidcConfig = &cfg
	}
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

// Deliver dispatches on manifest resource type. Trust-bundle manifests
// are stored in memory synchronously. Cluster manifests follow the
// existing async flow.
func (a *Agent) Deliver(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, _ *domain.Attestation, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	var clusterManifests []domain.Manifest
	for _, m := range manifests {
		switch m.ResourceType {
		case domain.TrustBundleResourceType:
			if err := a.storeTrustBundle(m); err != nil {
				return domain.DeliveryResult{
					State:   domain.DeliveryStateFailed,
					Message: fmt.Sprintf("store trust bundle: %v", err),
				}, nil
			}
		default:
			clusterManifests = append(clusterManifests, m)
		}
	}

	if len(clusterManifests) == 0 {
		go signaler.Done(ctx, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
		return domain.DeliveryResult{State: domain.DeliveryStateAccepted}, nil
	}

	specs, err := a.validateManifests(clusterManifests, auth)
	if err != nil {
		return domain.DeliveryResult{State: domain.DeliveryStateFailed}, err
	}

	if err := a.verifyToken(ctx, auth); err != nil {
		return domain.DeliveryResult{
			State:   domain.DeliveryStateAuthFailed,
			Message: fmt.Sprintf("token verification failed: %v", err),
		}, nil
	}

	provider := a.providerFactory(NewObserverLogger(ctx, signaler, time.Now))

	go a.deliverAsync(ctx, provider, specs, auth, signaler)

	return domain.DeliveryResult{State: domain.DeliveryStateAccepted}, nil
}

// storeTrustBundle unmarshals and stores a trust bundle entry.
func (a *Agent) storeTrustBundle(m domain.Manifest) error {
	var entry domain.TrustBundleEntry
	if err := json.Unmarshal(m.Raw, &entry); err != nil {
		return fmt.Errorf("unmarshal trust bundle entry: %w", err)
	}
	a.trustMu.Lock()
	defer a.trustMu.Unlock()
	a.trustBundles = append(a.trustBundles, entry)
	return nil
}

// TrustBundles returns a snapshot of the currently stored trust bundle
// entries. Used by tests and the cluster provisioning flow.
func (a *Agent) TrustBundles() []domain.TrustBundleEntry {
	a.trustMu.RLock()
	defer a.trustMu.RUnlock()
	out := make([]domain.TrustBundleEntry, len(a.trustBundles))
	copy(out, a.trustBundles)
	return out
}

// verifyToken checks the caller's JWT when a verifier is configured.
// This simulates the provenance responsibility of a remote delivery
// agent: refuse to act if the token is expired, has a bad signature,
// or targets the wrong audience.
func (a *Agent) verifyToken(ctx context.Context, auth domain.DeliveryAuth) error {
	if a.tokenVerifier == nil || a.oidcConfig == nil {
		return nil
	}
	if auth.Token == "" {
		return nil
	}
	_, err := a.tokenVerifier.Verify(ctx, *a.oidcConfig, string(auth.Token))
	return err
}

// Remove deletes kind clusters described by the manifests.
// Clusters that are already gone are silently skipped.
func (a *Agent) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, manifests []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ *domain.DeliverySignaler) error {
	specs, err := a.validateManifests(manifests, domain.DeliveryAuth{})
	if err != nil {
		return fmt.Errorf("validate manifests: %w", err)
	}

	provider := a.providerFactory(nil)

	for _, spec := range specs {
		if !a.clusterExists(provider, spec.Name) {
			continue
		}
		if err := provider.Delete(spec.Name, ""); err != nil {
			return fmt.Errorf("delete kind cluster %q: %w", spec.Name, err)
		}
	}
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
		result.ProducedSecrets = append(result.ProducedSecrets, out.Secrets()...)
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

	// External kubeconfig (127.0.0.1:<nodePort>) — used for stored connection info
	// accessible from the host.
	kc, err := provider.KubeConfig(spec.Name, false)
	if err != nil {
		signaler.Emit(ctx, domain.DeliveryEvent{
			Kind:    domain.DeliveryEventWarning,
			Message: fmt.Sprintf("get kubeconfig for %q: %v", spec.Name, err),
		})
		return nil, true
	}

	// Internal kubeconfig (container IP:6443) — used for in-container operations
	// (RBAC bootstrap, platform SA) where 127.0.0.1 would be unreachable.
	internalKC, err := provider.KubeConfig(spec.Name, true)
	if err != nil {
		signaler.Emit(ctx, domain.DeliveryEvent{
			Kind:    domain.DeliveryEventWarning,
			Message: fmt.Sprintf("get internal kubeconfig for %q: %v", spec.Name, err),
		})
		// Fall back to external kubeconfig (works when not in Docker).
		internalKC = kc
	}

	if auth.Caller != nil {
		signaler.Emit(ctx, domain.DeliveryEvent{
			Kind:    domain.DeliveryEventProgress,
			Message: fmt.Sprintf("Bootstrapping RBAC for %s on %q", auth.Caller.Subject, spec.Name),
		})
		username := string(auth.Caller.Issuer) + "#" + string(auth.Caller.Subject)
		if err := bootstrapRBAC(ctx, []byte(internalKC), auth.Caller.Issuer, auth.Caller); err != nil {
			probe.Error(err)
			failDelivery(ctx, signaler, "bootstrap RBAC on %q: %v", spec.Name, err)
			return nil, false
		}
		probe.RBACBootstrapped(auth.Caller.Subject, username)
	}

	apiServer, caCert, err := ExtractClusterConnInfo([]byte(kc))
	if err != nil {
		probe.Error(err)
		failDelivery(ctx, signaler, "extract connection info for %q: %v", spec.Name, err)
		return nil, false
	}

	targetID := domain.TargetID("k8s-" + spec.Name)
	out := ClusterOutput{
		TargetID:     targetID,
		Name:         spec.Name,
		APIServer:    apiServer,
		CACert:       caCert,
		TrustBundles: a.TrustBundles(),
	}

	signaler.Emit(ctx, domain.DeliveryEvent{
		Kind:    domain.DeliveryEventProgress,
		Message: fmt.Sprintf("Bootstrapping platform ServiceAccount on %q", spec.Name),
	})
	ref, token, saErr := bootstrapPlatformSA(ctx, []byte(internalKC), targetID)
	if saErr != nil {
		signaler.Emit(ctx, domain.DeliveryEvent{
			Kind:    domain.DeliveryEventWarning,
			Message: fmt.Sprintf("platform SA bootstrap on %q: %v (attested delivery will not work)", spec.Name, saErr),
		})
	} else {
		out.SATokenRef = ref
		out.SAToken = token
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
		issuer := a.rewriteIssuerForDocker(auth.Caller.Issuer)
		cfg, err := BuildKindOIDCConfig(issuer, auth.Audience[0], oidcSpec, caCertHostPath)
		return cfg, ConfigSourceOIDC, err
	}
	if len(spec.Config) > 0 {
		return spec.Config, ConfigSourceCustom, nil
	}
	return nil, ConfigSourceDefault, nil
}

// rewriteIssuerForDocker replaces localhost/127.0.0.1 in the issuer
// URL with the configured docker host so the URL is reachable from
// inside kind containers. When oidcHTTPSPort is set, it also upgrades
// HTTP issuer URLs to HTTPS on that port (kube-apiserver requires
// HTTPS for --oidc-issuer-url). Returns the original URL when no
// rewriting is needed.
func (a *Agent) rewriteIssuerForDocker(issuer domain.IssuerURL) domain.IssuerURL {
	u, err := url.Parse(string(issuer))
	if err != nil {
		return issuer
	}

	changed := false

	// Replace localhost/127.0.0.1 with the container host.
	if a.containerHost != "" {
		host := strings.Split(u.Host, ":")[0]
		if host == "localhost" || host == "127.0.0.1" {
			port := u.Port()
			if port != "" {
				u.Host = a.containerHost + ":" + port
			} else {
				u.Host = a.containerHost
			}
			changed = true
		}
	}

	// kube-apiserver requires HTTPS for --oidc-issuer-url. When
	// oidcHTTPSPort is configured, upgrade HTTP to HTTPS on that port.
	if u.Scheme == "http" && a.oidcHTTPSPort != "" {
		u.Scheme = "https"
		host := strings.Split(u.Host, ":")[0]
		u.Host = host + ":" + a.oidcHTTPSPort
		changed = true
	}

	if changed {
		return domain.IssuerURL(u.String())
	}
	return issuer
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
