// Package kubernetes implements a [domain.DeliveryAgent] that applies
// Kubernetes manifests to a cluster via server-side apply (SSA). It
// supports two modes:
//
//   - Token passthrough: authenticates using the caller's JWT (legacy).
//   - Attested delivery: verifies the attestation bundle, then applies
//     using platform credentials from target properties (kubeconfig or
//     service account token). This is the "run-as-platform" model.
package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/attestation"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// TargetType is the [domain.TargetType] for Kubernetes clusters
// managed by the direct delivery agent (token-passthrough, no fleetlet).
const TargetType domain.TargetType = "kubernetes"

// ManifestResourceType is the [domain.ResourceType] for generic
// Kubernetes manifests applied via server-side apply.
const ManifestResourceType domain.ResourceType = "kubernetes"

const fieldManager = "fleetshift"

// Agent implements [domain.DeliveryAgent] for Kubernetes clusters.
// When a target has a trust_bundle property and an attestation is
// present, verification is done per-target. Verifiers are cached by
// trust bundle content so repeated deliveries to the same target
// don't re-initialize JWKS fetching. Falls back to token passthrough
// when no attestation is present.
type Agent struct {
	keyResolver *application.KeyResolver
	httpClient  *http.Client
	vault       domain.Vault

	mu        sync.RWMutex
	verifiers map[string]*attestation.Verifier
}

// AgentOption configures an [Agent].
type AgentOption func(*Agent)

// WithKeyResolver sets the key resolver used for attestation
// verification (resolving signing keys from external registries).
func WithKeyResolver(r *application.KeyResolver) AgentOption {
	return func(a *Agent) { a.keyResolver = r }
}

// WithHTTPClient sets the HTTP client used by per-target JWKS
// fetchers. Defaults to [http.DefaultClient].
func WithHTTPClient(c *http.Client) AgentOption {
	return func(a *Agent) { a.httpClient = c }
}

// WithVault configures the vault used to resolve secret references in
// target properties (e.g. service_account_token_ref). Required for
// attested delivery when platform credentials are vault-backed.
func WithVault(v domain.Vault) AgentOption {
	return func(a *Agent) { a.vault = v }
}

// NewAgent returns an Agent with optional configuration.
func NewAgent(opts ...AgentOption) *Agent {
	a := &Agent{
		verifiers: make(map[string]*attestation.Verifier),
	}
	for _, o := range opts {
		o(a)
	}
	return a
}

// Deliver validates the target and auth synchronously and returns
// [domain.DeliveryStateAccepted] immediately. The actual SSA apply
// runs in a background goroutine; on completion the goroutine calls
// [domain.DeliverySignaler.Done].
//
// When an attestation is provided and the agent has a verification
// config, the attestation is verified before apply. Verification
// failure returns [domain.DeliveryStateAuthFailed] immediately.
func (a *Agent) Deliver(ctx context.Context, target domain.TargetInfo, _ domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	if _, ok := target.Properties["api_server"]; !ok {
		return domain.DeliveryResult{State: domain.DeliveryStateFailed},
			fmt.Errorf("%w: target %q missing api_server property", domain.ErrInvalidArgument, target.ID)
	}

	if att != nil {
		v, err := a.verifierForTarget(target)
		if err != nil {
			return domain.DeliveryResult{
				State:   domain.DeliveryStateAuthFailed,
				Message: fmt.Sprintf("build verifier for target %q: %v", target.ID, err),
			}, nil
		}
		if err := v.Verify(ctx, att); err != nil {
			return domain.DeliveryResult{
				State:   domain.DeliveryStateAuthFailed,
				Message: fmt.Sprintf("attestation verification failed: %v", err),
			}, nil
		}
		go a.deliverAsyncPlatform(ctx, target, manifests, signaler)
		return domain.DeliveryResult{State: domain.DeliveryStateAccepted}, nil
	}

	if auth.Token == "" {
		return domain.DeliveryResult{State: domain.DeliveryStateFailed},
			fmt.Errorf("%w: delivery to target %q requires an authenticated caller token", domain.ErrInvalidArgument, target.ID)
	}
	go a.deliverAsync(ctx, target, manifests, auth, signaler)
	return domain.DeliveryResult{State: domain.DeliveryStateAccepted}, nil
}

// verifierForTarget builds or retrieves a cached [attestation.Verifier]
// from the target's trust_bundle property. Returns an error if the
// target has no trust_bundle (you cannot deliver attested content to a
// target without trust config).
func (a *Agent) verifierForTarget(target domain.TargetInfo) (*attestation.Verifier, error) {
	trustJSON := target.Properties["trust_bundle"]
	if trustJSON == "" {
		return nil, fmt.Errorf("target %q has no trust_bundle property", target.ID)
	}

	a.mu.RLock()
	if v, ok := a.verifiers[trustJSON]; ok {
		a.mu.RUnlock()
		return v, nil
	}
	a.mu.RUnlock()

	var entries []domain.TrustBundleEntry
	if err := json.Unmarshal([]byte(trustJSON), &entries); err != nil {
		return nil, fmt.Errorf("parse trust_bundle: %w", err)
	}

	issuers := make(map[domain.IssuerURL]attestation.TrustedIssuer, len(entries))
	for _, e := range entries {
		issuers[e.IssuerURL] = attestation.TrustedIssuer{
			JWKSURI:                e.JWKSURI,
			Audience:               e.EnrollmentAudience,
			RegistrySubjectMapping: e.RegistrySubjectMapping,
		}
	}

	var opts []attestation.VerifierOption
	if a.httpClient != nil {
		opts = append(opts, attestation.WithHTTPClient(a.httpClient))
	}
	if a.keyResolver != nil {
		opts = append(opts, attestation.WithKeyResolver(a.keyResolver))
	}
	v := attestation.NewVerifier(issuers, opts...)

	a.mu.Lock()
	a.verifiers[trustJSON] = v
	a.mu.Unlock()

	return v, nil
}

// deliverAsyncPlatform applies manifests using platform credentials
// from target properties. Called after attestation verification passes.
// The platform token is resolved from the target's properties: first
// a direct service_account_token, then a service_account_token_ref
// that is looked up in the agent's [domain.Vault].
func (a *Agent) deliverAsyncPlatform(ctx context.Context, target domain.TargetInfo, manifests []domain.Manifest, signaler *domain.DeliverySignaler) {
	cfg, err := a.buildPlatformRESTConfig(ctx, target)
	if err != nil {
		signaler.Done(ctx, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("build platform kubernetes client for target %q: %v", target.ID, err),
		})
		return
	}
	a.applyManifests(ctx, target, cfg, manifests, signaler)
}

func (a *Agent) deliverAsync(ctx context.Context, target domain.TargetInfo, manifests []domain.Manifest, auth domain.DeliveryAuth, signaler *domain.DeliverySignaler) {
	cfg, err := buildRESTConfig(target, auth.Token)
	if err != nil {
		signaler.Done(ctx, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("build kubernetes client for target %q: %v", target.ID, err),
		})
		return
	}
	a.applyManifests(ctx, target, cfg, manifests, signaler)
}

func (a *Agent) applyManifests(ctx context.Context, target domain.TargetInfo, cfg *rest.Config, manifests []domain.Manifest, signaler *domain.DeliverySignaler) {
	ap, err := newApplierFromConfig(cfg)
	if err != nil {
		signaler.Done(ctx, domain.DeliveryResult{
			State:   deliveryStateForError(err),
			Message: fmt.Sprintf("build kubernetes client for target %q: %v", target.ID, err),
		})
		return
	}

	for i, m := range manifests {
		signaler.Emit(ctx, domain.DeliveryEvent{
			Kind:    domain.DeliveryEventProgress,
			Message: fmt.Sprintf("Applying manifest %d/%d", i+1, len(manifests)),
		})

		if err := ap.apply(ctx, m.Raw); err != nil {
			signaler.Done(ctx, domain.DeliveryResult{
				State:   deliveryStateForError(err),
				Message: fmt.Sprintf("apply manifest %d: %v", i+1, err),
			})
			return
		}
	}

	signaler.Done(ctx, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
}

// deleteManifests deletes Kubernetes resources described by manifests.
// Resources that are already gone (404) are silently skipped.
func (a *Agent) deleteManifests(ctx context.Context, cfg *rest.Config, manifests []domain.Manifest) error {
	ap, err := newApplierFromConfig(cfg)
	if err != nil {
		return fmt.Errorf("build kubernetes client: %w", err)
	}

	for i, m := range manifests {
		if err := ap.delete(ctx, m.Raw); err != nil {
			return fmt.Errorf("delete manifest %d: %w", i+1, err)
		}
	}
	return nil
}

// deliveryStateForError returns [domain.DeliveryStateAuthFailed] for
// Kubernetes API authentication/authorization errors (401/403), and
// [domain.DeliveryStateFailed] for everything else.
func deliveryStateForError(err error) domain.DeliveryState {
	if apierrors.IsUnauthorized(err) || apierrors.IsForbidden(err) {
		return domain.DeliveryStateAuthFailed
	}
	return domain.DeliveryStateFailed
}

// Remove deletes all manifested resources from the target cluster.
// When an attestation is provided the agent verifies it against the
// target's trust bundle (same dynamic, per-target verification as
// Deliver) and uses platform credentials. Otherwise falls back to
// token passthrough (auth.Token).
// Resources that are already gone (404) are silently skipped.
func (a *Agent) Remove(ctx context.Context, target domain.TargetInfo, _ domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, _ *domain.DeliverySignaler) error {
	if att != nil {
		v, err := a.verifierForTarget(target)
		if err != nil {
			return fmt.Errorf("build verifier for target %q: %w", target.ID, err)
		}
		if err := v.Verify(ctx, att); err != nil {
			return fmt.Errorf("attestation verification failed: %w", err)
		}
		cfg, err := a.buildPlatformRESTConfig(ctx, target)
		if err != nil {
			return fmt.Errorf("build platform REST config: %w", err)
		}
		return a.deleteManifests(ctx, cfg, manifests)
	}

	cfg, err := buildRESTConfig(target, auth.Token)
	if err != nil {
		return fmt.Errorf("build REST config: %w", err)
	}
	return a.deleteManifests(ctx, cfg, manifests)
}

// buildPlatformRESTConfig builds a REST config from target properties
// using platform credentials rather than the user's JWT. The token is
// resolved in order:
//  1. Direct service_account_token property (for tests / simple setups).
//  2. service_account_token_ref resolved from the agent's [domain.Vault].
func (a *Agent) buildPlatformRESTConfig(ctx context.Context, target domain.TargetInfo) (*rest.Config, error) {
	apiServer := target.Properties["api_server"]
	if apiServer == "" {
		return nil, fmt.Errorf("target %q missing api_server property", target.ID)
	}
	token, err := a.resolvePlatformToken(ctx, target)
	if err != nil {
		return nil, err
	}
	cfg := &rest.Config{
		Host:        apiServer,
		BearerToken: token,
	}
	if ca := target.Properties["ca_cert"]; ca != "" {
		cfg.TLSClientConfig.CAData = []byte(ca)
	}
	return cfg, nil
}

func (a *Agent) resolvePlatformToken(ctx context.Context, target domain.TargetInfo) (string, error) {
	if token := target.Properties["service_account_token"]; token != "" {
		return token, nil
	}
	ref := target.Properties["service_account_token_ref"]
	if ref == "" {
		return "", fmt.Errorf("target %q missing service_account_token or service_account_token_ref for platform delivery", target.ID)
	}
	if a.vault == nil {
		return "", fmt.Errorf("target %q has service_account_token_ref but agent has no vault configured", target.ID)
	}
	val, err := a.vault.Get(ctx, domain.SecretRef(ref))
	if err != nil {
		return "", fmt.Errorf("resolve service_account_token_ref %q for target %q: %w", ref, target.ID, err)
	}
	return string(val), nil
}

func buildRESTConfig(target domain.TargetInfo, token domain.RawToken) (*rest.Config, error) {
	apiServer := target.Properties["api_server"]
	if apiServer == "" {
		return nil, fmt.Errorf("target %q missing api_server property", target.ID)
	}
	cfg := &rest.Config{
		Host:        apiServer,
		BearerToken: string(token),
	}
	if ca := target.Properties["ca_cert"]; ca != "" {
		cfg.TLSClientConfig.CAData = []byte(ca)
	}
	return cfg, nil
}

// applier wraps a dynamic client and REST mapper for SSA.
type applier struct {
	client dynamic.Interface
	mapper meta.RESTMapper
}

func newApplierFromConfig(cfg *rest.Config) (*applier, error) {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create discovery client: %w", err)
	}

	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create dynamic client: %w", err)
	}

	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))

	return &applier{client: dyn, mapper: mapper}, nil
}

func (a *applier) apply(ctx context.Context, raw json.RawMessage) error {
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(raw); err != nil {
		return fmt.Errorf("parse manifest: %w", err)
	}

	gvk := obj.GroupVersionKind()
	mapping, err := a.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return fmt.Errorf("resolve GVR for %s: %w", gvk, err)
	}

	var dr dynamic.ResourceInterface
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		ns := obj.GetNamespace()
		if ns == "" {
			ns = "default"
		}
		dr = a.client.Resource(mapping.Resource).Namespace(ns)
	} else {
		dr = a.client.Resource(mapping.Resource)
	}

	data, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("marshal object: %w", err)
	}

	_, err = dr.Patch(ctx, obj.GetName(), "application/apply-patch+yaml", data, metav1.PatchOptions{
		FieldManager: fieldManager,
	})
	if err != nil {
		return fmt.Errorf("apply %s %s/%s: %w", gvk.Kind, obj.GetNamespace(), obj.GetName(), err)
	}

	return nil
}

func (a *applier) delete(ctx context.Context, raw json.RawMessage) error {
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(raw); err != nil {
		return fmt.Errorf("parse manifest: %w", err)
	}

	gvk := obj.GroupVersionKind()
	mapping, err := a.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return fmt.Errorf("resolve GVR for %s: %w", gvk, err)
	}

	var dr dynamic.ResourceInterface
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		ns := obj.GetNamespace()
		if ns == "" {
			ns = "default"
		}
		dr = a.client.Resource(mapping.Resource).Namespace(ns)
	} else {
		dr = a.client.Resource(mapping.Resource)
	}

	if err := dr.Delete(ctx, obj.GetName(), metav1.DeleteOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("delete %s %s/%s: %w", gvk.Kind, obj.GetNamespace(), obj.GetName(), err)
	}
	return nil
}
