package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/attestation"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// TargetType is the [domain.TargetType] for Kubernetes clusters
// managed by the direct delivery agent (token-passthrough, no fleetlet).
const TargetType domain.TargetType = "kubernetes"

// ManifestManifestType is the [domain.ManifestType] for generic
// Kubernetes manifests applied via server-side apply.
const ManifestManifestType domain.ManifestType = "kubernetes"

// DeliveryAgent implements [domain.DeliveryAgent] for Kubernetes clusters.
// When a target has a trust_bundle property and an attestation is
// present, verification is done per-target. Verifiers are cached by
// trust bundle content so repeated deliveries to the same target
// don't re-initialize JWKS fetching. Falls back to token passthrough
// when no attestation is present.
//
// Delivery supports two modes:
//
//   - Token passthrough: authenticates using the caller's JWT (legacy).
//   - Attested delivery: verifies the attestation bundle, then applies
//     using platform credentials from target properties (kubeconfig or
//     service account token). This is the "run-as-platform" model.
type DeliveryAgent struct {
	reporter    domain.DeliveryReporter
	keyResolver *domain.KeyResolver
	httpClient  *http.Client
	vault       domain.Vault

	mu        sync.RWMutex
	verifiers map[string]*attestation.Verifier
}

// DeliveryAgentOption configures a [DeliveryAgent].
type DeliveryAgentOption func(*DeliveryAgent)

// WithKeyResolver sets the key resolver used for attestation
// verification (resolving signing keys from external registries).
func WithKeyResolver(r *domain.KeyResolver) DeliveryAgentOption {
	return func(a *DeliveryAgent) { a.keyResolver = r }
}

// WithHTTPClient sets the HTTP client used by per-target JWKS
// fetchers. Defaults to [http.DefaultClient].
func WithHTTPClient(c *http.Client) DeliveryAgentOption {
	return func(a *DeliveryAgent) { a.httpClient = c }
}

// WithVault configures the vault used to resolve secret references in
// target properties (e.g. service_account_token_ref). Required for
// attested delivery when platform credentials are vault-backed.
func WithVault(v domain.Vault) DeliveryAgentOption {
	return func(a *DeliveryAgent) { a.vault = v }
}

// NewDeliveryAgent returns a DeliveryAgent. The reporter is the addon's
// client interface for communicating delivery updates back to the platform.
func NewDeliveryAgent(reporter domain.DeliveryReporter, opts ...DeliveryAgentOption) *DeliveryAgent {
	a := &DeliveryAgent{
		reporter:  reporter,
		verifiers: make(map[string]*attestation.Verifier),
	}
	for _, o := range opts {
		o(a)
	}
	return a
}

// Deliver validates the target and auth synchronously then dispatches
// the actual SSA apply in a background goroutine. All delivery
// outcomes are reported through the [domain.DeliveryReporter].
//
// When an attestation is provided and the agent has a verification
// config, the attestation is verified before apply. Verification
// failure is reported as [domain.DeliveryStateAuthFailed].
func (a *DeliveryAgent) Deliver(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, generation domain.Generation) error {
	if target.Properties()[PropAPIServer] == "" {
		return fmt.Errorf("%w: target %q missing %s property", domain.ErrInvalidArgument, target.ID(), PropAPIServer)
	}

	if att != nil {
		v, err := a.verifierForTarget(target)
		if err != nil {
			_ = a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
				State:   domain.DeliveryStateAuthFailed,
				Message: fmt.Sprintf("build verifier for target %q: %v", target.ID(), err),
			})
			return nil
		}
		if err := v.Verify(ctx, att, generation); err != nil {
			_ = a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
				State:   domain.DeliveryStateAuthFailed,
				Message: fmt.Sprintf("attestation verification failed: %v", err),
			})
			return nil
		}
		go a.deliverAsyncPlatform(context.WithoutCancel(ctx), target, deliveryID, generation, manifests)
		return nil
	}

	if auth.Token == "" {
		return fmt.Errorf("%w: delivery to target %q requires an authenticated caller token", domain.ErrInvalidArgument, target.ID())
	}
	go a.deliverAsync(context.WithoutCancel(ctx), target, deliveryID, generation, manifests, auth)
	return nil
}

// verifierForTarget builds or retrieves a cached [attestation.Verifier]
// from the target's trust_bundle property. Returns an error if the
// target has no trust_bundle (you cannot deliver attested content to a
// target without trust config).
func (a *DeliveryAgent) verifierForTarget(target domain.TargetInfo) (*attestation.Verifier, error) {
	trustJSON := target.Properties()["trust_bundle"]
	if trustJSON == "" {
		return nil, fmt.Errorf("target %q has no trust_bundle property", target.ID())
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
			JWKSURI:                  e.JWKSURI,
			Audience:                 e.EnrollmentAudience,
			PublicKeyClaimExpression: e.PublicKeyClaimExpression,
			RegistrySubjectMapping:   e.RegistrySubjectMapping,
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
func (a *DeliveryAgent) deliverAsyncPlatform(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, generation domain.Generation, manifests []domain.Manifest) {
	cfg, err := BuildPlatformRESTConfig(ctx, a.vault, target)
	if err != nil {
		_ = a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("build platform kubernetes client for target %q: %v", target.ID(), err),
		})
		return
	}
	a.applyManifests(ctx, target, deliveryID, generation, cfg, manifests)
}

func (a *DeliveryAgent) deliverAsync(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, generation domain.Generation, manifests []domain.Manifest, auth domain.DeliveryAuth) {
	cfg, err := buildCallerRESTConfig(target, auth.Token)
	if err != nil {
		_ = a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("build kubernetes client for target %q: %v", target.ID(), err),
		})
		return
	}
	a.applyManifests(ctx, target, deliveryID, generation, cfg, manifests)
}

func (a *DeliveryAgent) applyManifests(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, generation domain.Generation, cfg *rest.Config, manifests []domain.Manifest) {
	ap, err := newApplierFromConfig(cfg)
	if err != nil {
		_ = a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
			State:   deliveryStateForError(err),
			Message: fmt.Sprintf("build kubernetes client for target %q: %v", target.ID(), err),
		})
		return
	}

	for i, m := range manifests {
		_ = a.reporter.ReportEvent(ctx, deliveryID, generation, domain.DeliveryEvent{
			Kind:    domain.DeliveryEventProgress,
			Message: fmt.Sprintf("Applying manifest %d/%d", i+1, len(manifests)),
		})

		if err := ap.apply(ctx, m.Raw); err != nil {
			_ = a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
				State:   deliveryStateForError(err),
				Message: fmt.Sprintf("apply manifest %d: %v", i+1, err),
			})
			return
		}
	}

	_ = a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
}

// deleteManifests deletes Kubernetes resources described by manifests.
// Resources that are already gone (404) are silently skipped.
func (a *DeliveryAgent) deleteManifests(ctx context.Context, cfg *rest.Config, manifests []domain.Manifest) error {
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
//
// Like Deliver, the work runs asynchronously. The method validates
// inputs synchronously and returns nil, then reports the outcome via
// [domain.DeliveryReporter.ReportResult].
func (a *DeliveryAgent) Remove(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, generation domain.Generation) error {
	if target.Properties()[PropAPIServer] == "" {
		return fmt.Errorf("%w: target %q missing %s property", domain.ErrInvalidArgument, target.ID(), PropAPIServer)
	}

	asyncCtx := context.WithoutCancel(ctx)
	if att != nil {
		v, err := a.verifierForTarget(target)
		if err != nil {
			_ = a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
				State:   domain.DeliveryStateAuthFailed,
				Message: fmt.Sprintf("build verifier for target %q: %v", target.ID(), err),
			})
			return nil
		}
		if err := v.Verify(ctx, att, generation); err != nil {
			_ = a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
				State:   domain.DeliveryStateAuthFailed,
				Message: fmt.Sprintf("attestation verification failed: %v", err),
			})
			return nil
		}
		cfg, err := BuildPlatformRESTConfig(ctx, a.vault, target)
		if err != nil {
			_ = a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
				State:   domain.DeliveryStateFailed,
				Message: fmt.Sprintf("build platform REST config: %v", err),
			})
			return nil
		}
		go func() {
			err := a.deleteManifests(asyncCtx, cfg, manifests)
			if err != nil {
				_ = a.reporter.ReportResult(asyncCtx, deliveryID, generation, domain.DeliveryResult{
					State: deliveryStateForError(err), Message: err.Error(),
				})
				return
			}
			_ = a.reporter.ReportResult(asyncCtx, deliveryID, generation, domain.DeliveryResult{
				State: domain.DeliveryStateDelivered,
			})
		}()
		return nil
	}

	if auth.Token == "" {
		return fmt.Errorf("%w: removal from target %q requires an authenticated caller token", domain.ErrInvalidArgument, target.ID())
	}

	cfg, err := buildCallerRESTConfig(target, auth.Token)
	if err != nil {
		_ = a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("build REST config: %v", err),
		})
		return nil
	}
	go func() {
		err := a.deleteManifests(asyncCtx, cfg, manifests)
		if err != nil {
			_ = a.reporter.ReportResult(asyncCtx, deliveryID, generation, domain.DeliveryResult{
				State: deliveryStateForError(err), Message: err.Error(),
			})
			return
		}
		_ = a.reporter.ReportResult(asyncCtx, deliveryID, generation, domain.DeliveryResult{
			State: domain.DeliveryStateDelivered,
		})
	}()
	return nil
}
