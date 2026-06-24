package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"k8s.io/client-go/rest"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/attestation"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// deliveryDelegate holds delivery-specific state for an Agent.
// It handles attested and passthrough delivery to Kubernetes clusters.
type deliveryDelegate struct {
	reporter    domain.DeliveryReporter
	keyResolver *domain.KeyResolver
	httpClient  *http.Client

	mu        sync.RWMutex
	verifiers map[string]*attestation.Verifier
}

// newDeliveryDelegate creates a deliveryDelegate with the given
// reporter, key resolver, and HTTP client. The key resolver and HTTP
// client may be nil when attestation verification is not needed.
func newDeliveryDelegate(reporter domain.DeliveryReporter, keyResolver *domain.KeyResolver, httpClient *http.Client) *deliveryDelegate {
	return &deliveryDelegate{
		reporter:    reporter,
		keyResolver: keyResolver,
		httpClient:  httpClient,
		verifiers:   make(map[string]*attestation.Verifier),
	}
}

// deliver validates the target and auth synchronously then dispatches
// the actual SSA apply in a background goroutine. platformCfg is the
// shared REST config owned by the Agent; for passthrough delivery
// a clone is made with the caller's bearer token swapped in.
func (d *deliveryDelegate) deliver(ctx context.Context, platformCfg *rest.Config, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, generation domain.Generation) error {
	if target.Properties()["api_server"] == "" {
		return fmt.Errorf("%w: target %q missing api_server property", domain.ErrInvalidArgument, target.ID())
	}

	if att != nil {
		v, err := d.verifierForTarget(target)
		if err != nil {
			_ = d.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
				State:   domain.DeliveryStateAuthFailed,
				Message: fmt.Sprintf("build verifier for target %q: %v", target.ID(), err),
			})
			return nil
		}
		if err := v.Verify(ctx, att, generation); err != nil {
			_ = d.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
				State:   domain.DeliveryStateAuthFailed,
				Message: fmt.Sprintf("attestation verification failed: %v", err),
			})
			return nil
		}
		go d.deliverAsyncPlatform(context.WithoutCancel(ctx), platformCfg, deliveryID, generation, manifests)
		return nil
	}

	if auth.Token == "" {
		return fmt.Errorf("%w: delivery to target %q requires an authenticated caller token", domain.ErrInvalidArgument, target.ID())
	}

	passthroughCfg := rest.CopyConfig(platformCfg)
	passthroughCfg.BearerToken = string(auth.Token)

	go d.deliverAsync(context.WithoutCancel(ctx), passthroughCfg, deliveryID, generation, manifests)
	return nil
}

// remove deletes all manifested resources from the target cluster.
// When an attestation is provided the component verifies it against the
// target's trust bundle and uses platformCfg directly. Otherwise a
// clone is made with the caller's bearer token swapped in.
func (d *deliveryDelegate) remove(ctx context.Context, platformCfg *rest.Config, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, generation domain.Generation) error {
	if target.Properties()["api_server"] == "" {
		return fmt.Errorf("%w: target %q missing api_server property", domain.ErrInvalidArgument, target.ID())
	}

	asyncCtx := context.WithoutCancel(ctx)
	if att != nil {
		v, err := d.verifierForTarget(target)
		if err != nil {
			_ = d.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
				State:   domain.DeliveryStateAuthFailed,
				Message: fmt.Sprintf("build verifier for target %q: %v", target.ID(), err),
			})
			return nil
		}
		if err := v.Verify(ctx, att, generation); err != nil {
			_ = d.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
				State:   domain.DeliveryStateAuthFailed,
				Message: fmt.Sprintf("attestation verification failed: %v", err),
			})
			return nil
		}
		go func() {
			err := d.deleteManifests(asyncCtx, platformCfg, manifests)
			if err != nil {
				_ = d.reporter.ReportResult(asyncCtx, deliveryID, generation, domain.DeliveryResult{
					State: deliveryStateForError(err), Message: err.Error(),
				})
				return
			}
			_ = d.reporter.ReportResult(asyncCtx, deliveryID, generation, domain.DeliveryResult{
				State: domain.DeliveryStateDelivered,
			})
		}()
		return nil
	}

	if auth.Token == "" {
		return fmt.Errorf("%w: removal from target %q requires an authenticated caller token", domain.ErrInvalidArgument, target.ID())
	}

	passthroughCfg := rest.CopyConfig(platformCfg)
	passthroughCfg.BearerToken = string(auth.Token)

	go func() {
		err := d.deleteManifests(asyncCtx, passthroughCfg, manifests)
		if err != nil {
			_ = d.reporter.ReportResult(asyncCtx, deliveryID, generation, domain.DeliveryResult{
				State: deliveryStateForError(err), Message: err.Error(),
			})
			return
		}
		_ = d.reporter.ReportResult(asyncCtx, deliveryID, generation, domain.DeliveryResult{
			State: domain.DeliveryStateDelivered,
		})
	}()
	return nil
}

// verifierForTarget builds or retrieves a cached [attestation.Verifier]
// from the target's trust_bundle property.
func (d *deliveryDelegate) verifierForTarget(target domain.TargetInfo) (*attestation.Verifier, error) {
	trustJSON := target.Properties()["trust_bundle"]
	if trustJSON == "" {
		return nil, fmt.Errorf("target %q has no trust_bundle property", target.ID())
	}

	d.mu.RLock()
	if v, ok := d.verifiers[trustJSON]; ok {
		d.mu.RUnlock()
		return v, nil
	}
	d.mu.RUnlock()

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
	if d.httpClient != nil {
		opts = append(opts, attestation.WithHTTPClient(d.httpClient))
	}
	if d.keyResolver != nil {
		opts = append(opts, attestation.WithKeyResolver(d.keyResolver))
	}
	v := attestation.NewVerifier(issuers, opts...)

	d.mu.Lock()
	d.verifiers[trustJSON] = v
	d.mu.Unlock()

	return v, nil
}

// deliverAsyncPlatform applies manifests using the pre-built platform
// REST config. Called after attestation verification passes.
func (d *deliveryDelegate) deliverAsyncPlatform(ctx context.Context, cfg *rest.Config, deliveryID domain.DeliveryID, generation domain.Generation, manifests []domain.Manifest) {
	d.applyManifests(ctx, deliveryID, generation, cfg, manifests)
}

// deliverAsync applies manifests using a pre-built REST config (already
// cloned with the caller's bearer token by the caller).
func (d *deliveryDelegate) deliverAsync(ctx context.Context, cfg *rest.Config, deliveryID domain.DeliveryID, generation domain.Generation, manifests []domain.Manifest) {
	d.applyManifests(ctx, deliveryID, generation, cfg, manifests)
}

// applyManifests applies each manifest via server-side apply, reporting
// progress events and the final result through the delivery reporter.
func (d *deliveryDelegate) applyManifests(ctx context.Context, deliveryID domain.DeliveryID, generation domain.Generation, cfg *rest.Config, manifests []domain.Manifest) {
	ap, err := newApplierFromConfig(cfg)
	if err != nil {
		_ = d.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
			State:   deliveryStateForError(err),
			Message: fmt.Sprintf("build kubernetes client: %v", err),
		})
		return
	}

	for i, m := range manifests {
		_ = d.reporter.ReportEvent(ctx, deliveryID, generation, domain.DeliveryEvent{
			Kind:    domain.DeliveryEventProgress,
			Message: fmt.Sprintf("Applying manifest %d/%d", i+1, len(manifests)),
		})

		if err := ap.apply(ctx, m.Raw); err != nil {
			_ = d.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
				State:   deliveryStateForError(err),
				Message: fmt.Sprintf("apply manifest %d: %v", i+1, err),
			})
			return
		}
	}

	_ = d.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
}

// deleteManifests deletes Kubernetes resources described by manifests.
// Resources that are already gone (404) are silently skipped.
func (d *deliveryDelegate) deleteManifests(ctx context.Context, cfg *rest.Config, manifests []domain.Manifest) error {
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
