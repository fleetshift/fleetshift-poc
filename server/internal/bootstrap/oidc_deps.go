package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc"
)

// KeySetRegistrar registers JWKS URIs required for token verification.
// Optional capability on a Verifier; not all verifiers implement it.
type KeySetRegistrar interface {
	// RegisterKeySet registers jwksURI for subsequent token verification.
	// ctx bounds network I/O. A non-nil error means keys were not published;
	// callers may retry later. Success is idempotent when already registered.
	RegisterKeySet(ctx context.Context, jwksURI domain.EndpointURL) error
}

// OIDCDeps holds OIDC discovery and token verification dependencies for Start.
type OIDCDeps struct {
	Discovery domain.OIDCDiscoveryClient
	Verifier  domain.OIDCTokenVerifier
}

// NewProductionOIDCDeps builds the production discovery client and verifier.
// oidcHTTPClient is optional; nil uses the system trust store / default client.
func NewProductionOIDCDeps(ctx context.Context, oidcHTTPClient *http.Client) (OIDCDeps, error) {
	discoveryClient := oidc.NewDiscoveryClient(oidcHTTPClient)

	var verifierOpts []oidc.VerifierOption
	if oidcHTTPClient != nil {
		verifierOpts = append(verifierOpts, oidc.WithHTTPClient(oidcHTTPClient))
	}
	tokenVerifier, err := oidc.NewVerifier(ctx, verifierOpts...)
	if err != nil {
		return OIDCDeps{}, fmt.Errorf("create OIDC verifier: %w", err)
	}
	return OIDCDeps{
		Discovery: discoveryClient,
		Verifier:  tokenVerifier,
	}, nil
}

// registerPersistedKeySets attempts JWKS registration for every persisted OIDC
// auth method. Failures are logged and ignored so an unavailable IdP does not
// block process start. Each attempt is bounded so a slow/unreachable IdP cannot
// stall Start. The production verifier's Verify path retries registration on
// demand, so IdP recovery restores auth without a restart.
func registerPersistedKeySets(ctx context.Context, logger *slog.Logger, verifier domain.OIDCTokenVerifier, methods []domain.AuthMethod) {
	registrar, ok := verifier.(KeySetRegistrar)
	if !ok {
		return
	}
	for _, m := range methods {
		if m.Type() != domain.AuthMethodTypeOIDC || m.OIDC() == nil {
			continue
		}
		regCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := registrar.RegisterKeySet(regCtx, m.OIDC().JWKSURI)
		cancel()
		if err != nil {
			logger.Warn("failed to register JWKS for auth method", "id", m.ID(), "err", err)
		}
	}
}
