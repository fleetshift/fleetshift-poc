package serverapp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc"
)

// KeySetRegistrar registers JWKS URIs required for token verification.
// Production verifiers implement this. Startup registration is best-effort;
// Verify retries registration on demand when keys were not cached at boot.
type KeySetRegistrar interface {
	RegisterKeySet(ctx context.Context, jwksURI domain.EndpointURL) error
}

// Identity couples OIDC discovery with token verification (and required
// key-set registration when the verifier implements KeySetRegistrar).
type Identity struct {
	Discovery domain.OIDCDiscoveryClient
	Verifier  domain.OIDCTokenVerifier
}

// NewProductionIdentity builds the production discovery client and verifier.
// oidcCABundle is optional PEM material for a custom trust store.
func NewProductionIdentity(ctx context.Context, oidcCABundle []byte) (Identity, error) {
	var oidcHTTPClient *http.Client
	if len(oidcCABundle) > 0 {
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		pool.AppendCertsFromPEM(oidcCABundle)
		oidcHTTPClient = &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: pool},
			},
		}
	}

	discoveryClient := oidc.NewDiscoveryClient(oidcHTTPClient)

	var verifierOpts []oidc.VerifierOption
	if oidcHTTPClient != nil {
		verifierOpts = append(verifierOpts, oidc.WithHTTPClient(oidcHTTPClient))
	}
	tokenVerifier, err := oidc.NewVerifier(ctx, verifierOpts...)
	if err != nil {
		return Identity{}, fmt.Errorf("create OIDC verifier: %w", err)
	}
	return Identity{
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
