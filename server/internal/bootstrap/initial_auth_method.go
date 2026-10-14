package bootstrap

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// initialAuthMethodInstall is a deferred empty-store AuthMethod install. Non-nil
// only when [prepareAuthMethods] finds an empty store.
type initialAuthMethodInstall struct {
	oidc        domain.OIDCConfig
	authMethods *application.AuthMethodService
	verifier    domain.OIDCTokenVerifier
}

// prepareAuthMethods handles AuthMethod startup before the workflow worker.
// When methods already exist, it registers JWKS and returns nil. When the store
// is empty, it validates OIDC initial-install config and returns an
// [initialAuthMethodInstall] that must run after the worker starts so
// ProvisionIdP can complete.
func prepareAuthMethods(
	ctx context.Context,
	cfg Config,
	authMethods *application.AuthMethodService,
	verifier domain.OIDCTokenVerifier,
	logger *slog.Logger,
) (*initialAuthMethodInstall, error) {
	existing, err := authMethods.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("load auth methods: %w", err)
	}

	if len(existing) > 0 {
		if logger != nil {
			logger.Info("AuthMethod already configured, skipping initial AuthMethod setup")
		}
		registerAuthMethodJWKS(ctx, logger, verifier, existing)
		return nil, nil
	}

	if !cfg.OIDCInitialAuthMethodConfigured() {
		return nil, fmt.Errorf("initial AuthMethod install: AuthMethod store is empty and OIDC initial-AuthMethod config is incomplete (require --oidc-issuer and --oidc-resource-audience); refusing to open the public API")
	}

	return &initialAuthMethodInstall{
		oidc:        oidcInitialAuthMethodConfig(cfg),
		authMethods: authMethods,
		verifier:    verifier,
	}, nil
}

// Install installs authMethods/default via ProvisionIdP and registers JWKS.
// Call only after the workflow worker is running. Listeners may be bound but
// the public API must still be closed. The store must still be empty;
// [application.AuthMethodService.InstallFirst] enforces that.
func (s *initialAuthMethodInstall) Install(ctx context.Context, logger *slog.Logger) error {
	if s.authMethods == nil {
		return fmt.Errorf("initial AuthMethod install: AuthMethodService is required")
	}
	if s.oidc.IssuerURL == "" {
		return fmt.Errorf("initial AuthMethod install: %w: OIDC issuer is required when the AuthMethod store is empty",
			domain.ErrInvalidArgument)
	}
	if s.oidc.Audience == "" {
		return fmt.Errorf("initial AuthMethod install: %w: OIDC audience is required when the AuthMethod store is empty",
			domain.ErrInvalidArgument)
	}

	ensureCtx, ensureCancel := context.WithTimeout(ctx, DefaultInitialAuthMethodTimeout)
	defer ensureCancel()

	id := domain.DefaultAuthMethodID
	cfg := s.oidc
	method := domain.NewOIDCAuthMethod(id, &cfg)
	installed, err := s.authMethods.InstallFirst(ensureCtx, id, method)
	if err != nil {
		return fmt.Errorf("initial AuthMethod install: %w", err)
	}
	registerAuthMethodJWKS(ctx, logger, s.verifier, []domain.AuthMethod{installed})
	return nil
}

// oidcInitialAuthMethodConfig maps serve Config OIDC fields into a domain OIDCConfig
// for empty-store initial AuthMethod install.
func oidcInitialAuthMethodConfig(cfg Config) domain.OIDCConfig {
	oidc := domain.OIDCConfig{
		IssuerURL:                domain.IssuerURL(cfg.OIDCIssuer),
		Audience:                 domain.Audience(cfg.OIDCResourceAudience),
		KeyEnrollmentAudience:    domain.Audience(cfg.OIDCKeyEnrollmentAudience),
		PublicKeyClaimExpression: cfg.OIDCPublicKeyClaimExpression,
	}
	if cfg.OIDCRegistryID != "" && cfg.OIDCRegistrySubjectExpression != "" {
		oidc.RegistrySubjectMapping = &domain.RegistrySubjectMapping{
			RegistryID: domain.KeyRegistryID(cfg.OIDCRegistryID),
			Expression: cfg.OIDCRegistrySubjectExpression,
		}
	}
	return oidc
}
