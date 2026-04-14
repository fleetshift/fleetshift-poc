package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ProvisionIdPInput is the specification for provisioning a new
// identity provider. The workflow resolves OIDC discovery, persists
// the auth method, and creates a trust-bundle deployment.
type ProvisionIdPInput struct {
	AuthMethodID AuthMethodID
	AuthMethod   AuthMethod
}

// ProvisionIdPWorkflowSpec is a durable workflow that atomically
// persists an auth method and starts a trust-bundle deployment to
// distribute the IdP's trust configuration to delivery agents.
//
// Pass this spec to [Registry.RegisterProvisionIdP] to obtain a
// [ProvisionIdPWorkflow] that can start instances.
type ProvisionIdPWorkflowSpec struct {
	AuthMethods          AuthMethodRepository
	Discovery            OIDCDiscoveryClient
	CreateDeployment     CreateDeploymentWorkflow
	TrustBundlePlacement PlacementStrategySpec
	Now                  func() time.Time
}

func (s *ProvisionIdPWorkflowSpec) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Name returns the stable workflow name for registry registration.
func (s *ProvisionIdPWorkflowSpec) Name() string { return "provision-idp" }

// ResolveAndPersist returns an activity that runs OIDC discovery and
// saves the auth method. This is idempotent: repeated execution
// overwrites the same auth method ID.
func (s *ProvisionIdPWorkflowSpec) ResolveAndPersist() Activity[ProvisionIdPInput, AuthMethod] {
	return NewActivity("resolve-and-persist-auth-method", func(ctx context.Context, in ProvisionIdPInput) (AuthMethod, error) {
		method := in.AuthMethod
		method.ID = in.AuthMethodID

		if err := method.Validate(); err != nil {
			return AuthMethod{}, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
		}

		switch method.Type {
		case AuthMethodTypeOIDC:
			if method.OIDC.IssuerURL == "" {
				return AuthMethod{}, fmt.Errorf("%w: issuer_url is required", ErrInvalidArgument)
			}
			meta, err := s.Discovery.FetchMetadata(ctx, method.OIDC.IssuerURL)
			if err != nil {
				return AuthMethod{}, fmt.Errorf("fetch OIDC discovery: %w", err)
			}
			method.OIDC.JWKSURI = meta.JWKSURI
			method.OIDC.AuthorizationEndpoint = meta.AuthorizationEndpoint
			method.OIDC.TokenEndpoint = meta.TokenEndpoint
		}

		if err := s.AuthMethods.Save(ctx, method); err != nil {
			return AuthMethod{}, fmt.Errorf("save auth method: %w", err)
		}
		return method, nil
	})
}

// DeployTrustBundle returns an activity that creates a deployment
// carrying the IdP's trust configuration as an idp-trust-bundle
// manifest. The deployment is fire-and-forget: orchestration handles
// delivery independently.
func (s *ProvisionIdPWorkflowSpec) DeployTrustBundle() Activity[AuthMethod, struct{}] {
	return NewActivity("deploy-trust-bundle", func(ctx context.Context, method AuthMethod) (struct{}, error) {
		if method.Type != AuthMethodTypeOIDC || method.OIDC == nil {
			return struct{}{}, nil
		}

		entry := TrustBundleEntry{
			IssuerURL:              method.OIDC.IssuerURL,
			JWKSURI:                method.OIDC.JWKSURI,
			EnrollmentAudience:     method.OIDC.KeyEnrollmentAudience,
			RegistrySubjectMapping: method.OIDC.RegistrySubjectMapping,
		}

		raw, err := json.Marshal(entry)
		if err != nil {
			return struct{}{}, fmt.Errorf("marshal trust bundle entry: %w", err)
		}

		input := CreateDeploymentInput{
			ID: DeploymentID("idp-trust-" + string(method.ID)),
			ManifestStrategy: ManifestStrategySpec{
				Type: ManifestStrategyInline,
				Manifests: []Manifest{{
					ResourceType: TrustBundleResourceType,
					Raw:          raw,
				}},
			},
			PlacementStrategy: s.TrustBundlePlacement,
		}

		_, err = s.CreateDeployment.Start(ctx, input)
		return struct{}{}, err
	})
}

// Run is the workflow body: resolve and persist the auth method, then
// deploy the trust bundle.
func (s *ProvisionIdPWorkflowSpec) Run(record Record, input ProvisionIdPInput) (AuthMethod, error) {
	method, err := RunActivity(record, s.ResolveAndPersist(), input)
	if err != nil {
		return AuthMethod{}, fmt.Errorf("resolve and persist auth method: %w", err)
	}

	if _, err := RunActivity(record, s.DeployTrustBundle(), method); err != nil {
		return AuthMethod{}, fmt.Errorf("deploy trust bundle: %w", err)
	}

	return method, nil
}
