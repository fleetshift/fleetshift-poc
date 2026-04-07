package domain_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestProvisionIdPWorkflowSpec_Run(t *testing.T) {
	var savedMethod domain.AuthMethod
	var startedInput domain.CreateDeploymentInput

	spec := &domain.ProvisionIdPWorkflowSpec{
		AuthMethods: &fakeAuthMethodRepo{saveFn: func(_ context.Context, m domain.AuthMethod) error {
			savedMethod = m
			return nil
		}},
		Discovery: fakeDiscovery{},
		CreateDeployment: &fakeCreateDeploymentWF{startFn: func(_ context.Context, in domain.CreateDeploymentInput) (domain.Execution[domain.Deployment], error) {
			startedInput = in
			return &immediateExecution[domain.Deployment]{val: domain.Deployment{ID: in.ID}}, nil
		}},
		TrustBundlePlacement: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"kind-local"},
		},
	}

	input := domain.ProvisionIdPInput{
		AuthMethodID: "test-idp",
		AuthMethod: domain.AuthMethod{
			Type: domain.AuthMethodTypeOIDC,
			OIDC: &domain.OIDCConfig{
				IssuerURL:             "https://issuer.example.com",
				Audience:              "fleetshift",
				KeyEnrollmentAudience: "fleetshift-enroll",
				RegistrySubjectMapping: &domain.RegistrySubjectMapping{
					RegistryID: "github.com",
					Expression: "claims.preferred_username",
				},
			},
		},
	}

	record := &provisionSyncRecord{}
	result, err := spec.Run(record, input)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if result.ID != "test-idp" {
		t.Errorf("result.ID = %q, want %q", result.ID, "test-idp")
	}
	if result.OIDC.JWKSURI != "https://issuer.example.com/jwks" {
		t.Errorf("JWKSURI = %q, want discovery-resolved value", result.OIDC.JWKSURI)
	}

	if savedMethod.ID != "test-idp" {
		t.Errorf("saved method ID = %q, want %q", savedMethod.ID, "test-idp")
	}

	if startedInput.ID != "idp-trust-test-idp" {
		t.Errorf("deployment ID = %q, want %q", startedInput.ID, "idp-trust-test-idp")
	}
	if len(startedInput.ManifestStrategy.Manifests) != 1 {
		t.Fatalf("manifests len = %d, want 1", len(startedInput.ManifestStrategy.Manifests))
	}
	m := startedInput.ManifestStrategy.Manifests[0]
	if m.ResourceType != domain.TrustBundleResourceType {
		t.Errorf("resource type = %q, want %q", m.ResourceType, domain.TrustBundleResourceType)
	}

	var entry domain.TrustBundleEntry
	if err := json.Unmarshal(m.Raw, &entry); err != nil {
		t.Fatalf("unmarshal trust bundle: %v", err)
	}
	if entry.IssuerURL != "https://issuer.example.com" {
		t.Errorf("trust bundle issuer = %q", entry.IssuerURL)
	}
	if entry.JWKSURI != "https://issuer.example.com/jwks" {
		t.Errorf("trust bundle JWKSURI = %q", entry.JWKSURI)
	}
	if entry.EnrollmentAudience != "fleetshift-enroll" {
		t.Errorf("trust bundle enrollment audience = %q", entry.EnrollmentAudience)
	}
	if entry.RegistrySubjectMapping == nil || entry.RegistrySubjectMapping.Expression != "claims.preferred_username" {
		t.Errorf("trust bundle registry subject mapping = %+v", entry.RegistrySubjectMapping)
	}

	if startedInput.PlacementStrategy.Type != domain.PlacementStrategyStatic {
		t.Errorf("placement type = %q, want static", startedInput.PlacementStrategy.Type)
	}
}

func TestProvisionIdPWorkflowSpec_Run_InvalidMethod(t *testing.T) {
	spec := &domain.ProvisionIdPWorkflowSpec{
		AuthMethods: &fakeAuthMethodRepo{},
		Discovery:   fakeDiscovery{},
	}

	input := domain.ProvisionIdPInput{
		AuthMethodID: "bad",
		AuthMethod:   domain.AuthMethod{Type: "unknown"},
	}

	record := &provisionSyncRecord{}
	_, err := spec.Run(record, input)
	if err == nil {
		t.Fatal("expected error for invalid auth method")
	}
}

// --- test doubles ---

type fakeAuthMethodRepo struct {
	saveFn func(context.Context, domain.AuthMethod) error
}

func (f *fakeAuthMethodRepo) Save(ctx context.Context, m domain.AuthMethod) error {
	if f.saveFn != nil {
		return f.saveFn(ctx, m)
	}
	return nil
}

func (f *fakeAuthMethodRepo) Get(_ context.Context, _ domain.AuthMethodID) (domain.AuthMethod, error) {
	return domain.AuthMethod{}, domain.ErrNotFound
}

func (f *fakeAuthMethodRepo) List(_ context.Context) ([]domain.AuthMethod, error) {
	return nil, nil
}

type fakeDiscovery struct{}

func (fakeDiscovery) FetchMetadata(_ context.Context, issuerURL domain.IssuerURL) (domain.OIDCMetadata, error) {
	return domain.OIDCMetadata{
		Issuer:                issuerURL,
		AuthorizationEndpoint: domain.EndpointURL(string(issuerURL) + "/authorize"),
		TokenEndpoint:         domain.EndpointURL(string(issuerURL) + "/token"),
		JWKSURI:               domain.EndpointURL(string(issuerURL) + "/jwks"),
	}, nil
}

type fakeCreateDeploymentWF struct {
	startFn func(context.Context, domain.CreateDeploymentInput) (domain.Execution[domain.Deployment], error)
}

func (f *fakeCreateDeploymentWF) Start(ctx context.Context, in domain.CreateDeploymentInput) (domain.Execution[domain.Deployment], error) {
	if f.startFn != nil {
		return f.startFn(ctx, in)
	}
	return &immediateExecution[domain.Deployment]{val: domain.Deployment{ID: in.ID}}, nil
}

// provisionSyncRecord is a synchronous [domain.Record] that runs
// activities inline for unit testing workflow specs.
type provisionSyncRecord struct {
	id string
}

func (r *provisionSyncRecord) ID() string               { return r.id }
func (r *provisionSyncRecord) Context() context.Context { return context.Background() }

func (r *provisionSyncRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	return activity.Run(context.Background(), in)
}

func (r *provisionSyncRecord) Await(_ string) (any, error) {
	return nil, fmt.Errorf("provisionSyncRecord: Await not supported")
}
