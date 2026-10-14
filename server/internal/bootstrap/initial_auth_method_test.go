package bootstrap

import (
	"context"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

type recordingProvisionWF struct {
	startFn func(ctx context.Context, in domain.ProvisionIdPInput) (domain.Execution[domain.AuthMethod], error)
}

func (r recordingProvisionWF) Start(ctx context.Context, in domain.ProvisionIdPInput) (domain.Execution[domain.AuthMethod], error) {
	return r.startFn(ctx, in)
}

type immediateExec struct {
	result domain.AuthMethod
}

func (e immediateExec) WorkflowID() string { return "test-provision" }

func (e immediateExec) AwaitResult(context.Context) (domain.AuthMethod, error) {
	return e.result, nil
}

func testInitialAuthMethodOIDC(issuer domain.IssuerURL) domain.OIDCConfig {
	return domain.OIDCConfig{
		IssuerURL:             issuer,
		Audience:              "fleetshift",
		KeyEnrollmentAudience: "fleetshift-signing",
		RegistrySubjectMapping: &domain.RegistrySubjectMapping{
			RegistryID: "github.com",
			Expression: "claims.preferred_username",
		},
	}
}

func TestInitialAuthMethodInstall_InstallsDefault(t *testing.T) {
	ctx := context.Background()
	sqlDB := sqlite.OpenTestDB(t)
	authRepo := &sqlite.AuthMethodRepo{DB: sqlDB}
	provider := oidctest.Start(t)

	creates := 0
	authSvc := &application.AuthMethodService{
		Methods: authRepo,
		ProvisionWF: recordingProvisionWF{startFn: func(_ context.Context, in domain.ProvisionIdPInput) (domain.Execution[domain.AuthMethod], error) {
			creates++
			issuer := provider.IssuerURL()
			oidc := in.AuthMethod.OIDC()
			if oidc == nil {
				t.Fatal("expected OIDC auth method input")
			}
			if oidc.Audience != "fleetshift" {
				t.Fatalf("input audience = %q, want fleetshift", oidc.Audience)
			}
			if oidc.RegistrySubjectMapping == nil || oidc.RegistrySubjectMapping.RegistryID != "github.com" {
				t.Fatalf("input registry mapping = %+v", oidc.RegistrySubjectMapping)
			}
			method := domain.NewOIDCAuthMethod(in.AuthMethodID, &domain.OIDCConfig{
				IssuerURL:              issuer,
				Audience:               oidc.Audience,
				KeyEnrollmentAudience:  oidc.KeyEnrollmentAudience,
				RegistrySubjectMapping: oidc.RegistrySubjectMapping,
				AuthorizationEndpoint:  domain.EndpointURL(string(issuer) + "/auth"),
				TokenEndpoint:          domain.EndpointURL(string(issuer) + "/token"),
				JWKSURI:                domain.EndpointURL(string(issuer) + "/keys"),
			})
			if err := authRepo.Save(ctx, method); err != nil {
				return nil, err
			}
			return immediateExec{result: method}, nil
		}},
	}

	install := &initialAuthMethodInstall{
		oidc:        testInitialAuthMethodOIDC(provider.IssuerURL()),
		authMethods: authSvc,
	}
	if err := install.Install(ctx, nil); err != nil {
		t.Fatalf("install: %v", err)
	}
	if creates != 1 {
		t.Fatalf("creates = %d, want 1", creates)
	}
	got, err := authRepo.Get(ctx, domain.DefaultAuthMethodID)
	if err != nil {
		t.Fatal(err)
	}
	if got.OIDC() == nil || got.OIDC().Audience != "fleetshift" {
		t.Fatalf("persisted audience = %v, want fleetshift", got.OIDC())
	}
	if got.OIDC().RegistrySubjectMapping == nil || got.OIDC().RegistrySubjectMapping.RegistryID != "github.com" {
		t.Fatalf("persisted registry mapping = %+v", got.OIDC().RegistrySubjectMapping)
	}
	if err := install.Install(ctx, nil); err == nil {
		t.Fatal("expected second install to fail when store is not empty")
	}
	if creates != 1 {
		t.Fatalf("creates after failed reinstall = %d, want 1", creates)
	}
}

func TestInitialAuthMethodInstall_RequiresIssuerWhenEmpty(t *testing.T) {
	ctx := context.Background()
	sqlDB := sqlite.OpenTestDB(t)
	authRepo := &sqlite.AuthMethodRepo{DB: sqlDB}
	install := &initialAuthMethodInstall{
		oidc:        domain.OIDCConfig{Audience: "fleetshift"},
		authMethods: &application.AuthMethodService{Methods: authRepo},
	}
	err := install.Install(ctx, nil)
	if err == nil {
		t.Fatal("expected error when issuer is empty")
	}
	if !strings.Contains(err.Error(), "OIDC issuer is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInitialAuthMethodInstall_RequiresAudienceWhenEmpty(t *testing.T) {
	ctx := context.Background()
	sqlDB := sqlite.OpenTestDB(t)
	authRepo := &sqlite.AuthMethodRepo{DB: sqlDB}
	install := &initialAuthMethodInstall{
		oidc:        domain.OIDCConfig{IssuerURL: "https://example.com"},
		authMethods: &application.AuthMethodService{Methods: authRepo},
	}
	err := install.Install(ctx, nil)
	if err == nil {
		t.Fatal("expected error when audience is empty")
	}
	if !strings.Contains(err.Error(), "OIDC audience is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}
