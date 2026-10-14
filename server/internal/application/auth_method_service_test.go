package application_test

import (
	"context"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
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

func TestAuthMethodService_PublicCreateRejected(t *testing.T) {
	ctx := context.Background()
	sqlDB := sqlite.OpenTestDB(t)
	authRepo := &sqlite.AuthMethodRepo{DB: sqlDB}

	svc := &application.AuthMethodService{Methods: authRepo}
	_, err := svc.Create(ctx, "default", domain.NewOIDCAuthMethod("default", &domain.OIDCConfig{
		IssuerURL: "https://example.com",
		Audience:  "fleetshift",
	}))
	if err == nil {
		t.Fatal("expected public Create rejection on empty store")
	}
	if !strings.Contains(err.Error(), "public AuthMethod create is disabled") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAuthMethodService_InstallFirst_RejectsEmptyID(t *testing.T) {
	ctx := context.Background()
	sqlDB := sqlite.OpenTestDB(t)
	authRepo := &sqlite.AuthMethodRepo{DB: sqlDB}
	svc := &application.AuthMethodService{
		Methods: authRepo,
		ProvisionWF: recordingProvisionWF{startFn: func(context.Context, domain.ProvisionIdPInput) (domain.Execution[domain.AuthMethod], error) {
			t.Fatal("ProvisionWF must not start for empty ID")
			return nil, nil
		}},
	}
	_, err := svc.InstallFirst(ctx, "", domain.NewOIDCAuthMethod("", &domain.OIDCConfig{
		IssuerURL: "https://example.com",
	}))
	if err == nil {
		t.Fatal("expected empty ID error")
	}
	if !strings.Contains(err.Error(), "auth method ID is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAuthMethodService_InstallFirst_RejectsNonEmptyStore(t *testing.T) {
	ctx := context.Background()
	sqlDB := sqlite.OpenTestDB(t)
	authRepo := &sqlite.AuthMethodRepo{DB: sqlDB}
	existing := domain.NewOIDCAuthMethod("default", &domain.OIDCConfig{
		IssuerURL: "https://example.com",
		Audience:  "fleetshift",
	})
	if err := authRepo.Save(ctx, existing); err != nil {
		t.Fatal(err)
	}

	started := false
	svc := &application.AuthMethodService{
		Methods: authRepo,
		ProvisionWF: recordingProvisionWF{startFn: func(context.Context, domain.ProvisionIdPInput) (domain.Execution[domain.AuthMethod], error) {
			started = true
			return nil, nil
		}},
	}
	_, err := svc.InstallFirst(ctx, "other", domain.NewOIDCAuthMethod("other", &domain.OIDCConfig{
		IssuerURL: "https://other.example",
	}))
	if err == nil {
		t.Fatal("expected non-empty store error")
	}
	if !strings.Contains(err.Error(), "AuthMethod store is not empty") {
		t.Fatalf("unexpected error: %v", err)
	}
	if started {
		t.Fatal("ProvisionWF must not start when store is non-empty")
	}
}

func TestAuthMethodService_InstallFirst_Success(t *testing.T) {
	ctx := context.Background()
	sqlDB := sqlite.OpenTestDB(t)
	authRepo := &sqlite.AuthMethodRepo{DB: sqlDB}

	input := domain.NewOIDCAuthMethod("default", &domain.OIDCConfig{
		IssuerURL: "https://example.com",
		Audience:  "fleetshift",
	})
	want := domain.NewOIDCAuthMethod("default", &domain.OIDCConfig{
		IssuerURL:             "https://example.com",
		Audience:              "fleetshift",
		AuthorizationEndpoint: "https://example.com/auth",
		TokenEndpoint:         "https://example.com/token",
		JWKSURI:               "https://example.com/keys",
	})

	var gotInput domain.ProvisionIdPInput
	svc := &application.AuthMethodService{
		Methods: authRepo,
		ProvisionWF: recordingProvisionWF{startFn: func(_ context.Context, in domain.ProvisionIdPInput) (domain.Execution[domain.AuthMethod], error) {
			gotInput = in
			return immediateExec{result: want}, nil
		}},
	}
	got, err := svc.InstallFirst(ctx, "default", input)
	if err != nil {
		t.Fatalf("InstallFirst: %v", err)
	}
	if gotInput.AuthMethodID != "default" {
		t.Fatalf("ProvisionIdPInput.AuthMethodID = %q, want default", gotInput.AuthMethodID)
	}
	if got.ID() != want.ID() {
		t.Fatalf("result ID = %q, want %q", got.ID(), want.ID())
	}
	if got.OIDC() == nil || got.OIDC().JWKSURI != want.OIDC().JWKSURI {
		t.Fatalf("result OIDC = %#v, want JWKSURI %q", got.OIDC(), want.OIDC().JWKSURI)
	}
}
