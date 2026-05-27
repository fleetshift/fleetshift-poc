package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

type stubClusterAccessProvider struct {
	mintFunc func(ctx context.Context, callerToken string, target domain.TargetInfo) (*domain.ClusterCredential, error)
}

func (s *stubClusterAccessProvider) MintCredential(ctx context.Context, callerToken string, target domain.TargetInfo) (*domain.ClusterCredential, error) {
	return s.mintFunc(ctx, callerToken, target)
}

func setupClusterService(t *testing.T) (*application.ClusterService, *application.TargetService, *application.ClusterAccessRegistry) {
	t.Helper()
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	targetSvc := &application.TargetService{Store: store}
	reg := application.NewClusterAccessRegistry()
	svc := &application.ClusterService{
		Targets:   targetSvc,
		Providers: reg,
	}
	return svc, targetSvc, reg
}

func TestClusterService_GetConnectionInfo_Success(t *testing.T) {
	svc, targetSvc, _ := setupClusterService(t)
	ctx := context.Background()

	if err := targetSvc.Register(ctx, domain.TargetInfo{
		ID:   "k8s-my-cluster",
		Type: "kubernetes",
		Name: "my-cluster",
		Properties: map[string]string{
			"api_server": "https://api.example.com:6443",
			"ca_cert":    "FAKE-CA-DATA",
		},
	}); err != nil {
		t.Fatalf("register target: %v", err)
	}

	endpoint, caCert, err := svc.GetConnectionInfo(ctx, "my-cluster")
	if err != nil {
		t.Fatalf("GetConnectionInfo: %v", err)
	}
	if endpoint != "https://api.example.com:6443" {
		t.Errorf("endpoint = %q, want https://api.example.com:6443", endpoint)
	}
	if caCert != "FAKE-CA-DATA" {
		t.Errorf("caCert = %q, want FAKE-CA-DATA", caCert)
	}
}

func TestClusterService_GetConnectionInfo_NoCaCert(t *testing.T) {
	svc, targetSvc, _ := setupClusterService(t)
	ctx := context.Background()

	if err := targetSvc.Register(ctx, domain.TargetInfo{
		ID:         "k8s-no-ca",
		Type:       "kubernetes",
		Name:       "no-ca",
		Properties: map[string]string{"api_server": "https://api.example.com:6443"},
	}); err != nil {
		t.Fatalf("register target: %v", err)
	}

	endpoint, caCert, err := svc.GetConnectionInfo(ctx, "no-ca")
	if err != nil {
		t.Fatalf("GetConnectionInfo: %v", err)
	}
	if endpoint != "https://api.example.com:6443" {
		t.Errorf("endpoint = %q, want https://api.example.com:6443", endpoint)
	}
	if caCert != "" {
		t.Errorf("caCert = %q, want empty", caCert)
	}
}

func TestClusterService_GetConnectionInfo_TargetNotFound(t *testing.T) {
	svc, _, _ := setupClusterService(t)
	ctx := context.Background()

	_, _, err := svc.GetConnectionInfo(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing target")
	}
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

func TestClusterService_GetConnectionInfo_MissingAPIServer(t *testing.T) {
	svc, targetSvc, _ := setupClusterService(t)
	ctx := context.Background()

	if err := targetSvc.Register(ctx, domain.TargetInfo{
		ID:         "k8s-no-api",
		Type:       "kubernetes",
		Name:       "no-api",
		Properties: map[string]string{"ca_cert": "FAKE"},
	}); err != nil {
		t.Fatalf("register target: %v", err)
	}

	_, _, err := svc.GetConnectionInfo(ctx, "no-api")
	if err == nil {
		t.Fatal("expected error for missing api_server")
	}
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

func TestClusterService_GetCredential_Success(t *testing.T) {
	svc, targetSvc, reg := setupClusterService(t)
	ctx := context.Background()

	if err := targetSvc.Register(ctx, domain.TargetInfo{
		ID:                   "seeded-gcphcp",
		Type:                 "gcphcp",
		Name:                 "GCP Target",
		Properties:           map[string]string{"gcp_project": "my-proj"},
	}); err != nil {
		t.Fatalf("register seeded target: %v", err)
	}

	if err := targetSvc.Register(ctx, domain.TargetInfo{
		ID:                   "k8s-my-cluster",
		Type:                 "kubernetes",
		Name:                 "my-cluster",
		Properties:           map[string]string{"api_server": "https://api.example.com:6443"},
		ProvisioningTargetID: "seeded-gcphcp",
	}); err != nil {
		t.Fatalf("register emitted target: %v", err)
	}

	expiration := time.Now().Add(55 * time.Minute)
	reg.Register("gcphcp", &stubClusterAccessProvider{
		mintFunc: func(_ context.Context, callerToken string, target domain.TargetInfo) (*domain.ClusterCredential, error) {
			if callerToken != "my-bearer-token" {
				t.Errorf("callerToken = %q, want my-bearer-token", callerToken)
			}
			if target.ID != "seeded-gcphcp" {
				t.Errorf("target.ID = %q, want seeded-gcphcp", target.ID)
			}
			return &domain.ClusterCredential{Token: "minted-token", Expiration: expiration}, nil
		},
	})

	cred, err := svc.GetCredential(ctx, "my-cluster", "my-bearer-token")
	if err != nil {
		t.Fatalf("GetCredential: %v", err)
	}
	if cred.Token != "minted-token" {
		t.Errorf("Token = %q, want minted-token", cred.Token)
	}
}

func TestClusterService_GetCredential_TargetNotFound(t *testing.T) {
	svc, _, _ := setupClusterService(t)
	ctx := context.Background()

	_, err := svc.GetCredential(ctx, "nonexistent", "token")
	if err == nil {
		t.Fatal("expected error for missing target")
	}
}

func TestClusterService_GetCredential_NoProvisioningTarget(t *testing.T) {
	svc, targetSvc, _ := setupClusterService(t)
	ctx := context.Background()

	if err := targetSvc.Register(ctx, domain.TargetInfo{
		ID:         "k8s-orphan",
		Type:       "kubernetes",
		Name:       "orphan",
		Properties: map[string]string{"api_server": "https://api.example.com"},
	}); err != nil {
		t.Fatalf("register target: %v", err)
	}

	_, err := svc.GetCredential(ctx, "orphan", "token")
	if err == nil {
		t.Fatal("expected error when ProvisioningTargetID is empty")
	}
}

func TestClusterService_GetCredential_ProvisioningTargetNotFound(t *testing.T) {
	svc, targetSvc, _ := setupClusterService(t)
	ctx := context.Background()

	if err := targetSvc.Register(ctx, domain.TargetInfo{
		ID:                   "k8s-dangling",
		Type:                 "kubernetes",
		Name:                 "dangling",
		Properties:           map[string]string{"api_server": "https://api.example.com"},
		ProvisioningTargetID: "gone-target",
	}); err != nil {
		t.Fatalf("register target: %v", err)
	}

	_, err := svc.GetCredential(ctx, "dangling", "token")
	if err == nil {
		t.Fatal("expected error when provisioning target does not exist")
	}
}

func TestClusterService_GetCredential_NoProviderRegistered(t *testing.T) {
	svc, targetSvc, _ := setupClusterService(t)
	ctx := context.Background()

	if err := targetSvc.Register(ctx, domain.TargetInfo{
		ID:   "seeded-unknown",
		Type: "unknown-type",
		Name: "Unknown",
	}); err != nil {
		t.Fatalf("register seeded target: %v", err)
	}

	if err := targetSvc.Register(ctx, domain.TargetInfo{
		ID:                   "k8s-no-provider",
		Type:                 "kubernetes",
		Name:                 "no-provider",
		Properties:           map[string]string{"api_server": "https://api.example.com"},
		ProvisioningTargetID: "seeded-unknown",
	}); err != nil {
		t.Fatalf("register emitted target: %v", err)
	}

	_, err := svc.GetCredential(ctx, "no-provider", "token")
	if err == nil {
		t.Fatal("expected error when no provider is registered for target type")
	}
}
