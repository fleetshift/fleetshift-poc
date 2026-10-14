package bootstrap

import (
	"context"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type staticAuthMethodLister struct {
	methods []domain.AuthMethod
	err     error
}

func (s staticAuthMethodLister) List(context.Context) ([]domain.AuthMethod, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.methods, nil
}

func TestUIAuthFunc(t *testing.T) {
	oidcReady := domain.NewOIDCAuthMethod("default", &domain.OIDCConfig{
		IssuerURL:             "https://issuer.example/dex",
		AuthorizationEndpoint: "https://issuer.example/dex/auth",
		Audience:              "fleetshift",
	})
	oidcMissingAuthz := domain.NewOIDCAuthMethod("default", &domain.OIDCConfig{
		IssuerURL: "https://issuer.example/dex",
		Audience:  "fleetshift",
	})

	tests := []struct {
		name       string
		lister     staticAuthMethodLister
		wantAuth   string
		wantAuthz  string
		wantConfig bool
		wantErr    string
	}{
		{
			name:       "empty store is unconfigured",
			lister:     staticAuthMethodLister{},
			wantConfig: false,
		},
		{
			name:       "one complete OIDC method",
			lister:     staticAuthMethodLister{methods: []domain.AuthMethod{oidcReady}},
			wantAuth:   "https://issuer.example/dex",
			wantAuthz:  "https://issuer.example/dex/auth",
			wantConfig: true,
		},
		{
			name:    "multiple methods unavailable",
			lister:  staticAuthMethodLister{methods: []domain.AuthMethod{oidcReady, oidcReady}},
			wantErr: "at most one auth method",
		},
		{
			name:    "OIDC missing authorization endpoint",
			lister:  staticAuthMethodLister{methods: []domain.AuthMethod{oidcMissingAuthz}},
			wantErr: "missing issuer or authorization endpoint",
		},
		{
			name:    "list error propagates",
			lister:  staticAuthMethodLister{err: context.Canceled},
			wantErr: "canceled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAuth, gotAuthz, gotConfig, err := uiAuthFunc(tt.lister)(context.Background())
			if tt.wantErr != "" {
				if err == nil {
					t.Fatal("expected error")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotAuth != tt.wantAuth || gotAuthz != tt.wantAuthz || gotConfig != tt.wantConfig {
				t.Fatalf("auth=(%q,%q,%v), want (%q,%q,%v)",
					gotAuth, gotAuthz, gotConfig, tt.wantAuth, tt.wantAuthz, tt.wantConfig)
			}
		})
	}
}
