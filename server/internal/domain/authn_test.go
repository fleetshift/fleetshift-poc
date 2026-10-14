package domain_test

import (
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestAuthMethod_Validate_ValidOIDCPasses(t *testing.T) {
	m := domain.AuthMethodFromSnapshot(domain.AuthMethodSnapshot{
		ID:   "am1",
		Type: domain.AuthMethodTypeOIDC,
		OIDC: &domain.OIDCConfig{
			IssuerURL:             "https://issuer.example.com",
			Audience:              "aud",
			JWKSURI:               "https://issuer.example.com/.well-known/jwks.json",
			AuthorizationEndpoint: "https://issuer.example.com/authorize",
			TokenEndpoint:         "https://issuer.example.com/token",
		},
	})
	if err := m.Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil", err)
	}
}

func TestAuthMethod_Validate_OIDCWithoutConfigReturnsErrInvalidArgument(t *testing.T) {
	m := domain.AuthMethodFromSnapshot(domain.AuthMethodSnapshot{
		ID:   "am1",
		Type: domain.AuthMethodTypeOIDC,
		OIDC: nil,
	})
	err := m.Validate()
	if err == nil {
		t.Fatal("Validate() = nil, want ErrInvalidArgument")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("Validate() = %v, want ErrInvalidArgument (errors.Is)", err)
	}
}

func TestAuthMethod_Validate_UnknownTypeReturnsErrInvalidArgument(t *testing.T) {
	m := domain.AuthMethodFromSnapshot(domain.AuthMethodSnapshot{
		ID:   "am1",
		Type: domain.AuthMethodType("unknown"),
	})
	err := m.Validate()
	if err == nil {
		t.Fatal("Validate() = nil, want ErrInvalidArgument")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("Validate() = %v, want ErrInvalidArgument (errors.Is)", err)
	}
}
