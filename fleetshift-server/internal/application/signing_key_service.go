package application

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// SigningKeyService handles signing key enrollment by validating
// key binding bundles and persisting them.
type SigningKeyService struct {
	Store       domain.Store
	Verifier    domain.OIDCTokenVerifier
	AuthMethods domain.AuthMethodRepository
}

// CreateSigningKeyBindingInput carries the three components of a
// key binding bundle submitted by the client.
type CreateSigningKeyBindingInput struct {
	ID                  domain.SigningKeyBindingID
	KeyBindingDoc       []byte
	KeyBindingSignature []byte
	IdentityToken       string
}

// keyBindingDoc is the canonical JSON structure signed by the user.
type keyBindingDoc struct {
	PublicKeyJWK json.RawMessage `json:"public_key_jwk"`
	Subject      string          `json:"subject"`
	Issuer       string          `json:"issuer"`
	EnrolledAt   string          `json:"enrolled_at"`
}

// Create validates a key binding bundle and stores it. The validation
// steps are:
//
//  1. Caller must be authenticated (Authorization header).
//  2. Load the OIDC auth method to get the enrollment audience.
//  3. Verify the bundle's identity token against IdP JWKS using the
//     enrollment audience.
//  4. Confirm the identity token's sub matches the caller's sub.
//  5. Parse the key binding doc and extract the public key JWK.
//  6. Verify the key binding signature against the public key.
//  7. Persist the SigningKeyBinding.
func (s *SigningKeyService) Create(ctx context.Context, in CreateSigningKeyBindingInput) (domain.SigningKeyBinding, error) {
	ac := AuthFromContext(ctx)
	if ac == nil || ac.Subject == nil {
		return domain.SigningKeyBinding{}, fmt.Errorf(
			"%w: enrolling a signing key requires an authenticated caller",
			domain.ErrInvalidArgument)
	}

	if in.ID == "" {
		return domain.SigningKeyBinding{}, fmt.Errorf(
			"%w: signing_key_binding_id is required",
			domain.ErrInvalidArgument)
	}

	oidcConfig, err := s.loadEnrollmentConfig(ctx)
	if err != nil {
		return domain.SigningKeyBinding{}, err
	}

	idTokenClaims, err := s.verifyIdentityToken(ctx, oidcConfig, in.IdentityToken)
	if err != nil {
		return domain.SigningKeyBinding{}, fmt.Errorf("identity token verification failed: %w", err)
	}

	if idTokenClaims.ID != ac.Subject.ID {
		return domain.SigningKeyBinding{}, fmt.Errorf(
			"%w: identity token subject %q does not match caller %q",
			domain.ErrInvalidArgument, idTokenClaims.ID, ac.Subject.ID)
	}

	var doc keyBindingDoc
	if err := json.Unmarshal(in.KeyBindingDoc, &doc); err != nil {
		return domain.SigningKeyBinding{}, fmt.Errorf(
			"%w: invalid key_binding_doc JSON: %v",
			domain.ErrInvalidArgument, err)
	}

	pubKey, err := ParseECPublicKeyFromJWK(doc.PublicKeyJWK)
	if err != nil {
		return domain.SigningKeyBinding{}, fmt.Errorf(
			"%w: invalid public key in key_binding_doc: %v",
			domain.ErrInvalidArgument, err)
	}

	if err := VerifyECDSASignature(pubKey, in.KeyBindingDoc, in.KeyBindingSignature); err != nil {
		return domain.SigningKeyBinding{}, fmt.Errorf(
			"%w: proof of possession verification failed: %v",
			domain.ErrInvalidArgument, err)
	}

	now := time.Now().UTC()
	binding := domain.SigningKeyBinding{
		ID:                  in.ID,
		SubjectID:           ac.Subject.ID,
		Issuer:              ac.Subject.Issuer,
		PublicKeyJWK:        []byte(doc.PublicKeyJWK),
		Algorithm:           "ES256",
		KeyBindingDoc:       in.KeyBindingDoc,
		KeyBindingSignature: in.KeyBindingSignature,
		IdentityToken:       domain.RawToken(in.IdentityToken),
		CreatedAt:           now,
		ExpiresAt:           now.Add(365 * 24 * time.Hour), // TODO: make configurable
	}

	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return domain.SigningKeyBinding{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := tx.SigningKeyBindings().Create(ctx, binding); err != nil {
		return domain.SigningKeyBinding{}, fmt.Errorf("persist signing key binding: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return domain.SigningKeyBinding{}, fmt.Errorf("commit: %w", err)
	}

	return binding, nil
}

func (s *SigningKeyService) loadEnrollmentConfig(ctx context.Context) (domain.OIDCConfig, error) {
	methods, err := s.AuthMethods.List(ctx)
	if err != nil {
		return domain.OIDCConfig{}, fmt.Errorf("list auth methods: %w", err)
	}

	for _, m := range methods {
		if m.Type == domain.AuthMethodTypeOIDC && m.OIDC != nil {
			if m.OIDC.KeyEnrollmentAudience == "" {
				return domain.OIDCConfig{}, fmt.Errorf(
					"%w: auth method %q has no key_enrollment_audience configured",
					domain.ErrInvalidArgument, m.ID)
			}
			return *m.OIDC, nil
		}
	}

	return domain.OIDCConfig{}, fmt.Errorf(
		"%w: no OIDC auth method configured", domain.ErrInvalidArgument)
}

func (s *SigningKeyService) verifyIdentityToken(ctx context.Context, oidcConfig domain.OIDCConfig, rawToken string) (domain.SubjectClaims, error) {
	enrollmentConfig := domain.OIDCConfig{
		IssuerURL: oidcConfig.IssuerURL,
		Audience:  oidcConfig.KeyEnrollmentAudience,
		JWKSURI:   oidcConfig.JWKSURI,
	}
	return s.Verifier.Verify(ctx, enrollmentConfig, rawToken)
}

