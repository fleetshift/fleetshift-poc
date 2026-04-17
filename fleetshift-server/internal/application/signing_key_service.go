package application

import (
	"context"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// SignerEnrollmentService handles signer enrollment by verifying the
// ID token, evaluating the CEL claim mapping to derive the registry
// subject, and persisting the enrollment.
type SignerEnrollmentService struct {
	Store       domain.Store
	Verifier    domain.OIDCTokenVerifier
	AuthMethods domain.AuthMethodRepository
}

// CreateSignerEnrollmentInput carries the data submitted by the
// client to enroll a signer.
type CreateSignerEnrollmentInput struct {
	ID            domain.SignerEnrollmentID
	IdentityToken string
}

// Create validates the enrollment ID token, evaluates the configured
// CEL claim mapping to derive the registry subject, and persists a
// [domain.SignerEnrollment].
//
// The validation steps are:
//  1. Caller must be authenticated (Authorization header).
//  2. Load the OIDC auth method to get the enrollment audience and
//     registry subject mapping.
//  3. Verify the ID token against IdP JWKS using the enrollment
//     audience.
//  4. Confirm the identity token's sub matches the caller's sub.
//  5. Evaluate the CEL claim mapping to derive the registry subject.
//  6. Persist the SignerEnrollment.
func (s *SignerEnrollmentService) Create(ctx context.Context, in CreateSignerEnrollmentInput) (domain.SignerEnrollment, error) {
	ac := AuthFromContext(ctx)
	if ac == nil || ac.Subject == nil {
		return domain.SignerEnrollment{}, fmt.Errorf(
			"%w: enrolling a signer requires an authenticated caller",
			domain.ErrInvalidArgument)
	}

	if in.ID == "" {
		return domain.SignerEnrollment{}, fmt.Errorf(
			"%w: signer_enrollment_id is required",
			domain.ErrInvalidArgument)
	}

	oidcConfig, err := s.loadEnrollmentConfig(ctx)
	if err != nil {
		return domain.SignerEnrollment{}, err
	}

	mapping := oidcConfig.RegistrySubjectMapping
	if mapping == nil {
		return domain.SignerEnrollment{}, fmt.Errorf(
			"%w: no registry subject mapping configured on OIDC auth method",
			domain.ErrInvalidArgument)
	}

	idTokenClaims, err := s.verifyIdentityToken(ctx, oidcConfig, in.IdentityToken)
	if err != nil {
		return domain.SignerEnrollment{}, fmt.Errorf("identity token verification failed: %w", err)
	}

	if idTokenClaims.Subject != ac.Subject.Subject {
		return domain.SignerEnrollment{}, fmt.Errorf(
			"%w: identity token subject %q does not match caller %q",
			domain.ErrInvalidArgument, idTokenClaims.Subject, ac.Subject.Subject)
	}

	registrySubject, err := EvalClaimMapping(mapping, in.IdentityToken)
	if err != nil {
		return domain.SignerEnrollment{}, fmt.Errorf(
			"%w: claim mapping evaluation failed: %v",
			domain.ErrInvalidArgument, err)
	}

	registryID := mapping.RegistryID

	now := time.Now().UTC()
	enrollment := domain.SignerEnrollment{
		ID:                in.ID,
		FederatedIdentity: ac.Subject.FederatedIdentity,
		IdentityToken:     domain.RawToken(in.IdentityToken),
		RegistrySubject:   registrySubject,
		RegistryID:        registryID,
		CreatedAt:         now,
		ExpiresAt:         now.Add(365 * 24 * time.Hour), // TODO: make configurable
	}

	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return domain.SignerEnrollment{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := tx.SignerEnrollments().Create(ctx, enrollment); err != nil {
		return domain.SignerEnrollment{}, fmt.Errorf("persist signer enrollment: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return domain.SignerEnrollment{}, fmt.Errorf("commit: %w", err)
	}

	return enrollment, nil
}

func (s *SignerEnrollmentService) loadEnrollmentConfig(ctx context.Context) (domain.OIDCConfig, error) {
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

func (s *SignerEnrollmentService) verifyIdentityToken(ctx context.Context, oidcConfig domain.OIDCConfig, rawToken string) (domain.SubjectClaims, error) {
	enrollmentConfig := domain.OIDCConfig{
		IssuerURL: oidcConfig.IssuerURL,
		Audience:  oidcConfig.KeyEnrollmentAudience,
		JWKSURI:   oidcConfig.JWKSURI,
	}
	return s.Verifier.Verify(ctx, enrollmentConfig, rawToken)
}
