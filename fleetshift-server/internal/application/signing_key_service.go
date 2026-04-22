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

// Create validates the enrollment ID token, resolves the registry
// subject, and persists a [domain.SignerEnrollment].
//
// The validation steps are:
//  1. Caller must be authenticated (Authorization header).
//  2. Load the OIDC auth method to get the enrollment audience.
//  3. Verify the ID token against IdP JWKS using the enrollment
//     audience.
//  4. Confirm the identity token's sub matches the caller's sub.
//  5. Determine registry: PublicKeyClaimExpression → OIDC registry,
//     RegistrySubjectMapping → external registry (e.g. GitHub),
//     neither → error (admin must configure one explicitly).
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

	idTokenClaims, err := s.verifyIdentityToken(ctx, oidcConfig, in.IdentityToken)
	if err != nil {
		return domain.SignerEnrollment{}, fmt.Errorf("identity token verification failed: %w", err)
	}

	if idTokenClaims.Subject != ac.Subject.Subject {
		return domain.SignerEnrollment{}, fmt.Errorf(
			"%w: identity token subject %q does not match caller %q",
			domain.ErrInvalidArgument, idTokenClaims.Subject, ac.Subject.Subject)
	}

	var registrySubject domain.RegistrySubject
	var registryID domain.KeyRegistryID

	mapping := oidcConfig.RegistrySubjectMapping
	switch {
	case oidcConfig.PublicKeyClaimExpression != "":
		registryID = "oidc"
		registrySubject = domain.RegistrySubject(idTokenClaims.Subject)
	case mapping != nil:
		registrySubject, err = EvalClaimMapping(mapping, in.IdentityToken)
		if err != nil {
			return domain.SignerEnrollment{}, fmt.Errorf(
				"%w: claim mapping evaluation failed: %v",
				domain.ErrInvalidArgument, err)
		}
		registryID = mapping.RegistryID
	default:
		return domain.SignerEnrollment{}, fmt.Errorf(
			"%w: no key registry configured — run fleetctl auth setup with "+
				"--public-key-claim-expression (OIDC) or --registry-id + --registry-subject-expression (GitHub)",
			domain.ErrInvalidArgument)
	}

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
