package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// SignerEnrollmentRepo implements [domain.SignerEnrollmentRepository]
// backed by SQLite.
type SignerEnrollmentRepo struct {
	DB *sql.Tx
}

func (r *SignerEnrollmentRepo) Create(ctx context.Context, e domain.SignerEnrollment) error {
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO signer_enrollments
		 (id, subject_id, issuer, identity_token, registry_subject, registry_id, created_at, expires_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		string(e.ID),
		string(e.Subject),
		string(e.Issuer),
		string(e.IdentityToken),
		string(e.RegistrySubject),
		string(e.RegistryID),
		e.CreatedAt.UTC().Format(time.RFC3339),
		e.ExpiresAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("signer enrollment %q: %w", e.ID, domain.ErrAlreadyExists)
		}
		return fmt.Errorf("insert signer enrollment: %w", err)
	}
	return nil
}

func (r *SignerEnrollmentRepo) Get(ctx context.Context, id domain.SignerEnrollmentID) (domain.SignerEnrollment, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT id, subject_id, issuer, identity_token, registry_subject, registry_id,
		        created_at, expires_at
		 FROM signer_enrollments WHERE id = ?`,
		string(id),
	)
	return scanSignerEnrollment(row)
}

func (r *SignerEnrollmentRepo) ListBySubject(ctx context.Context, identity domain.FederatedIdentity) ([]domain.SignerEnrollment, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT id, subject_id, issuer, identity_token, registry_subject, registry_id,
		        created_at, expires_at
		 FROM signer_enrollments WHERE subject_id = ? AND issuer = ?`,
		string(identity.Subject), string(identity.Issuer),
	)
	if err != nil {
		return nil, fmt.Errorf("query signer enrollments: %w", err)
	}
	defer rows.Close()

	var enrollments []domain.SignerEnrollment
	for rows.Next() {
		e, err := scanSignerEnrollment(rows)
		if err != nil {
			return nil, err
		}
		enrollments = append(enrollments, e)
	}
	return enrollments, rows.Err()
}

func scanSignerEnrollment(s scanner) (domain.SignerEnrollment, error) {
	var e domain.SignerEnrollment
	var id, subjectID, issuer, identityToken, registrySubject, registryID, createdAtStr, expiresAtStr string

	if err := s.Scan(&id, &subjectID, &issuer, &identityToken, &registrySubject, &registryID,
		&createdAtStr, &expiresAtStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return e, domain.ErrNotFound
		}
		return e, fmt.Errorf("scan signer enrollment: %w", err)
	}

	e.ID = domain.SignerEnrollmentID(id)
	e.Subject = domain.SubjectID(subjectID)
	e.Issuer = domain.IssuerURL(issuer)
	e.IdentityToken = domain.RawToken(identityToken)
	e.RegistrySubject = domain.RegistrySubject(registrySubject)
	e.RegistryID = domain.KeyRegistryID(registryID)

	t, err := time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return e, fmt.Errorf("parse created_at: %w", err)
	}
	e.CreatedAt = t

	t, err = time.Parse(time.RFC3339, expiresAtStr)
	if err != nil {
		return e, fmt.Errorf("parse expires_at: %w", err)
	}
	e.ExpiresAt = t

	return e, nil
}
