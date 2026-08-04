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
	s := e.Snapshot()
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO signer_enrollments
		 (id, subject_id, issuer, identity_token, registry_subject, registry_id, created_at, expires_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		string(s.ID),
		string(s.Subject),
		string(s.Issuer),
		string(s.IdentityToken),
		string(s.RegistrySubject),
		string(s.RegistryID),
		s.CreatedAt.UTC().Format(time.RFC3339),
		s.ExpiresAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("signer enrollment %q: %w", s.ID, domain.ErrAlreadyExists)
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
	s, err := scanSignerEnrollmentSnapshot(row)
	if err != nil {
		return domain.SignerEnrollment{}, err
	}
	return domain.SignerEnrollmentFromSnapshot(s), nil
}

func (r *SignerEnrollmentRepo) ListBySubject(ctx context.Context, identity domain.FederatedIdentity) ([]domain.SignerEnrollment, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT id, subject_id, issuer, identity_token, registry_subject, registry_id,
		        created_at, expires_at
		 FROM signer_enrollments WHERE subject_id = ? AND issuer = ?
		 ORDER BY created_at DESC`, // newest first so callers that pick [0] get the latest key (re-enrollment)
		string(identity.Subject), string(identity.Issuer),
	)
	if err != nil {
		return nil, fmt.Errorf("query signer enrollments: %w", err)
	}
	defer rows.Close()

	var enrollments []domain.SignerEnrollment
	for rows.Next() {
		s, err := scanSignerEnrollmentSnapshot(rows)
		if err != nil {
			return nil, err
		}
		enrollments = append(enrollments, domain.SignerEnrollmentFromSnapshot(s))
	}
	return enrollments, rows.Err()
}

func scanSignerEnrollmentSnapshot(s scanner) (domain.SignerEnrollmentSnapshot, error) {
	var snap domain.SignerEnrollmentSnapshot
	var id, subjectID, issuer, identityToken, registrySubject, registryID, createdAtStr, expiresAtStr string

	if err := s.Scan(&id, &subjectID, &issuer, &identityToken, &registrySubject, &registryID,
		&createdAtStr, &expiresAtStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return snap, domain.ErrNotFound
		}
		return snap, fmt.Errorf("scan signer enrollment: %w", err)
	}

	snap.ID = domain.SignerEnrollmentID(id)
	snap.Subject = domain.SubjectID(subjectID)
	snap.Issuer = domain.IssuerURL(issuer)
	snap.IdentityToken = domain.RawToken(identityToken)
	snap.RegistrySubject = domain.RegistrySubject(registrySubject)
	snap.RegistryID = domain.KeyRegistryID(registryID)

	t, err := time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return snap, fmt.Errorf("parse created_at: %w", err)
	}
	snap.CreatedAt = t

	t, err = time.Parse(time.RFC3339, expiresAtStr)
	if err != nil {
		return snap, fmt.Errorf("parse expires_at: %w", err)
	}
	snap.ExpiresAt = t

	return snap, nil
}
