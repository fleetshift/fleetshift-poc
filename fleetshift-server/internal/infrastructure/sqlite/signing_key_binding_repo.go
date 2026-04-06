package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// SigningKeyBindingRepo implements [domain.SigningKeyBindingRepository]
// backed by SQLite.
type SigningKeyBindingRepo struct {
	DB *sql.Tx
}

func (r *SigningKeyBindingRepo) Create(ctx context.Context, b domain.SigningKeyBinding) error {
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO signing_key_bindings
		 (id, subject_id, issuer, public_key_jwk, algorithm, key_binding_doc, key_binding_signature, identity_token, created_at, expires_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(b.ID),
		string(b.Subject),
		string(b.Issuer),
		b.PublicKeyJWK,
		b.Algorithm,
		b.KeyBindingDoc,
		b.KeyBindingSignature,
		string(b.IdentityToken),
		b.CreatedAt.UTC().Format(time.RFC3339),
		b.ExpiresAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("signing key binding %q: %w", b.ID, domain.ErrAlreadyExists)
		}
		return fmt.Errorf("insert signing key binding: %w", err)
	}
	return nil
}

func (r *SigningKeyBindingRepo) Get(ctx context.Context, id domain.SigningKeyBindingID) (domain.SigningKeyBinding, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT id, subject_id, issuer, public_key_jwk, algorithm,
		        key_binding_doc, key_binding_signature, identity_token,
		        created_at, expires_at
		 FROM signing_key_bindings WHERE id = ?`,
		string(id),
	)
	return scanSigningKeyBinding(row)
}

func (r *SigningKeyBindingRepo) ListBySubject(ctx context.Context, identity domain.FederatedIdentity) ([]domain.SigningKeyBinding, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT id, subject_id, issuer, public_key_jwk, algorithm,
		        key_binding_doc, key_binding_signature, identity_token,
		        created_at, expires_at
		 FROM signing_key_bindings WHERE subject_id = ? AND issuer = ?`,
		string(identity.Subject), string(identity.Issuer),
	)
	if err != nil {
		return nil, fmt.Errorf("query signing key bindings: %w", err)
	}
	defer rows.Close()

	var bindings []domain.SigningKeyBinding
	for rows.Next() {
		b, err := scanSigningKeyBinding(rows)
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, b)
	}
	return bindings, rows.Err()
}

func scanSigningKeyBinding(s scanner) (domain.SigningKeyBinding, error) {
	var b domain.SigningKeyBinding
	var id, subjectID, issuer, algorithm, identityToken, createdAtStr, expiresAtStr string
	var publicKeyJWK, keyBindingDoc, keyBindingSig []byte

	if err := s.Scan(&id, &subjectID, &issuer, &publicKeyJWK, &algorithm,
		&keyBindingDoc, &keyBindingSig, &identityToken,
		&createdAtStr, &expiresAtStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return b, domain.ErrNotFound
		}
		return b, fmt.Errorf("scan signing key binding: %w", err)
	}

	b.ID = domain.SigningKeyBindingID(id)
	b.Subject = domain.SubjectID(subjectID)
	b.Issuer = domain.IssuerURL(issuer)
	b.PublicKeyJWK = publicKeyJWK
	b.Algorithm = algorithm
	b.KeyBindingDoc = keyBindingDoc
	b.KeyBindingSignature = keyBindingSig
	b.IdentityToken = domain.RawToken(identityToken)

	t, err := time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return b, fmt.Errorf("parse created_at: %w", err)
	}
	b.CreatedAt = t

	t, err = time.Parse(time.RFC3339, expiresAtStr)
	if err != nil {
		return b, fmt.Errorf("parse expires_at: %w", err)
	}
	b.ExpiresAt = t

	return b, nil
}
