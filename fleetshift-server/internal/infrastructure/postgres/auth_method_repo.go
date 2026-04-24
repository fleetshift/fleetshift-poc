package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// AuthMethodRepo implements [domain.AuthMethodRepository] backed by Postgres.
// Unlike other repos in this package, it operates on [*sql.DB] directly
// because auth method operations do not participate in cross-repo
// transactions.
type AuthMethodRepo struct {
	DB *sql.DB
}

func (r *AuthMethodRepo) Save(ctx context.Context, method domain.AuthMethod) error {
	configJSON, err := marshalAuthMethodConfig(method)
	if err != nil {
		return err
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO auth_methods (id, type, config_json) VALUES ($1, $2, $3)
		 ON CONFLICT(id) DO UPDATE SET type = excluded.type, config_json = excluded.config_json`,
		method.ID, method.Type, string(configJSON),
	)
	if err != nil {
		return fmt.Errorf("save auth method: %w", err)
	}
	return nil
}

func (r *AuthMethodRepo) Get(ctx context.Context, id domain.AuthMethodID) (domain.AuthMethod, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT id, type, config_json FROM auth_methods WHERE id = $1`,
		id,
	)
	return scanAuthMethod(row)
}

func (r *AuthMethodRepo) List(ctx context.Context) ([]domain.AuthMethod, error) {
	rows, err := r.DB.QueryContext(ctx, `SELECT id, type, config_json FROM auth_methods`)
	if err != nil {
		return nil, fmt.Errorf("list auth methods: %w", err)
	}
	return collectRows(rows, scanAuthMethod)
}

func marshalAuthMethodConfig(m domain.AuthMethod) ([]byte, error) {
	switch m.Type {
	case domain.AuthMethodTypeOIDC:
		return json.Marshal(m.OIDC)
	default:
		return nil, fmt.Errorf("unknown auth method type: %s", m.Type)
	}
}

func scanAuthMethod(s scanner) (domain.AuthMethod, error) {
	var id, methodType, configJSON string
	if err := s.Scan(&id, &methodType, &configJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.AuthMethod{}, domain.ErrNotFound
		}
		return domain.AuthMethod{}, fmt.Errorf("scan auth method: %w", err)
	}

	m := domain.AuthMethod{
		ID:   domain.AuthMethodID(id),
		Type: domain.AuthMethodType(methodType),
	}

	switch m.Type {
	case domain.AuthMethodTypeOIDC:
		var cfg domain.OIDCConfig
		if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
			return domain.AuthMethod{}, fmt.Errorf("unmarshal OIDC config: %w", err)
		}
		m.OIDC = &cfg
	}

	return m, nil
}
