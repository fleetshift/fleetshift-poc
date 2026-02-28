package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// DeploymentRepo implements [domain.DeploymentRepository] backed by SQLite.
type DeploymentRepo struct {
	DB *sql.DB
}

func (r *DeploymentRepo) Create(ctx context.Context, d domain.Deployment) error {
	ms, err := json.Marshal(d.ManifestStrategy)
	if err != nil {
		return fmt.Errorf("marshal manifest strategy: %w", err)
	}
	ps, err := json.Marshal(d.PlacementStrategy)
	if err != nil {
		return fmt.Errorf("marshal placement strategy: %w", err)
	}
	var rs []byte
	if d.RolloutStrategy != nil {
		rs, err = json.Marshal(d.RolloutStrategy)
		if err != nil {
			return fmt.Errorf("marshal rollout strategy: %w", err)
		}
	}
	rt, err := json.Marshal(d.ResolvedTargets)
	if err != nil {
		return fmt.Errorf("marshal resolved targets: %w", err)
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO deployments (id, manifest_strategy, placement_strategy, rollout_strategy, resolved_targets, state)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		string(d.ID), string(ms), string(ps), nullString(rs), string(rt), string(d.State),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("deployment %q: %w", d.ID, domain.ErrAlreadyExists)
		}
		return fmt.Errorf("insert deployment: %w", err)
	}
	return nil
}

func (r *DeploymentRepo) Get(ctx context.Context, id domain.DeploymentID) (domain.Deployment, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT id, manifest_strategy, placement_strategy, rollout_strategy, resolved_targets, state
		 FROM deployments WHERE id = ?`,
		string(id),
	)
	return scanDeployment(row)
}

func (r *DeploymentRepo) List(ctx context.Context) ([]domain.Deployment, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT id, manifest_strategy, placement_strategy, rollout_strategy, resolved_targets, state
		 FROM deployments`,
	)
	if err != nil {
		return nil, fmt.Errorf("list deployments: %w", err)
	}
	defer rows.Close()

	var deployments []domain.Deployment
	for rows.Next() {
		d, err := scanDeployment(rows)
		if err != nil {
			return nil, err
		}
		deployments = append(deployments, d)
	}
	return deployments, rows.Err()
}

func (r *DeploymentRepo) Update(ctx context.Context, d domain.Deployment) error {
	ms, _ := json.Marshal(d.ManifestStrategy)
	ps, _ := json.Marshal(d.PlacementStrategy)
	var rs []byte
	if d.RolloutStrategy != nil {
		rs, _ = json.Marshal(d.RolloutStrategy)
	}
	rt, _ := json.Marshal(d.ResolvedTargets)

	res, err := r.DB.ExecContext(ctx,
		`UPDATE deployments
		 SET manifest_strategy = ?, placement_strategy = ?, rollout_strategy = ?,
		     resolved_targets = ?, state = ?
		 WHERE id = ?`,
		string(ms), string(ps), nullString(rs), string(rt), string(d.State), string(d.ID),
	)
	if err != nil {
		return fmt.Errorf("update deployment: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("deployment %q: %w", d.ID, domain.ErrNotFound)
	}
	return nil
}

func (r *DeploymentRepo) Delete(ctx context.Context, id domain.DeploymentID) error {
	res, err := r.DB.ExecContext(ctx, `DELETE FROM deployments WHERE id = ?`, string(id))
	if err != nil {
		return fmt.Errorf("delete deployment: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("deployment %q: %w", id, domain.ErrNotFound)
	}
	return nil
}

func scanDeployment(s scanner) (domain.Deployment, error) {
	var d domain.Deployment
	var id, msJSON, psJSON, rtJSON, stateStr string
	var rsJSON sql.NullString
	if err := s.Scan(&id, &msJSON, &psJSON, &rsJSON, &rtJSON, &stateStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return d, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return d, fmt.Errorf("scan deployment: %w", err)
	}
	d.ID = domain.DeploymentID(id)
	d.State = domain.DeploymentState(stateStr)

	if err := json.Unmarshal([]byte(msJSON), &d.ManifestStrategy); err != nil {
		return d, fmt.Errorf("unmarshal manifest strategy: %w", err)
	}
	if err := json.Unmarshal([]byte(psJSON), &d.PlacementStrategy); err != nil {
		return d, fmt.Errorf("unmarshal placement strategy: %w", err)
	}
	if rsJSON.Valid {
		d.RolloutStrategy = &domain.RolloutStrategySpec{}
		if err := json.Unmarshal([]byte(rsJSON.String), d.RolloutStrategy); err != nil {
			return d, fmt.Errorf("unmarshal rollout strategy: %w", err)
		}
	}
	if err := json.Unmarshal([]byte(rtJSON), &d.ResolvedTargets); err != nil {
		return d, fmt.Errorf("unmarshal resolved targets: %w", err)
	}
	return d, nil
}

func nullString(b []byte) sql.NullString {
	if b == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: string(b), Valid: true}
}
