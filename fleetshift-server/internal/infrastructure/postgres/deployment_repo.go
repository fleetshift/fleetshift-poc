package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// DeploymentRepo implements [domain.DeploymentRepository] backed by Postgres.
type DeploymentRepo struct {
	DB *sql.Tx
}

type marshaledDeployment struct {
	manifest   string
	placement  string
	rollout    sql.NullString
	targets    string
	auth       string
	provenance sql.NullString
}

func marshalDeploymentFields(d domain.Deployment) (marshaledDeployment, error) {
	ms, err := json.Marshal(d.ManifestStrategy)
	if err != nil {
		return marshaledDeployment{}, fmt.Errorf("marshal manifest strategy: %w", err)
	}
	ps, err := json.Marshal(d.PlacementStrategy)
	if err != nil {
		return marshaledDeployment{}, fmt.Errorf("marshal placement strategy: %w", err)
	}
	var rs []byte
	if d.RolloutStrategy != nil {
		rs, err = json.Marshal(d.RolloutStrategy)
		if err != nil {
			return marshaledDeployment{}, fmt.Errorf("marshal rollout strategy: %w", err)
		}
	}
	rt, err := json.Marshal(d.ResolvedTargets)
	if err != nil {
		return marshaledDeployment{}, fmt.Errorf("marshal resolved targets: %w", err)
	}
	auth, err := json.Marshal(d.Auth)
	if err != nil {
		return marshaledDeployment{}, fmt.Errorf("marshal auth: %w", err)
	}
	var provJSON []byte
	if d.Provenance != nil {
		provJSON, err = json.Marshal(d.Provenance)
		if err != nil {
			return marshaledDeployment{}, fmt.Errorf("marshal provenance: %w", err)
		}
	}
	return marshaledDeployment{
		manifest:   string(ms),
		placement:  string(ps),
		rollout:    nullString(rs),
		targets:    string(rt),
		auth:       string(auth),
		provenance: nullString(provJSON),
	}, nil
}

func (r *DeploymentRepo) Create(ctx context.Context, d domain.Deployment) error {
	m, err := marshalDeploymentFields(d)
	if err != nil {
		return err
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO deployments (id, uid, manifest_strategy, placement_strategy, rollout_strategy, resolved_targets, state, auth, provenance, generation, observed_generation, active_workflow_gen, created_at, updated_at, etag)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
		d.ID, d.UID, m.manifest, m.placement, m.rollout, m.targets, d.State, m.auth, m.provenance,
		int64(d.Generation), int64(d.ObservedGeneration), nullGeneration(d.ActiveWorkflowGen),
		d.CreatedAt.UTC().Format(time.RFC3339), d.UpdatedAt.UTC().Format(time.RFC3339), d.Etag,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("deployment %q: %w", d.ID, domain.ErrAlreadyExists)
		}
		return fmt.Errorf("insert deployment: %w", err)
	}
	return nil
}

const deploymentColumns = `id, uid, manifest_strategy, placement_strategy, rollout_strategy, resolved_targets, state, auth, provenance, generation, observed_generation, active_workflow_gen, created_at, updated_at, etag`

func (r *DeploymentRepo) Get(ctx context.Context, id domain.DeploymentID) (domain.Deployment, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT `+deploymentColumns+` FROM deployments WHERE id = $1`,
		id,
	)
	return scanDeployment(row)
}

func (r *DeploymentRepo) List(ctx context.Context) ([]domain.Deployment, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT `+deploymentColumns+` FROM deployments`,
	)
	if err != nil {
		return nil, fmt.Errorf("list deployments: %w", err)
	}
	return collectRows(rows, scanDeployment)
}

func (r *DeploymentRepo) Update(ctx context.Context, d domain.Deployment) error {
	m, err := marshalDeploymentFields(d)
	if err != nil {
		return err
	}

	res, err := r.DB.ExecContext(ctx,
		`UPDATE deployments
		 SET manifest_strategy = $1, placement_strategy = $2, rollout_strategy = $3,
		     resolved_targets = $4, state = $5, auth = $6, provenance = $7,
		     generation = $8, observed_generation = $9, active_workflow_gen = $10,
		     updated_at = $11, etag = $12
		 WHERE id = $13`,
		m.manifest, m.placement, m.rollout, m.targets, d.State, m.auth, m.provenance,
		int64(d.Generation), int64(d.ObservedGeneration), nullGeneration(d.ActiveWorkflowGen),
		d.UpdatedAt.UTC().Format(time.RFC3339), d.Etag, d.ID,
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
	res, err := r.DB.ExecContext(ctx, `DELETE FROM deployments WHERE id = $1`, id)
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
	var id, uid, msJSON, psJSON, rtJSON, stateStr, authJSON, createdAtStr, updatedAtStr, etag string
	var rsJSON, provJSON sql.NullString
	var generation, observedGeneration int64
	var activeWorkflowGen sql.NullInt64
	if err := s.Scan(&id, &uid, &msJSON, &psJSON, &rsJSON, &rtJSON, &stateStr, &authJSON, &provJSON,
		&generation, &observedGeneration, &activeWorkflowGen,
		&createdAtStr, &updatedAtStr, &etag); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return d, domain.ErrNotFound
		}
		return d, fmt.Errorf("scan deployment: %w", err)
	}
	d.ID = domain.DeploymentID(id)
	d.UID = uid
	d.State = domain.DeploymentState(stateStr)
	d.Generation = domain.Generation(generation)
	d.ObservedGeneration = domain.Generation(observedGeneration)
	if activeWorkflowGen.Valid {
		g := domain.Generation(activeWorkflowGen.Int64)
		d.ActiveWorkflowGen = &g
	}
	d.Etag = etag

	t, err := time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return d, fmt.Errorf("parse created_at: %w", err)
	}
	d.CreatedAt = t
	t, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return d, fmt.Errorf("parse updated_at: %w", err)
	}
	d.UpdatedAt = t

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
	if authJSON != "" {
		if err := json.Unmarshal([]byte(authJSON), &d.Auth); err != nil {
			return d, fmt.Errorf("unmarshal auth: %w", err)
		}
	}
	if provJSON.Valid {
		d.Provenance = &domain.Provenance{}
		if err := json.Unmarshal([]byte(provJSON.String), d.Provenance); err != nil {
			return d, fmt.Errorf("unmarshal provenance: %w", err)
		}
	}
	return d, nil
}

func nullString(b []byte) sql.NullString {
	if b == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: string(b), Valid: true}
}

func nullGeneration(g *domain.Generation) sql.NullInt64 {
	if g == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(*g), Valid: true}
}
