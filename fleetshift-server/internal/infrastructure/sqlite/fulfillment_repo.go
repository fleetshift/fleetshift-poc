package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// FulfillmentRepo implements [domain.FulfillmentRepository] backed by SQLite.
type FulfillmentRepo struct {
	DB *sql.Tx
}

func (r *FulfillmentRepo) Create(ctx context.Context, f domain.Fulfillment) error {
	ms, err := json.Marshal(f.ManifestStrategy)
	if err != nil {
		return fmt.Errorf("marshal manifest strategy: %w", err)
	}
	ps, err := json.Marshal(f.PlacementStrategy)
	if err != nil {
		return fmt.Errorf("marshal placement strategy: %w", err)
	}
	var rs []byte
	if f.RolloutStrategy != nil {
		rs, err = json.Marshal(f.RolloutStrategy)
		if err != nil {
			return fmt.Errorf("marshal rollout strategy: %w", err)
		}
	}
	rt, err := json.Marshal(f.ResolvedTargets)
	if err != nil {
		return fmt.Errorf("marshal resolved targets: %w", err)
	}
	auth, err := json.Marshal(f.Auth)
	if err != nil {
		return fmt.Errorf("marshal auth: %w", err)
	}
	var provJSON []byte
	if f.Provenance != nil {
		provJSON, err = json.Marshal(f.Provenance)
		if err != nil {
			return fmt.Errorf("marshal provenance: %w", err)
		}
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO fulfillments (
			id, manifest_strategy, manifest_strategy_version,
			placement_strategy, placement_strategy_version,
			rollout_strategy, rollout_strategy_version,
			resolved_targets, state, status_reason, auth, provenance,
			generation, observed_generation, active_workflow_gen,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(f.ID),
		string(ms), int64(f.ManifestStrategyVersion),
		string(ps), int64(f.PlacementStrategyVersion),
		nullString(rs), int64(f.RolloutStrategyVersion),
		string(rt), string(f.State), f.StatusReason,
		string(auth), nullString(provJSON),
		int64(f.Generation), int64(f.ObservedGeneration),
		nullGeneration(f.ActiveWorkflowGen),
		f.CreatedAt.UTC().Format(time.RFC3339),
		f.UpdatedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("fulfillment %q: %w", f.ID, domain.ErrAlreadyExists)
		}
		return fmt.Errorf("insert fulfillment: %w", err)
	}

	return r.flushPendingStrategyRecords(ctx, &f)
}

func (r *FulfillmentRepo) Get(ctx context.Context, id domain.FulfillmentID) (domain.Fulfillment, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT `+fulfillmentColumns+` FROM fulfillments WHERE id = ?`,
		string(id),
	)
	return scanFulfillment(row)
}

func (r *FulfillmentRepo) Update(ctx context.Context, f domain.Fulfillment) error {
	ms, _ := json.Marshal(f.ManifestStrategy)
	ps, _ := json.Marshal(f.PlacementStrategy)
	var rs []byte
	if f.RolloutStrategy != nil {
		rs, _ = json.Marshal(f.RolloutStrategy)
	}
	rt, _ := json.Marshal(f.ResolvedTargets)
	auth, _ := json.Marshal(f.Auth)
	var provJSON []byte
	if f.Provenance != nil {
		provJSON, _ = json.Marshal(f.Provenance)
	}

	res, err := r.DB.ExecContext(ctx,
		`UPDATE fulfillments SET
			manifest_strategy = ?, manifest_strategy_version = ?,
			placement_strategy = ?, placement_strategy_version = ?,
			rollout_strategy = ?, rollout_strategy_version = ?,
			resolved_targets = ?, state = ?, status_reason = ?,
			auth = ?, provenance = ?,
			generation = ?, observed_generation = ?, active_workflow_gen = ?,
			updated_at = ?
		WHERE id = ?`,
		string(ms), int64(f.ManifestStrategyVersion),
		string(ps), int64(f.PlacementStrategyVersion),
		nullString(rs), int64(f.RolloutStrategyVersion),
		string(rt), string(f.State), f.StatusReason,
		string(auth), nullString(provJSON),
		int64(f.Generation), int64(f.ObservedGeneration),
		nullGeneration(f.ActiveWorkflowGen),
		f.UpdatedAt.UTC().Format(time.RFC3339),
		string(f.ID),
	)
	if err != nil {
		return fmt.Errorf("update fulfillment: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("fulfillment %q: %w", f.ID, domain.ErrNotFound)
	}

	return r.flushPendingStrategyRecords(ctx, &f)
}

func (r *FulfillmentRepo) Delete(ctx context.Context, id domain.FulfillmentID) error {
	res, err := r.DB.ExecContext(ctx, `DELETE FROM fulfillments WHERE id = ?`, string(id))
	if err != nil {
		return fmt.Errorf("delete fulfillment: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("fulfillment %q: %w", id, domain.ErrNotFound)
	}
	return nil
}

func (r *FulfillmentRepo) flushPendingStrategyRecords(ctx context.Context, f *domain.Fulfillment) error {
	pending := f.DrainPendingStrategyRecords()
	for _, rec := range pending.Manifest {
		spec, _ := json.Marshal(rec.Spec)
		if _, err := r.DB.ExecContext(ctx,
			`INSERT INTO manifest_strategies (fulfillment_id, version, spec, created_at) VALUES (?, ?, ?, ?)`,
			string(rec.FulfillmentID), int64(rec.Version), string(spec),
			rec.CreatedAt.UTC().Format(time.RFC3339),
		); err != nil {
			return fmt.Errorf("insert manifest strategy v%d: %w", rec.Version, err)
		}
	}
	for _, rec := range pending.Placement {
		spec, _ := json.Marshal(rec.Spec)
		if _, err := r.DB.ExecContext(ctx,
			`INSERT INTO placement_strategies (fulfillment_id, version, spec, created_at) VALUES (?, ?, ?, ?)`,
			string(rec.FulfillmentID), int64(rec.Version), string(spec),
			rec.CreatedAt.UTC().Format(time.RFC3339),
		); err != nil {
			return fmt.Errorf("insert placement strategy v%d: %w", rec.Version, err)
		}
	}
	for _, rec := range pending.Rollout {
		var spec []byte
		if rec.Spec != nil {
			spec, _ = json.Marshal(rec.Spec)
		}
		if _, err := r.DB.ExecContext(ctx,
			`INSERT INTO rollout_strategies (fulfillment_id, version, spec, created_at) VALUES (?, ?, ?, ?)`,
			string(rec.FulfillmentID), int64(rec.Version), nullString(spec),
			rec.CreatedAt.UTC().Format(time.RFC3339),
		); err != nil {
			return fmt.Errorf("insert rollout strategy v%d: %w", rec.Version, err)
		}
	}
	return nil
}

const fulfillmentColumns = `id, manifest_strategy, manifest_strategy_version, placement_strategy, placement_strategy_version, rollout_strategy, rollout_strategy_version, resolved_targets, state, status_reason, auth, provenance, generation, observed_generation, active_workflow_gen, created_at, updated_at`

func scanFulfillment(s scanner) (domain.Fulfillment, error) {
	var f domain.Fulfillment
	var id, msJSON, psJSON, rtJSON, stateStr, statusReason, authJSON, createdAtStr, updatedAtStr string
	var rsJSON, provJSON sql.NullString
	var msVer, psVer, rsVer, generation, observedGeneration int64
	var activeWorkflowGen sql.NullInt64
	if err := s.Scan(
		&id, &msJSON, &msVer, &psJSON, &psVer, &rsJSON, &rsVer,
		&rtJSON, &stateStr, &statusReason, &authJSON, &provJSON,
		&generation, &observedGeneration, &activeWorkflowGen,
		&createdAtStr, &updatedAtStr,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return f, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return f, fmt.Errorf("scan fulfillment: %w", err)
	}
	f.ID = domain.FulfillmentID(id)
	f.ManifestStrategyVersion = domain.StrategyVersion(msVer)
	f.PlacementStrategyVersion = domain.StrategyVersion(psVer)
	f.RolloutStrategyVersion = domain.StrategyVersion(rsVer)
	f.State = domain.FulfillmentState(stateStr)
	f.StatusReason = statusReason
	f.Generation = domain.Generation(generation)
	f.ObservedGeneration = domain.Generation(observedGeneration)
	if activeWorkflowGen.Valid {
		g := domain.Generation(activeWorkflowGen.Int64)
		f.ActiveWorkflowGen = &g
	}

	if t, err := time.Parse(time.RFC3339, createdAtStr); err == nil {
		f.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339, updatedAtStr); err == nil {
		f.UpdatedAt = t
	}

	if err := json.Unmarshal([]byte(msJSON), &f.ManifestStrategy); err != nil {
		return f, fmt.Errorf("unmarshal manifest strategy: %w", err)
	}
	if err := json.Unmarshal([]byte(psJSON), &f.PlacementStrategy); err != nil {
		return f, fmt.Errorf("unmarshal placement strategy: %w", err)
	}
	if rsJSON.Valid {
		f.RolloutStrategy = &domain.RolloutStrategySpec{}
		if err := json.Unmarshal([]byte(rsJSON.String), f.RolloutStrategy); err != nil {
			return f, fmt.Errorf("unmarshal rollout strategy: %w", err)
		}
	}
	if err := json.Unmarshal([]byte(rtJSON), &f.ResolvedTargets); err != nil {
		return f, fmt.Errorf("unmarshal resolved targets: %w", err)
	}
	if authJSON != "" {
		if err := json.Unmarshal([]byte(authJSON), &f.Auth); err != nil {
			return f, fmt.Errorf("unmarshal auth: %w", err)
		}
	}
	if provJSON.Valid {
		f.Provenance = &domain.Provenance{}
		if err := json.Unmarshal([]byte(provJSON.String), f.Provenance); err != nil {
			return f, fmt.Errorf("unmarshal provenance: %w", err)
		}
	}
	return f, nil
}
