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

// FulfillmentRepo implements [domain.FulfillmentRepository] backed by Postgres.
type FulfillmentRepo struct {
	DB *sql.Tx
}

func (r *FulfillmentRepo) Create(ctx context.Context, f domain.Fulfillment) error {
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
			id, manifest_strategy_version,
			placement_strategy_version,
			rollout_strategy_version,
			resolved_targets, state, status_reason, auth, provenance,
			generation, observed_generation, active_workflow_gen,
			created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
		string(f.ID),
		int64(f.ManifestStrategyVersion),
		int64(f.PlacementStrategyVersion),
		int64(f.RolloutStrategyVersion),
		string(rt), string(f.State), f.StatusReason,
		string(auth), nullStringFromBytes(provJSON),
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
		`SELECT `+fulfillmentColumnsJoined("f")+`
		 FROM fulfillments f
		 `+strategyJoins("f")+`
		 WHERE f.id = $1`,
		string(id),
	)
	return scanFulfillment(row)
}

func (r *FulfillmentRepo) Update(ctx context.Context, f domain.Fulfillment) error {
	rt, _ := json.Marshal(f.ResolvedTargets)
	auth, _ := json.Marshal(f.Auth)
	var provJSON []byte
	if f.Provenance != nil {
		provJSON, _ = json.Marshal(f.Provenance)
	}

	res, err := r.DB.ExecContext(ctx,
		`UPDATE fulfillments SET
			manifest_strategy_version = $1,
			placement_strategy_version = $2,
			rollout_strategy_version = $3,
			resolved_targets = $4, state = $5, status_reason = $6,
			auth = $7, provenance = $8,
			generation = $9, observed_generation = $10, active_workflow_gen = $11,
			updated_at = $12
		WHERE id = $13`,
		int64(f.ManifestStrategyVersion),
		int64(f.PlacementStrategyVersion),
		int64(f.RolloutStrategyVersion),
		string(rt), string(f.State), f.StatusReason,
		string(auth), nullStringFromBytes(provJSON),
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
	for _, table := range []string{"manifest_strategies", "placement_strategies", "rollout_strategies"} {
		if _, err := r.DB.ExecContext(ctx,
			`DELETE FROM `+table+` WHERE fulfillment_id = $1`, string(id),
		); err != nil {
			return fmt.Errorf("delete %s for fulfillment %q: %w", table, id, err)
		}
	}

	res, err := r.DB.ExecContext(ctx, `DELETE FROM fulfillments WHERE id = $1`, string(id))
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
			`INSERT INTO manifest_strategies (fulfillment_id, version, spec, created_at) VALUES ($1, $2, $3, $4)`,
			string(rec.FulfillmentID), int64(rec.Version), string(spec),
			rec.CreatedAt.UTC().Format(time.RFC3339),
		); err != nil {
			return fmt.Errorf("insert manifest strategy v%d: %w", rec.Version, err)
		}
	}
	for _, rec := range pending.Placement {
		spec, _ := json.Marshal(rec.Spec)
		if _, err := r.DB.ExecContext(ctx,
			`INSERT INTO placement_strategies (fulfillment_id, version, spec, created_at) VALUES ($1, $2, $3, $4)`,
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
			`INSERT INTO rollout_strategies (fulfillment_id, version, spec, created_at) VALUES ($1, $2, $3, $4)`,
			string(rec.FulfillmentID), int64(rec.Version), nullStringFromBytes(spec),
			rec.CreatedAt.UTC().Format(time.RFC3339),
		); err != nil {
			return fmt.Errorf("insert rollout strategy v%d: %w", rec.Version, err)
		}
	}
	return nil
}

// fulfillmentColumnsJoined returns the SELECT column list for a
// fulfillment row joined with its strategy version tables. The caller
// must alias fulfillments as f and include [strategyJoins].
func fulfillmentColumnsJoined(f string) string {
	return f + ".id, " +
		f + ".manifest_strategy_version, ms.spec, " +
		f + ".placement_strategy_version, ps.spec, " +
		f + ".rollout_strategy_version, rs.spec, " +
		f + ".resolved_targets, " + f + ".state, " + f + ".status_reason, " +
		f + ".auth, " + f + ".provenance, " +
		f + ".generation, " + f + ".observed_generation, " + f + ".active_workflow_gen, " +
		f + ".created_at, " + f + ".updated_at"
}

// strategyJoins returns LEFT JOIN clauses that materialize strategy
// specs from the version tables. The join aliases are ms, ps, rs.
func strategyJoins(f string) string {
	return `LEFT JOIN manifest_strategies ms ON ms.fulfillment_id = ` + f + `.id AND ms.version = ` + f + `.manifest_strategy_version
		 LEFT JOIN placement_strategies ps ON ps.fulfillment_id = ` + f + `.id AND ps.version = ` + f + `.placement_strategy_version
		 LEFT JOIN rollout_strategies rs ON rs.fulfillment_id = ` + f + `.id AND rs.version = ` + f + `.rollout_strategy_version`
}

func scanFulfillment(s scanner) (domain.Fulfillment, error) {
	var f domain.Fulfillment
	var id, rtJSON, stateStr, statusReason, authJSON, createdAtStr, updatedAtStr string
	var msSpec, psSpec, rsSpec, provJSON sql.NullString
	var msVer, psVer, rsVer, generation, observedGeneration int64
	var activeWorkflowGen sql.NullInt64
	if err := s.Scan(
		&id, &msVer, &msSpec, &psVer, &psSpec, &rsVer, &rsSpec,
		&rtJSON, &stateStr, &statusReason, &authJSON, &provJSON,
		&generation, &observedGeneration, &activeWorkflowGen,
		&createdAtStr, &updatedAtStr,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return f, domain.ErrNotFound
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

	if msSpec.Valid {
		if err := json.Unmarshal([]byte(msSpec.String), &f.ManifestStrategy); err != nil {
			return f, fmt.Errorf("unmarshal manifest strategy: %w", err)
		}
	}
	if psSpec.Valid {
		if err := json.Unmarshal([]byte(psSpec.String), &f.PlacementStrategy); err != nil {
			return f, fmt.Errorf("unmarshal placement strategy: %w", err)
		}
	}
	if rsSpec.Valid {
		f.RolloutStrategy = &domain.RolloutStrategySpec{}
		if err := json.Unmarshal([]byte(rsSpec.String), f.RolloutStrategy); err != nil {
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
