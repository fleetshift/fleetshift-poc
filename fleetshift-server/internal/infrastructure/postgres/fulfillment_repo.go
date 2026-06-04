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

func (r *FulfillmentRepo) Create(ctx context.Context, f *domain.Fulfillment) error {
	s := f.Snapshot()
	rt, err := json.Marshal(s.ResolvedTargets)
	if err != nil {
		return fmt.Errorf("marshal resolved targets: %w", err)
	}
	auth, err := json.Marshal(s.Auth)
	if err != nil {
		return fmt.Errorf("marshal auth: %w", err)
	}
	var provJSON []byte
	if s.Provenance != nil {
		provJSON, err = json.Marshal(s.Provenance)
		if err != nil {
			return fmt.Errorf("marshal provenance: %w", err)
		}
	}
	var attestRefJSON []byte
	if s.AttestationRef != nil {
		attestRefJSON, err = json.Marshal(s.AttestationRef)
		if err != nil {
			return fmt.Errorf("marshal attestation ref: %w", err)
		}
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO fulfillments (
			id, manifest_strategy_version,
			placement_strategy_version,
			rollout_strategy_version,
			resolved_targets, state, status_reason, auth, provenance,
			attestation_ref,
			generation, observed_generation, active_workflow_gen,
			created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
		string(s.ID),
		int64(s.ManifestStrategyVersion),
		int64(s.PlacementStrategyVersion),
		int64(s.RolloutStrategyVersion),
		string(rt), string(s.State), s.StatusReason,
		string(auth), nullStringFromBytes(provJSON),
		nullStringFromBytes(attestRefJSON),
		int64(s.Generation), int64(s.ObservedGeneration),
		nullGeneration(s.ActiveWorkflowGen),
		s.CreatedAt.UTC().Format(time.RFC3339),
		s.UpdatedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("fulfillment %q: %w", s.ID, domain.ErrAlreadyExists)
		}
		return fmt.Errorf("insert fulfillment: %w", err)
	}

	if err := r.flushPendingStrategyRecords(ctx, s.PendingStrategyRecords); err != nil {
		return err
	}
	f.DrainPendingStrategyRecords()
	return nil
}

func (r *FulfillmentRepo) Get(ctx context.Context, id domain.FulfillmentID) (*domain.Fulfillment, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT `+fulfillmentColumnsJoined("f")+`
		 FROM fulfillments f
		 `+strategyJoins("f")+`
		 WHERE f.id = $1`,
		string(id),
	)
	snap, err := scanFulfillmentSnapshot(row)
	if err != nil {
		return nil, err
	}
	return domain.FulfillmentFromSnapshot(snap), nil
}

func (r *FulfillmentRepo) Update(ctx context.Context, f *domain.Fulfillment) error {
	s := f.Snapshot()
	rt, _ := json.Marshal(s.ResolvedTargets)
	auth, _ := json.Marshal(s.Auth)
	var provJSON []byte
	if s.Provenance != nil {
		provJSON, _ = json.Marshal(s.Provenance)
	}
	var attestRefJSON []byte
	if s.AttestationRef != nil {
		attestRefJSON, _ = json.Marshal(s.AttestationRef)
	}

	res, err := r.DB.ExecContext(ctx,
		`UPDATE fulfillments SET
			manifest_strategy_version = $1,
			placement_strategy_version = $2,
			rollout_strategy_version = $3,
			resolved_targets = $4, state = $5, status_reason = $6,
			auth = $7, provenance = $8, attestation_ref = $9,
			generation = $10, observed_generation = $11, active_workflow_gen = $12,
			updated_at = $13
		WHERE id = $14`,
		int64(s.ManifestStrategyVersion),
		int64(s.PlacementStrategyVersion),
		int64(s.RolloutStrategyVersion),
		string(rt), string(s.State), s.StatusReason,
		string(auth), nullStringFromBytes(provJSON), nullStringFromBytes(attestRefJSON),
		int64(s.Generation), int64(s.ObservedGeneration),
		nullGeneration(s.ActiveWorkflowGen),
		s.UpdatedAt.UTC().Format(time.RFC3339),
		string(s.ID),
	)
	if err != nil {
		return fmt.Errorf("update fulfillment: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("fulfillment %q: %w", s.ID, domain.ErrNotFound)
	}

	if err := r.flushPendingStrategyRecords(ctx, s.PendingStrategyRecords); err != nil {
		return err
	}
	f.DrainPendingStrategyRecords()
	return nil
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

func (r *FulfillmentRepo) flushPendingStrategyRecords(ctx context.Context, pending domain.PendingStrategyRecords) error {
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
		f + ".auth, " + f + ".provenance, " + f + ".attestation_ref, " +
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

func scanFulfillmentSnapshot(s scanner) (domain.FulfillmentSnapshot, error) {
	var snap domain.FulfillmentSnapshot
	var id, rtJSON, stateStr, statusReason, authJSON, createdAtStr, updatedAtStr string
	var msSpec, psSpec, rsSpec, provJSON, attestRefJSON sql.NullString
	var msVer, psVer, rsVer, generation, observedGeneration int64
	var activeWorkflowGen sql.NullInt64
	if err := s.Scan(
		&id, &msVer, &msSpec, &psVer, &psSpec, &rsVer, &rsSpec,
		&rtJSON, &stateStr, &statusReason, &authJSON, &provJSON, &attestRefJSON,
		&generation, &observedGeneration, &activeWorkflowGen,
		&createdAtStr, &updatedAtStr,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return snap, domain.ErrNotFound
		}
		return snap, fmt.Errorf("scan fulfillment: %w", err)
	}
	return fulfillmentSnapshotFromColumns(
		id, msVer, msSpec, psVer, psSpec, rsVer, rsSpec,
		rtJSON, stateStr, statusReason, authJSON, provJSON, attestRefJSON,
		generation, observedGeneration, activeWorkflowGen,
		createdAtStr, updatedAtStr,
	)
}

func fulfillmentSnapshotFromColumns(
	id string,
	msVer int64, msSpec sql.NullString,
	psVer int64, psSpec sql.NullString,
	rsVer int64, rsSpec sql.NullString,
	rtJSON, stateStr, statusReason, authJSON string,
	provJSON, attestRefJSON sql.NullString,
	generation, observedGeneration int64,
	activeWorkflowGen sql.NullInt64,
	createdAtStr, updatedAtStr string,
) (domain.FulfillmentSnapshot, error) {
	var snap domain.FulfillmentSnapshot
	snap.ID = domain.FulfillmentID(id)
	snap.ManifestStrategyVersion = domain.StrategyVersion(msVer)
	snap.PlacementStrategyVersion = domain.StrategyVersion(psVer)
	snap.RolloutStrategyVersion = domain.StrategyVersion(rsVer)
	snap.State = domain.FulfillmentState(stateStr)
	snap.StatusReason = statusReason
	snap.Generation = domain.Generation(generation)
	snap.ObservedGeneration = domain.Generation(observedGeneration)
	if activeWorkflowGen.Valid {
		g := domain.Generation(activeWorkflowGen.Int64)
		snap.ActiveWorkflowGen = &g
	}

	if t, err := time.Parse(time.RFC3339, createdAtStr); err == nil {
		snap.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339, updatedAtStr); err == nil {
		snap.UpdatedAt = t
	}

	if msSpec.Valid {
		if err := json.Unmarshal([]byte(msSpec.String), &snap.ManifestStrategy); err != nil {
			return snap, fmt.Errorf("unmarshal manifest strategy: %w", err)
		}
	}
	if psSpec.Valid {
		if err := json.Unmarshal([]byte(psSpec.String), &snap.PlacementStrategy); err != nil {
			return snap, fmt.Errorf("unmarshal placement strategy: %w", err)
		}
	}
	if rsSpec.Valid {
		snap.RolloutStrategy = &domain.RolloutStrategySpec{}
		if err := json.Unmarshal([]byte(rsSpec.String), snap.RolloutStrategy); err != nil {
			return snap, fmt.Errorf("unmarshal rollout strategy: %w", err)
		}
	}
	if err := json.Unmarshal([]byte(rtJSON), &snap.ResolvedTargets); err != nil {
		return snap, fmt.Errorf("unmarshal resolved targets: %w", err)
	}
	if authJSON != "" {
		if err := json.Unmarshal([]byte(authJSON), &snap.Auth); err != nil {
			return snap, fmt.Errorf("unmarshal auth: %w", err)
		}
	}
	if provJSON.Valid {
		snap.Provenance = &domain.Provenance{}
		if err := json.Unmarshal([]byte(provJSON.String), snap.Provenance); err != nil {
			return snap, fmt.Errorf("unmarshal provenance: %w", err)
		}
	}
	if attestRefJSON.Valid {
		snap.AttestationRef = &domain.AttestationRef{}
		if err := json.Unmarshal([]byte(attestRefJSON.String), snap.AttestationRef); err != nil {
			return snap, fmt.Errorf("unmarshal attestation ref: %w", err)
		}
	}
	return snap, nil
}
