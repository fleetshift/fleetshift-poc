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
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(s.ID),
		int64(s.ManifestStrategyVersion),
		int64(s.PlacementStrategyVersion),
		int64(s.RolloutStrategyVersion),
		string(rt), string(s.State), s.StatusReason,
		string(auth), nullString(provJSON),
		nullString(attestRefJSON),
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
		 WHERE f.id = ?`,
		string(id),
	)
	s, err := scanFulfillmentSnapshot(row)
	if err != nil {
		return nil, err
	}
	return domain.FulfillmentFromSnapshot(s), nil
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
			manifest_strategy_version = ?,
			placement_strategy_version = ?,
			rollout_strategy_version = ?,
			resolved_targets = ?, state = ?, status_reason = ?,
			auth = ?, provenance = ?, attestation_ref = ?,
			generation = ?, observed_generation = ?, active_workflow_gen = ?,
			updated_at = ?
		WHERE id = ?`,
		int64(s.ManifestStrategyVersion),
		int64(s.PlacementStrategyVersion),
		int64(s.RolloutStrategyVersion),
		string(rt), string(s.State), s.StatusReason,
		string(auth), nullString(provJSON), nullString(attestRefJSON),
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
			`DELETE FROM `+table+` WHERE fulfillment_id = ?`, string(id),
		); err != nil {
			return fmt.Errorf("delete %s for fulfillment %q: %w", table, id, err)
		}
	}

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

func (r *FulfillmentRepo) flushPendingStrategyRecords(ctx context.Context, pending domain.PendingStrategyRecords) error {
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

// fulfillmentColumnsJoined returns the SELECT column list for a
// fulfillment row joined with its strategy version tables. The caller
// must alias fulfillments as fAlias and include [strategyJoins].
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

// fulfillmentScanColumns holds Scan destinations for fulfillment row
// columns produced by [fulfillmentColumnsJoined]. Embed its dests in
// a wider Scan for joined queries, then call snapshot().
type fulfillmentScanColumns struct {
	id, rtJSON, stateStr, statusReason, authJSON, createdAtStr, updatedAtStr string
	msSpec, psSpec, rsSpec, provJSON, attestRefJSON                          sql.NullString
	msVer, psVer, rsVer, generation, observedGeneration                      int64
	activeWorkflowGen                                                        sql.NullInt64
}

func (c *fulfillmentScanColumns) dests() []any {
	return []any{
		&c.id, &c.msVer, &c.msSpec, &c.psVer, &c.psSpec, &c.rsVer, &c.rsSpec,
		&c.rtJSON, &c.stateStr, &c.statusReason, &c.authJSON, &c.provJSON, &c.attestRefJSON,
		&c.generation, &c.observedGeneration, &c.activeWorkflowGen,
		&c.createdAtStr, &c.updatedAtStr,
	}
}

func (c *fulfillmentScanColumns) snapshot() (domain.FulfillmentSnapshot, error) {
	var s domain.FulfillmentSnapshot
	s.ID = domain.FulfillmentID(c.id)
	s.ManifestStrategyVersion = domain.StrategyVersion(c.msVer)
	s.PlacementStrategyVersion = domain.StrategyVersion(c.psVer)
	s.RolloutStrategyVersion = domain.StrategyVersion(c.rsVer)
	s.State = domain.FulfillmentState(c.stateStr)
	s.StatusReason = c.statusReason
	s.Generation = domain.Generation(c.generation)
	s.ObservedGeneration = domain.Generation(c.observedGeneration)
	if c.activeWorkflowGen.Valid {
		g := domain.Generation(c.activeWorkflowGen.Int64)
		s.ActiveWorkflowGen = &g
	}

	if t, err := time.Parse(time.RFC3339, c.createdAtStr); err == nil {
		s.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339, c.updatedAtStr); err == nil {
		s.UpdatedAt = t
	}

	if c.msSpec.Valid {
		if err := json.Unmarshal([]byte(c.msSpec.String), &s.ManifestStrategy); err != nil {
			return s, fmt.Errorf("unmarshal manifest strategy: %w", err)
		}
	}
	if c.psSpec.Valid {
		if err := json.Unmarshal([]byte(c.psSpec.String), &s.PlacementStrategy); err != nil {
			return s, fmt.Errorf("unmarshal placement strategy: %w", err)
		}
	}
	if c.rsSpec.Valid {
		s.RolloutStrategy = &domain.RolloutStrategySpec{}
		if err := json.Unmarshal([]byte(c.rsSpec.String), s.RolloutStrategy); err != nil {
			return s, fmt.Errorf("unmarshal rollout strategy: %w", err)
		}
	}
	if err := json.Unmarshal([]byte(c.rtJSON), &s.ResolvedTargets); err != nil {
		return s, fmt.Errorf("unmarshal resolved targets: %w", err)
	}
	if c.authJSON != "" {
		if err := json.Unmarshal([]byte(c.authJSON), &s.Auth); err != nil {
			return s, fmt.Errorf("unmarshal auth: %w", err)
		}
	}
	if c.provJSON.Valid {
		s.Provenance = &domain.Provenance{}
		if err := json.Unmarshal([]byte(c.provJSON.String), s.Provenance); err != nil {
			return s, fmt.Errorf("unmarshal provenance: %w", err)
		}
	}
	if c.attestRefJSON.Valid {
		s.AttestationRef = &domain.AttestationRef{}
		if err := json.Unmarshal([]byte(c.attestRefJSON.String), s.AttestationRef); err != nil {
			return s, fmt.Errorf("unmarshal attestation ref: %w", err)
		}
	}
	return s, nil
}

func scanFulfillmentSnapshot(s scanner) (domain.FulfillmentSnapshot, error) {
	var cols fulfillmentScanColumns
	if err := s.Scan(cols.dests()...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.FulfillmentSnapshot{}, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return domain.FulfillmentSnapshot{}, fmt.Errorf("scan fulfillment: %w", err)
	}
	return cols.snapshot()
}
