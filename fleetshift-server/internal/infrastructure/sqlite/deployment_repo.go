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

// DeploymentRepo implements [domain.DeploymentRepository] backed by SQLite.
type DeploymentRepo struct {
	DB *sql.Tx
}

func (r *DeploymentRepo) Create(ctx context.Context, d domain.Deployment) error {
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO deployments (id, uid, fulfillment_id, created_at, updated_at, etag)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		string(d.ID), d.UID, string(d.FulfillmentID),
		d.CreatedAt.UTC().Format(time.RFC3339),
		d.UpdatedAt.UTC().Format(time.RFC3339),
		d.Etag,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("deployment %q: %w", d.ID, domain.ErrAlreadyExists)
		}
		return fmt.Errorf("insert deployment: %w", err)
	}
	return nil
}

const thinDeploymentColumns = `id, uid, fulfillment_id, created_at, updated_at, etag`

func (r *DeploymentRepo) Get(ctx context.Context, id domain.DeploymentID) (domain.Deployment, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT `+thinDeploymentColumns+` FROM deployments WHERE id = ?`,
		string(id),
	)
	return scanThinDeployment(row)
}

func (r *DeploymentRepo) GetView(ctx context.Context, id domain.DeploymentID) (domain.DeploymentView, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT d.id, d.uid, d.fulfillment_id, d.created_at, d.updated_at, d.etag,
		        `+fulfillmentColumnsJoined("f")+`
		 FROM deployments d
		 JOIN fulfillments f ON f.id = d.fulfillment_id
		 `+strategyJoins("f")+`
		 WHERE d.id = ?`,
		string(id),
	)
	return scanDeploymentView(row)
}

func (r *DeploymentRepo) ListView(ctx context.Context) ([]domain.DeploymentView, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT d.id, d.uid, d.fulfillment_id, d.created_at, d.updated_at, d.etag,
		        `+fulfillmentColumnsJoined("f")+`
		 FROM deployments d
		 JOIN fulfillments f ON f.id = d.fulfillment_id
		 `+strategyJoins("f"),
	)
	if err != nil {
		return nil, fmt.Errorf("list deployment views: %w", err)
	}
	defer rows.Close()

	var views []domain.DeploymentView
	for rows.Next() {
		v, err := scanDeploymentView(rows)
		if err != nil {
			return nil, err
		}
		views = append(views, v)
	}
	return views, rows.Err()
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

func scanThinDeployment(s scanner) (domain.Deployment, error) {
	var d domain.Deployment
	var id, uid, fID, createdAtStr, updatedAtStr, etag string
	if err := s.Scan(&id, &uid, &fID, &createdAtStr, &updatedAtStr, &etag); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return d, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return d, fmt.Errorf("scan deployment: %w", err)
	}
	d.ID = domain.DeploymentID(id)
	d.UID = uid
	d.FulfillmentID = domain.FulfillmentID(fID)
	d.Etag = etag
	if t, err := time.Parse(time.RFC3339, createdAtStr); err == nil {
		d.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339, updatedAtStr); err == nil {
		d.UpdatedAt = t
	}
	return d, nil
}

func scanDeploymentView(s scanner) (domain.DeploymentView, error) {
	var v domain.DeploymentView
	var dID, uid, fRefID, dCreatedAtStr, dUpdatedAtStr, etag string
	var fID, fRtJSON, fStateStr, fStatusReason, fAuthJSON, fCreatedAtStr, fUpdatedAtStr string
	var fMsSpec, fPsSpec, fRsSpec, fProvJSON sql.NullString
	var fMsVer, fPsVer, fRsVer, fGen, fObsGen int64
	var fActiveWfGen sql.NullInt64

	if err := s.Scan(
		&dID, &uid, &fRefID, &dCreatedAtStr, &dUpdatedAtStr, &etag,
		&fID, &fMsVer, &fMsSpec, &fPsVer, &fPsSpec, &fRsVer, &fRsSpec,
		&fRtJSON, &fStateStr, &fStatusReason, &fAuthJSON, &fProvJSON,
		&fGen, &fObsGen, &fActiveWfGen,
		&fCreatedAtStr, &fUpdatedAtStr,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return v, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return v, fmt.Errorf("scan deployment view: %w", err)
	}

	v.Deployment.ID = domain.DeploymentID(dID)
	v.Deployment.UID = uid
	v.Deployment.FulfillmentID = domain.FulfillmentID(fRefID)
	v.Deployment.Etag = etag
	if t, err := time.Parse(time.RFC3339, dCreatedAtStr); err == nil {
		v.Deployment.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339, dUpdatedAtStr); err == nil {
		v.Deployment.UpdatedAt = t
	}

	v.Fulfillment.ID = domain.FulfillmentID(fID)
	v.Fulfillment.ManifestStrategyVersion = domain.StrategyVersion(fMsVer)
	v.Fulfillment.PlacementStrategyVersion = domain.StrategyVersion(fPsVer)
	v.Fulfillment.RolloutStrategyVersion = domain.StrategyVersion(fRsVer)
	v.Fulfillment.State = domain.FulfillmentState(fStateStr)
	v.Fulfillment.StatusReason = fStatusReason
	v.Fulfillment.Generation = domain.Generation(fGen)
	v.Fulfillment.ObservedGeneration = domain.Generation(fObsGen)
	if fActiveWfGen.Valid {
		g := domain.Generation(fActiveWfGen.Int64)
		v.Fulfillment.ActiveWorkflowGen = &g
	}
	if t, err := time.Parse(time.RFC3339, fCreatedAtStr); err == nil {
		v.Fulfillment.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339, fUpdatedAtStr); err == nil {
		v.Fulfillment.UpdatedAt = t
	}

	if fMsSpec.Valid {
		if err := unmarshalJSON(fMsSpec.String, &v.Fulfillment.ManifestStrategy, "manifest strategy"); err != nil {
			return v, err
		}
	}
	if fPsSpec.Valid {
		if err := unmarshalJSON(fPsSpec.String, &v.Fulfillment.PlacementStrategy, "placement strategy"); err != nil {
			return v, err
		}
	}
	if fRsSpec.Valid {
		v.Fulfillment.RolloutStrategy = &domain.RolloutStrategySpec{}
		if err := unmarshalJSON(fRsSpec.String, v.Fulfillment.RolloutStrategy, "rollout strategy"); err != nil {
			return v, err
		}
	}
	if err := unmarshalJSON(fRtJSON, &v.Fulfillment.ResolvedTargets, "resolved targets"); err != nil {
		return v, err
	}
	if fAuthJSON != "" {
		if err := unmarshalJSON(fAuthJSON, &v.Fulfillment.Auth, "auth"); err != nil {
			return v, err
		}
	}
	if fProvJSON.Valid {
		v.Fulfillment.Provenance = &domain.Provenance{}
		if err := unmarshalJSON(fProvJSON.String, v.Fulfillment.Provenance, "provenance"); err != nil {
			return v, err
		}
	}

	return v, nil
}

func unmarshalJSON[T any](data string, dst *T, label string) error {
	if err := json.Unmarshal([]byte(data), dst); err != nil {
		return fmt.Errorf("unmarshal %s: %w", label, err)
	}
	return nil
}
