package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// DeploymentRepo implements [domain.DeploymentRepository] backed by Postgres.
type DeploymentRepo struct {
	DB *sql.Tx
}

func (r *DeploymentRepo) Create(ctx context.Context, d domain.Deployment) error {
	s := d.Snapshot()
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO deployments (name, uid, fulfillment_id, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		string(s.Name), s.UID, string(s.FulfillmentID),
		s.CreatedAt.UTC().Format(time.RFC3339),
		s.UpdatedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("deployment %q: %w", s.Name, domain.ErrAlreadyExists)
		}
		return fmt.Errorf("insert deployment: %w", err)
	}
	return nil
}

const thinDeploymentColumns = `name, uid, fulfillment_id, created_at, updated_at`

func (r *DeploymentRepo) Get(ctx context.Context, name domain.ResourceName) (domain.Deployment, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT `+thinDeploymentColumns+` FROM deployments WHERE name = $1`,
		string(name),
	)
	snap, err := scanDeploymentSnapshot(row)
	if err != nil {
		return domain.Deployment{}, err
	}
	return domain.DeploymentFromSnapshot(snap), nil
}

func (r *DeploymentRepo) GetView(ctx context.Context, name domain.ResourceName) (domain.DeploymentView, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT d.name, d.uid, d.fulfillment_id, d.created_at, d.updated_at,
		        `+fulfillmentColumnsJoined("f")+`
		 FROM deployments d
		 JOIN fulfillments f ON f.id = d.fulfillment_id
		 `+strategyJoins("f")+`
		 WHERE d.name = $1`,
		string(name),
	)
	return scanDeploymentView(row)
}

func (r *DeploymentRepo) ListView(ctx context.Context) ([]domain.DeploymentView, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT d.name, d.uid, d.fulfillment_id, d.created_at, d.updated_at,
		        `+fulfillmentColumnsJoined("f")+`
		 FROM deployments d
		 JOIN fulfillments f ON f.id = d.fulfillment_id
		 `+strategyJoins("f"),
	)
	if err != nil {
		return nil, fmt.Errorf("list deployment views: %w", err)
	}
	return collectRows(rows, scanDeploymentView)
}

func (r *DeploymentRepo) Delete(ctx context.Context, name domain.ResourceName) error {
	res, err := r.DB.ExecContext(ctx, `DELETE FROM deployments WHERE name = $1`, string(name))
	if err != nil {
		return fmt.Errorf("delete deployment: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("deployment %q: %w", name, domain.ErrNotFound)
	}
	return nil
}

func scanDeploymentSnapshot(s scanner) (domain.DeploymentSnapshot, error) {
	var snap domain.DeploymentSnapshot
	var name, fID, createdAtStr, updatedAtStr string
	if err := s.Scan(&name, &snap.UID, &fID, &createdAtStr, &updatedAtStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return snap, domain.ErrNotFound
		}
		return snap, fmt.Errorf("scan deployment: %w", err)
	}
	snap.Name = domain.ResourceName(name)
	snap.FulfillmentID = domain.FulfillmentID(fID)
	t, err := time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return snap, fmt.Errorf("parse deployment.created_at for %q: %w", name, err)
	}
	snap.CreatedAt = t
	t, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return snap, fmt.Errorf("parse deployment.updated_at for %q: %w", name, err)
	}
	snap.UpdatedAt = t
	return snap, nil
}

func scanDeploymentView(s scanner) (domain.DeploymentView, error) {
	var v domain.DeploymentView
	var dName, fRefID, dCreatedAtStr, dUpdatedAtStr string
	var uid domain.DeploymentUID
	var fID, fRtJSON, fStateStr, fPauseReason, fStatusReason, fAuthJSON, fCreatedAtStr, fUpdatedAtStr string
	var fMsSpec, fPsSpec, fRsSpec, fProvJSON, fAttestRefJSON sql.NullString
	var fMsVer, fPsVer, fRsVer, fGen, fObsGen int64
	var fActiveWfGen sql.NullInt64

	if err := s.Scan(
		&dName, &uid, &fRefID, &dCreatedAtStr, &dUpdatedAtStr,
		&fID, &fMsVer, &fMsSpec, &fPsVer, &fPsSpec, &fRsVer, &fRsSpec,
		&fRtJSON, &fStateStr, &fPauseReason, &fStatusReason, &fAuthJSON, &fProvJSON, &fAttestRefJSON,
		&fGen, &fObsGen, &fActiveWfGen,
		&fCreatedAtStr, &fUpdatedAtStr,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return v, domain.ErrNotFound
		}
		return v, fmt.Errorf("scan deployment view: %w", err)
	}

	dSnap := domain.DeploymentSnapshot{
		Name:          domain.ResourceName(dName),
		UID:           uid,
		FulfillmentID: domain.FulfillmentID(fRefID),
	}
	t, err := time.Parse(time.RFC3339, dCreatedAtStr)
	if err != nil {
		return v, fmt.Errorf("parse deployment.created_at for %q: %w", dName, err)
	}
	dSnap.CreatedAt = t
	t, err = time.Parse(time.RFC3339, dUpdatedAtStr)
	if err != nil {
		return v, fmt.Errorf("parse deployment.updated_at for %q: %w", dName, err)
	}
	dSnap.UpdatedAt = t
	v.Deployment = domain.DeploymentFromSnapshot(dSnap)

	fSnap, err := fulfillmentSnapshotFromColumns(
		fID, fMsVer, fMsSpec, fPsVer, fPsSpec, fRsVer, fRsSpec,
		fRtJSON, fStateStr, fPauseReason, fStatusReason, fAuthJSON, fProvJSON, fAttestRefJSON,
		fGen, fObsGen, fActiveWfGen,
		fCreatedAtStr, fUpdatedAtStr,
	)
	if err != nil {
		return v, err
	}
	v.Fulfillment = *domain.FulfillmentFromSnapshot(fSnap)

	return v, nil
}
