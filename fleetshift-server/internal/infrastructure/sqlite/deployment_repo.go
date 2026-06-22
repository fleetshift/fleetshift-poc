package sqlite

import (
	"context"
	"database/sql"
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
	s := d.Snapshot()
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO deployments (name, uid, fulfillment_id, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)`,
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
		`SELECT `+thinDeploymentColumns+` FROM deployments WHERE name = ?`,
		string(name),
	)
	s, err := scanDeploymentSnapshot(row)
	if err != nil {
		return domain.Deployment{}, err
	}
	return domain.DeploymentFromSnapshot(s), nil
}

func (r *DeploymentRepo) GetView(ctx context.Context, name domain.ResourceName) (domain.DeploymentView, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT d.name, d.uid, d.fulfillment_id, d.created_at, d.updated_at,
		        `+fulfillmentColumnsJoined("f")+`
		 FROM deployments d
		 JOIN fulfillments f ON f.id = d.fulfillment_id
		 `+strategyJoins("f")+`
		 WHERE d.name = ?`,
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

func (r *DeploymentRepo) Delete(ctx context.Context, name domain.ResourceName) error {
	res, err := r.DB.ExecContext(ctx, `DELETE FROM deployments WHERE name = ?`, string(name))
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
			return snap, fmt.Errorf("%w", domain.ErrNotFound)
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
	var fCols fulfillmentScanColumns

	if err := s.Scan(append([]any{
		&dName, &uid, &fRefID, &dCreatedAtStr, &dUpdatedAtStr,
	}, fCols.dests()...)...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return v, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return v, fmt.Errorf("scan deployment view: %w", err)
	}

	ds := domain.DeploymentSnapshot{
		Name:          domain.ResourceName(dName),
		UID:           uid,
		FulfillmentID: domain.FulfillmentID(fRefID),
	}
	t, err := time.Parse(time.RFC3339, dCreatedAtStr)
	if err != nil {
		return v, fmt.Errorf("parse deployment.created_at for %q: %w", dName, err)
	}
	ds.CreatedAt = t
	t, err = time.Parse(time.RFC3339, dUpdatedAtStr)
	if err != nil {
		return v, fmt.Errorf("parse deployment.updated_at for %q: %w", dName, err)
	}
	ds.UpdatedAt = t
	v.Deployment = domain.DeploymentFromSnapshot(ds)

	fs, err := fCols.snapshot()
	if err != nil {
		return v, err
	}
	v.Fulfillment = *domain.FulfillmentFromSnapshot(fs)

	return v, nil
}
