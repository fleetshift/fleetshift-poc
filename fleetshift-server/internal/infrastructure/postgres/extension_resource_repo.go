package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

var _ domain.ExtensionResourceRepository = (*ExtensionResourceRepo)(nil)

// ExtensionResourceRepo implements [domain.ExtensionResourceRepository] for Postgres.
type ExtensionResourceRepo struct {
	DB interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	}
}

// ---------------------------------------------------------------------------
// Type CRUD
// ---------------------------------------------------------------------------

func (r *ExtensionResourceRepo) CreateType(ctx context.Context, def domain.ExtensionResourceType) error {
	snap := def.Snapshot()
	mgmtJSON, err := marshalManagementSnapshot(snap.Management)
	if err != nil {
		return fmt.Errorf("marshal management: %w", err)
	}
	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_types
			(resource_type, api_version, collection_id, management, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		snap.ResourceType,
		string(snap.APIVersion), string(snap.CollectionID),
		nullStringFromBytes(mgmtJSON),
		snap.CreatedAt.UTC(), snap.UpdatedAt.UTC())
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: resource type %q", domain.ErrAlreadyExists, snap.ResourceType)
		}
		return err
	}
	return nil
}

func (r *ExtensionResourceRepo) GetType(ctx context.Context, rt domain.ResourceType) (domain.ExtensionResourceType, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT resource_type, api_version, collection_id, management, created_at, updated_at
		 FROM extension_resource_types WHERE resource_type = $1`, rt)
	return scanExtensionResourceType(row)
}

func (r *ExtensionResourceRepo) ListTypes(ctx context.Context) ([]domain.ExtensionResourceType, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT resource_type, api_version, collection_id, management, created_at, updated_at
		 FROM extension_resource_types ORDER BY resource_type`)
	if err != nil {
		return nil, err
	}
	return collectRows(rows, scanExtensionResourceType)
}

func (r *ExtensionResourceRepo) DeleteType(ctx context.Context, rt domain.ResourceType) error {
	res, err := r.DB.ExecContext(ctx,
		`DELETE FROM extension_resource_types WHERE resource_type = $1`, rt)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: resource type %q", domain.ErrNotFound, rt)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Instance CRUD
// ---------------------------------------------------------------------------

func (r *ExtensionResourceRepo) Create(ctx context.Context, er *domain.ExtensionResource) error {
	snap := er.Snapshot()

	// Flush pending intents to the shared resource_intents table.
	for _, intent := range snap.PendingIntents {
		if _, err := r.DB.ExecContext(ctx,
			`INSERT INTO resource_intents (resource_type, name, version, spec, created_at)
			 VALUES ($1, $2, $3, $4, $5)`,
			intent.ResourceType, intent.Name, intent.Version, string(intent.Spec),
			intent.CreatedAt.UTC().Format(time.RFC3339)); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("%w: intent %s/%s v%d", domain.ErrAlreadyExists, intent.ResourceType, intent.Name, intent.Version)
			}
			return err
		}
	}

	labelsJSON, err := json.Marshal(snap.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}
	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO extension_resources (uid, resource_type, resource_name, labels, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		snap.UID.String(), snap.ResourceType, snap.Name,
		string(labelsJSON),
		snap.CreatedAt.UTC(), snap.UpdatedAt.UTC())
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: extension resource %s/%s", domain.ErrAlreadyExists, snap.ResourceType, snap.Name)
		}
		return err
	}

	if snap.Managed != nil {
		_, err = r.DB.ExecContext(ctx,
			`INSERT INTO extension_resource_managed
				(extension_resource_uid, current_version, fulfillment_id)
			 VALUES ($1, $2, $3)`,
			snap.UID.String(), snap.Managed.CurrentVersion,
			string(snap.Managed.FulfillmentID))
		if err != nil {
			return fmt.Errorf("insert managed state: %w", err)
		}
	}

	return nil
}

func (r *ExtensionResourceRepo) Get(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) (*domain.ExtensionResource, error) {
	row := r.DB.QueryRowContext(ctx,
		erSelectColumns+`
		FROM extension_resources er
		LEFT JOIN extension_resource_managed erm ON erm.extension_resource_uid = er.uid
		WHERE er.resource_type = $1 AND er.resource_name = $2`,
		rt, name)
	snap, err := scanExtensionResourceSnapshot(row)
	if err != nil {
		return nil, err
	}
	return domain.ExtensionResourceFromSnapshot(snap), nil
}

func (r *ExtensionResourceRepo) GetByUID(ctx context.Context, uid domain.ExtensionResourceUID) (*domain.ExtensionResource, error) {
	row := r.DB.QueryRowContext(ctx,
		erSelectColumns+`
		FROM extension_resources er
		LEFT JOIN extension_resource_managed erm ON erm.extension_resource_uid = er.uid
		WHERE er.uid = $1`,
		uid.String())
	snap, err := scanExtensionResourceSnapshot(row)
	if err != nil {
		return nil, err
	}
	return domain.ExtensionResourceFromSnapshot(snap), nil
}

func (r *ExtensionResourceRepo) ListByResourceType(ctx context.Context, rt domain.ResourceType) ([]*domain.ExtensionResource, error) {
	rows, err := r.DB.QueryContext(ctx,
		erSelectColumns+`
		FROM extension_resources er
		LEFT JOIN extension_resource_managed erm ON erm.extension_resource_uid = er.uid
		WHERE er.resource_type = $1 ORDER BY er.resource_name`,
		rt)
	if err != nil {
		return nil, err
	}
	snaps, err := collectRows(rows, scanExtensionResourceSnapshot)
	if err != nil {
		return nil, err
	}
	result := make([]*domain.ExtensionResource, len(snaps))
	for i, s := range snaps {
		result[i] = domain.ExtensionResourceFromSnapshot(s)
	}
	return result, nil
}

func (r *ExtensionResourceRepo) Delete(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) error {
	res, err := r.DB.ExecContext(ctx,
		`DELETE FROM extension_resources WHERE resource_type = $1 AND resource_name = $2`,
		rt, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: extension resource %s/%s", domain.ErrNotFound, rt, name)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------

func (r *ExtensionResourceRepo) GetView(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) (domain.ExtensionResourceView, error) {
	q := erViewQuery + `
		WHERE er.resource_type = $1 AND er.resource_name = $2`
	row := r.DB.QueryRowContext(ctx, q, rt, name)
	return scanExtensionResourceView(row)
}

func (r *ExtensionResourceRepo) ListViewsByType(ctx context.Context, rt domain.ResourceType) ([]domain.ExtensionResourceView, error) {
	q := erViewQuery + `
		WHERE er.resource_type = $1 ORDER BY er.resource_name`
	rows, err := r.DB.QueryContext(ctx, q, rt)
	if err != nil {
		return nil, err
	}
	return collectRows(rows, scanExtensionResourceView)
}

// ---------------------------------------------------------------------------
// Intents (reuses the shared resource_intents table)
// ---------------------------------------------------------------------------

func (r *ExtensionResourceRepo) GetIntent(ctx context.Context, rt domain.ResourceType, name domain.ResourceName, version domain.IntentVersion) (domain.ResourceIntent, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT resource_type, name, version, spec, created_at
		 FROM resource_intents WHERE resource_type = $1 AND name = $2 AND version = $3`,
		rt, name, version)
	var ri domain.ResourceIntent
	var specStr, createdAt string
	if err := row.Scan(&ri.ResourceType, &ri.Name, &ri.Version, &specStr, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ResourceIntent{}, fmt.Errorf("%w: intent %s/%s v%d", domain.ErrNotFound, rt, name, version)
		}
		return domain.ResourceIntent{}, err
	}
	ri.Spec = compactJSONB(specStr)
	if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
		ri.CreatedAt = t
	}
	return ri, nil
}

func (r *ExtensionResourceRepo) DeleteIntents(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) error {
	_, err := r.DB.ExecContext(ctx,
		`DELETE FROM resource_intents WHERE resource_type = $1 AND name = $2`,
		rt, name)
	if err != nil {
		return fmt.Errorf("delete intents for extension resource %s/%s: %w", rt, name, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Scan helpers and query fragments
// ---------------------------------------------------------------------------

const erSelectColumns = `SELECT er.uid, er.resource_type, er.resource_name, er.labels, er.created_at, er.updated_at,
	erm.current_version, erm.fulfillment_id `

var erViewQuery = `SELECT
	er.uid, er.resource_type, er.resource_name, er.labels, er.created_at, er.updated_at,
	erm.current_version, erm.fulfillment_id,
	ri.spec, ri.created_at,
	` + fulfillmentColumnsJoined("f") + `
FROM extension_resources er
JOIN extension_resource_managed erm ON erm.extension_resource_uid = er.uid
JOIN resource_intents ri
  ON ri.resource_type = er.resource_type AND ri.name = er.resource_name AND ri.version = erm.current_version
JOIN fulfillments f ON f.id = erm.fulfillment_id
` + strategyJoins("f") + ` `

func scanExtensionResourceType(s scanner) (domain.ExtensionResourceType, error) {
	var snap domain.ExtensionResourceTypeSnapshot
	var apiVersion, collectionID string
	var mgmtJSON sql.NullString

	if err := s.Scan(
		&snap.ResourceType, &apiVersion, &collectionID,
		&mgmtJSON, &snap.CreatedAt, &snap.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ExtensionResourceType{}, domain.ErrNotFound
		}
		return domain.ExtensionResourceType{}, err
	}

	snap.APIVersion = domain.APIVersion(apiVersion)
	snap.CollectionID = domain.CollectionID(collectionID)

	if mgmtJSON.Valid {
		var mt domain.ManagementType
		if err := json.Unmarshal([]byte(mgmtJSON.String), &mt); err != nil {
			return domain.ExtensionResourceType{}, fmt.Errorf("unmarshal management: %w", err)
		}
		snap.Management = &domain.ManagementTypeSnapshot{
			Relation:  mt.Relation(),
			Signature: mt.Signature(),
		}
	}

	return domain.ExtensionResourceTypeFromSnapshot(snap), nil
}

func scanExtensionResourceSnapshot(s scanner) (domain.ExtensionResourceSnapshot, error) {
	var snap domain.ExtensionResourceSnapshot
	var labelsStr string
	var currentVersion sql.NullInt64
	var fulfillmentID sql.NullString

	if err := s.Scan(
		&snap.UID, &snap.ResourceType, &snap.Name, &labelsStr,
		&snap.CreatedAt, &snap.UpdatedAt,
		&currentVersion, &fulfillmentID,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return snap, domain.ErrNotFound
		}
		return snap, fmt.Errorf("scan extension resource: %w", err)
	}

	if err := json.Unmarshal([]byte(labelsStr), &snap.Labels); err != nil {
		return snap, fmt.Errorf("unmarshal labels: %w", err)
	}

	if fulfillmentID.Valid {
		snap.Managed = &domain.ManagedStateSnapshot{
			CurrentVersion: domain.IntentVersion(currentVersion.Int64),
			FulfillmentID:  domain.FulfillmentID(fulfillmentID.String),
		}
	}

	return snap, nil
}

func scanExtensionResourceView(s scanner) (domain.ExtensionResourceView, error) {
	var v domain.ExtensionResourceView

	var uid domain.ExtensionResourceUID
	var resourceType domain.ResourceType
	var name domain.ResourceName
	var labelsStr string
	var erCreatedAt, erUpdatedAt time.Time

	var currentVersion int64
	var managedFID string

	var riSpec, riCreatedAt string

	var fID, rtJSON, stateStr, pauseReason, statusReason, authJSON, fCreatedAt, fUpdatedAt string
	var msSpec, psSpec, rsSpec, provJSON, attestRefJSON sql.NullString
	var msVer, psVer, rsVer, generation, observedGeneration int64
	var activeWorkflowGen sql.NullInt64

	if err := s.Scan(
		&uid, &resourceType, &name, &labelsStr,
		&erCreatedAt, &erUpdatedAt,
		&currentVersion, &managedFID,
		&riSpec, &riCreatedAt,
		&fID, &msVer, &msSpec, &psVer, &psSpec, &rsVer, &rsSpec,
		&rtJSON, &stateStr, &pauseReason, &statusReason, &authJSON, &provJSON, &attestRefJSON,
		&generation, &observedGeneration, &activeWorkflowGen,
		&fCreatedAt, &fUpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ExtensionResourceView{}, domain.ErrNotFound
		}
		return domain.ExtensionResourceView{}, fmt.Errorf("scan extension resource view: %w", err)
	}

	var labels map[string]string
	if err := json.Unmarshal([]byte(labelsStr), &labels); err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("unmarshal labels: %w", err)
	}

	erSnap := domain.ExtensionResourceSnapshot{
		UID:          uid,
		ResourceType: resourceType,
		Name:         name,
		Labels:       labels,
		CreatedAt:    erCreatedAt,
		UpdatedAt:    erUpdatedAt,
		Managed: &domain.ManagedStateSnapshot{
			CurrentVersion: domain.IntentVersion(currentVersion),
			FulfillmentID:  domain.FulfillmentID(managedFID),
		},
	}
	v.Resource = *domain.ExtensionResourceFromSnapshot(erSnap)

	v.Intent = &domain.ResourceIntent{
		ResourceType: resourceType,
		Name:         name,
		Version:      domain.IntentVersion(currentVersion),
		Spec:         compactJSONB(riSpec),
	}
	if t, err := time.Parse(time.RFC3339, riCreatedAt); err == nil {
		v.Intent.CreatedAt = t
	}

	fSnap, err := fulfillmentSnapshotFromColumns(
		fID, msVer, msSpec, psVer, psSpec, rsVer, rsSpec,
		rtJSON, stateStr, pauseReason, statusReason, authJSON, provJSON, attestRefJSON,
		generation, observedGeneration, activeWorkflowGen,
		fCreatedAt, fUpdatedAt,
	)
	if err != nil {
		return domain.ExtensionResourceView{}, err
	}
	v.Fulfillment = domain.FulfillmentFromSnapshot(fSnap)

	return v, nil
}

// compactJSONB strips insignificant whitespace that Postgres JSONB
// adds during storage normalization, preserving round-trip fidelity
// with the original compact JSON form.
func compactJSONB(s string) json.RawMessage {
	var buf bytes.Buffer
	if err := json.Compact(&buf, []byte(s)); err != nil {
		return json.RawMessage(s)
	}
	return json.RawMessage(buf.Bytes())
}

// marshalManagementSnapshot converts a [domain.ManagementTypeSnapshot]
// into JSONB-ready bytes. Returns nil for nil input (SQL NULL).
func marshalManagementSnapshot(snap *domain.ManagementTypeSnapshot) ([]byte, error) {
	if snap == nil {
		return nil, nil
	}
	mt, err := domain.NewManagementType(snap.Relation, snap.Signature)
	if err != nil {
		return nil, err
	}
	return json.Marshal(mt)
}
