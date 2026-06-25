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

var _ domain.ExtensionResourceRepository = (*ExtensionResourceRepo)(nil)

// ExtensionResourceRepo implements [domain.ExtensionResourceRepository] for SQLite.
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
	var mgmtJSON sql.NullString
	if snap.Management != nil {
		mt, _ := domain.NewManagementType(snap.Management.Relation, snap.Management.Signature)
		b, err := json.Marshal(mt)
		if err != nil {
			return fmt.Errorf("marshal management: %w", err)
		}
		mgmtJSON = sql.NullString{String: string(b), Valid: true}
	}

	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_types (resource_type, api_service_name, api_version, collection_id, management, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		snap.ResourceType,
		string(snap.APIServiceName),
		string(snap.APIVersion),
		string(snap.CollectionID),
		mgmtJSON,
		snap.CreatedAt.UTC().Format(time.RFC3339Nano),
		snap.UpdatedAt.UTC().Format(time.RFC3339Nano))
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: extension resource type %q", domain.ErrAlreadyExists, snap.ResourceType)
		}
		return err
	}
	return nil
}

func (r *ExtensionResourceRepo) GetType(ctx context.Context, rt domain.ResourceType) (domain.ExtensionResourceType, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT resource_type, api_service_name, api_version, collection_id, management, created_at, updated_at
		 FROM extension_resource_types WHERE resource_type = ?`, rt)
	return r.scanType(row)
}

func (r *ExtensionResourceRepo) ListTypes(ctx context.Context) ([]domain.ExtensionResourceType, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT resource_type, api_service_name, api_version, collection_id, management, created_at, updated_at
		 FROM extension_resource_types ORDER BY resource_type`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var defs []domain.ExtensionResourceType
	for rows.Next() {
		def, err := r.scanType(rows)
		if err != nil {
			return nil, err
		}
		defs = append(defs, def)
	}
	return defs, rows.Err()
}

func (r *ExtensionResourceRepo) DeleteType(ctx context.Context, rt domain.ResourceType) error {
	res, err := r.DB.ExecContext(ctx,
		`DELETE FROM extension_resource_types WHERE resource_type = ?`, rt)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: extension resource type %q", domain.ErrNotFound, rt)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Instance aggregate
// ---------------------------------------------------------------------------

func (r *ExtensionResourceRepo) Create(ctx context.Context, er *domain.ExtensionResource) error {
	s := er.Snapshot()

	// Flush pending intents.
	for _, intent := range s.PendingIntents {
		if _, err := r.DB.ExecContext(ctx,
			`INSERT INTO resource_intents (resource_type, name, version, spec, created_at)
			 VALUES (?, ?, ?, ?, ?)`,
			intent.ResourceType, intent.Name, intent.Version, string(intent.Spec),
			intent.CreatedAt.UTC().Format(time.RFC3339Nano)); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("%w: intent %s/%s v%d", domain.ErrAlreadyExists, intent.ResourceType, intent.Name, intent.Version)
			}
			return err
		}
	}

	labelsJSON, err := json.Marshal(s.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO extension_resources (uid, resource_type, resource_name, labels, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		s.UID.String(), s.ResourceType, s.Name,
		string(labelsJSON),
		s.CreatedAt.UTC().Format(time.RFC3339Nano),
		s.UpdatedAt.UTC().Format(time.RFC3339Nano))
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: extension resource %s/%s", domain.ErrAlreadyExists, s.ResourceType, s.Name)
		}
		return err
	}

	// Insert managed state row if present.
	if s.Managed != nil {
		_, err = r.DB.ExecContext(ctx,
			`INSERT INTO extension_resource_managed (extension_resource_uid, current_version, fulfillment_id)
			 VALUES (?, ?, ?)`,
			s.UID.String(), int64(s.Managed.CurrentVersion), string(s.Managed.FulfillmentID))
		if err != nil {
			return fmt.Errorf("insert managed state: %w", err)
		}
	}

	return nil
}

func (r *ExtensionResourceRepo) Get(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) (*domain.ExtensionResource, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT er.uid, er.resource_type, er.resource_name, er.labels, er.created_at, er.updated_at,
		        m.current_version, m.fulfillment_id
		 FROM extension_resources er
		 LEFT JOIN extension_resource_managed m ON m.extension_resource_uid = er.uid
		 WHERE er.resource_type = ? AND er.resource_name = ?`, rt, name)
	return r.scanInstance(row)
}

func (r *ExtensionResourceRepo) GetByUID(ctx context.Context, uid domain.ExtensionResourceUID) (*domain.ExtensionResource, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT er.uid, er.resource_type, er.resource_name, er.labels, er.created_at, er.updated_at,
		        m.current_version, m.fulfillment_id
		 FROM extension_resources er
		 LEFT JOIN extension_resource_managed m ON m.extension_resource_uid = er.uid
		 WHERE er.uid = ?`, uid.String())
	return r.scanInstance(row)
}

func (r *ExtensionResourceRepo) ListByResourceType(ctx context.Context, rt domain.ResourceType) ([]*domain.ExtensionResource, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT er.uid, er.resource_type, er.resource_name, er.labels, er.created_at, er.updated_at,
		        m.current_version, m.fulfillment_id
		 FROM extension_resources er
		 LEFT JOIN extension_resource_managed m ON m.extension_resource_uid = er.uid
		 WHERE er.resource_type = ? ORDER BY er.resource_name`, rt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []*domain.ExtensionResource
	for rows.Next() {
		inst, err := r.scanInstance(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, inst)
	}
	return results, rows.Err()
}

func (r *ExtensionResourceRepo) Save(ctx context.Context, er *domain.ExtensionResource) error {
	s := er.Snapshot()

	labelsJSON, err := json.Marshal(s.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}

	res, err := r.DB.ExecContext(ctx,
		`UPDATE extension_resources SET labels = ?, updated_at = ? WHERE uid = ?`,
		string(labelsJSON),
		s.UpdatedAt.UTC().Format(time.RFC3339Nano),
		s.UID.String())
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("%w: extension resource %s", domain.ErrNotFound, s.UID)
	}

	if s.Managed != nil {
		_, err = r.DB.ExecContext(ctx,
			`UPDATE extension_resource_managed SET current_version = ?, fulfillment_id = ?
			 WHERE extension_resource_uid = ?`,
			int64(s.Managed.CurrentVersion), string(s.Managed.FulfillmentID),
			s.UID.String())
		if err != nil {
			return err
		}
	}

	for _, intent := range s.PendingIntents {
		if _, err := r.DB.ExecContext(ctx,
			`INSERT INTO resource_intents (resource_type, name, version, spec, created_at)
			 VALUES (?, ?, ?, ?, ?)`,
			intent.ResourceType, intent.Name, intent.Version, string(intent.Spec),
			intent.CreatedAt.UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	return nil
}

func (r *ExtensionResourceRepo) Delete(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) error {
	res, err := r.DB.ExecContext(ctx,
		`DELETE FROM extension_resources WHERE resource_type = ? AND resource_name = ?`, rt, name)
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
	q := `SELECT
		er.uid, er.resource_type, er.resource_name, er.labels, er.created_at, er.updated_at,
		m.current_version, m.fulfillment_id,
		ri.spec, ri.created_at,
		` + fulfillmentColumnsJoined("f") + `
	FROM extension_resources er
	LEFT JOIN extension_resource_managed m ON m.extension_resource_uid = er.uid
	JOIN resource_intents ri
	  ON ri.resource_type = er.resource_type AND ri.name = er.resource_name AND ri.version = m.current_version
	JOIN fulfillments f ON f.id = m.fulfillment_id
	` + strategyJoins("f") + `
	WHERE er.resource_type = ? AND er.resource_name = ?`
	row := r.DB.QueryRowContext(ctx, q, rt, name)
	return r.scanView(row)
}

func (r *ExtensionResourceRepo) ListViewsByType(ctx context.Context, rt domain.ResourceType) ([]domain.ExtensionResourceView, error) {
	q := `SELECT
		er.uid, er.resource_type, er.resource_name, er.labels, er.created_at, er.updated_at,
		m.current_version, m.fulfillment_id,
		ri.spec, ri.created_at,
		` + fulfillmentColumnsJoined("f") + `
	FROM extension_resources er
	LEFT JOIN extension_resource_managed m ON m.extension_resource_uid = er.uid
	JOIN resource_intents ri
	  ON ri.resource_type = er.resource_type AND ri.name = er.resource_name AND ri.version = m.current_version
	JOIN fulfillments f ON f.id = m.fulfillment_id
	` + strategyJoins("f") + `
	WHERE er.resource_type = ? ORDER BY er.resource_name`
	rows, err := r.DB.QueryContext(ctx, q, rt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var views []domain.ExtensionResourceView
	for rows.Next() {
		v, err := r.scanView(rows)
		if err != nil {
			return nil, err
		}
		views = append(views, v)
	}
	return views, rows.Err()
}

// ---------------------------------------------------------------------------
// Intents
// ---------------------------------------------------------------------------

func (r *ExtensionResourceRepo) GetIntent(ctx context.Context, rt domain.ResourceType, name domain.ResourceName, version domain.IntentVersion) (domain.ResourceIntent, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT resource_type, name, version, spec, created_at
		 FROM resource_intents WHERE resource_type = ? AND name = ? AND version = ?`,
		rt, name, version)
	var ri domain.ResourceIntent
	var specStr, createdAt string
	if err := row.Scan(&ri.ResourceType, &ri.Name, &ri.Version, &specStr, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ResourceIntent{}, fmt.Errorf("%w: intent %s/%s v%d", domain.ErrNotFound, rt, name, version)
		}
		return domain.ResourceIntent{}, err
	}
	ri.Spec = json.RawMessage(specStr)
	if t, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
		ri.CreatedAt = t
	}
	return ri, nil
}

func (r *ExtensionResourceRepo) DeleteIntents(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) error {
	_, err := r.DB.ExecContext(ctx,
		`DELETE FROM resource_intents WHERE resource_type = ? AND name = ?`,
		rt, name)
	if err != nil {
		return fmt.Errorf("delete intents for extension resource %s/%s: %w", rt, name, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Scan helpers
// ---------------------------------------------------------------------------

func (r *ExtensionResourceRepo) scanType(s interface{ Scan(...any) error }) (domain.ExtensionResourceType, error) {
	var rtStr, apiServiceName, apiVersion, collectionID, createdAt, updatedAt string
	var mgmtJSON sql.NullString
	if err := s.Scan(&rtStr, &apiServiceName, &apiVersion, &collectionID, &mgmtJSON, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ExtensionResourceType{}, domain.ErrNotFound
		}
		return domain.ExtensionResourceType{}, err
	}

	snap := domain.ExtensionResourceTypeSnapshot{
		ResourceType:   domain.ResourceType(rtStr),
		APIServiceName: domain.ServiceName(apiServiceName),
		APIVersion:     domain.APIVersion(apiVersion),
		CollectionID:   domain.CollectionID(collectionID),
	}
	if t, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
		snap.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339Nano, updatedAt); err == nil {
		snap.UpdatedAt = t
	}
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

func (r *ExtensionResourceRepo) scanInstance(s interface{ Scan(...any) error }) (*domain.ExtensionResource, error) {
	var uidStr, rtStr, nameStr, labelsJSON, createdAt, updatedAt string
	var mVersion sql.NullInt64
	var mFulfillmentID sql.NullString

	if err := s.Scan(&uidStr, &rtStr, &nameStr, &labelsJSON, &createdAt, &updatedAt,
		&mVersion, &mFulfillmentID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}

	uid, err := domain.ParseExtensionResourceUID(uidStr)
	if err != nil {
		return nil, err
	}

	var labels map[string]string
	if err := json.Unmarshal([]byte(labelsJSON), &labels); err != nil {
		return nil, fmt.Errorf("unmarshal labels: %w", err)
	}

	snap := domain.ExtensionResourceSnapshot{
		UID:          uid,
		ResourceType: domain.ResourceType(rtStr),
		Name:         domain.ResourceName(nameStr),
		Labels:       labels,
	}
	if t, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
		snap.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339Nano, updatedAt); err == nil {
		snap.UpdatedAt = t
	}
	if mVersion.Valid {
		snap.Managed = &domain.ManagedStateSnapshot{
			CurrentVersion: domain.IntentVersion(mVersion.Int64),
			FulfillmentID:  domain.FulfillmentID(mFulfillmentID.String),
		}
	}
	return domain.ExtensionResourceFromSnapshot(snap), nil
}

func (r *ExtensionResourceRepo) scanView(s interface{ Scan(...any) error }) (domain.ExtensionResourceView, error) {
	var uidStr, rtStr, nameStr, labelsJSON, erCreatedAt, erUpdatedAt string
	var mVersion sql.NullInt64
	var mFulfillmentID sql.NullString
	var riSpec, riCreatedAt string
	var fCols fulfillmentScanColumns

	if err := s.Scan(append([]any{
		&uidStr, &rtStr, &nameStr, &labelsJSON, &erCreatedAt, &erUpdatedAt,
		&mVersion, &mFulfillmentID,
		&riSpec, &riCreatedAt,
	}, fCols.dests()...)...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ExtensionResourceView{}, domain.ErrNotFound
		}
		return domain.ExtensionResourceView{}, fmt.Errorf("scan extension resource view: %w", err)
	}

	uid, err := domain.ParseExtensionResourceUID(uidStr)
	if err != nil {
		return domain.ExtensionResourceView{}, err
	}

	var labels map[string]string
	if err := json.Unmarshal([]byte(labelsJSON), &labels); err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("unmarshal labels: %w", err)
	}

	snap := domain.ExtensionResourceSnapshot{
		UID:          uid,
		ResourceType: domain.ResourceType(rtStr),
		Name:         domain.ResourceName(nameStr),
		Labels:       labels,
	}
	if t, err := time.Parse(time.RFC3339Nano, erCreatedAt); err == nil {
		snap.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339Nano, erUpdatedAt); err == nil {
		snap.UpdatedAt = t
	}
	if mVersion.Valid {
		snap.Managed = &domain.ManagedStateSnapshot{
			CurrentVersion: domain.IntentVersion(mVersion.Int64),
			FulfillmentID:  domain.FulfillmentID(mFulfillmentID.String),
		}
	}

	resource := domain.ExtensionResourceFromSnapshot(snap)

	intent := &domain.ResourceIntent{
		ResourceType: resource.ResourceType(),
		Name:         resource.Name(),
		Spec:         json.RawMessage(riSpec),
	}
	if resource.Managed() != nil {
		intent.Version = resource.Managed().CurrentVersion()
	}
	if t, err := time.Parse(time.RFC3339Nano, riCreatedAt); err == nil {
		intent.CreatedAt = t
	}

	fs, err := fCols.snapshot()
	if err != nil {
		return domain.ExtensionResourceView{}, err
	}
	fulfillment := domain.FulfillmentFromSnapshot(fs)

	return domain.ExtensionResourceView{
		Resource:    *resource,
		Intent:      intent,
		Fulfillment: fulfillment,
	}, nil
}
