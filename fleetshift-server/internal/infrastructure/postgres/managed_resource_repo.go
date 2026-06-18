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

// ManagedResourceRepo implements [domain.ManagedResourceRepository] for Postgres.
type ManagedResourceRepo struct {
	DB interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	}
}

func (r *ManagedResourceRepo) CreateType(ctx context.Context, def domain.ManagedResourceTypeDef) error {
	relJSON, err := domain.MarshalFulfillmentRelation(def.Relation)
	if err != nil {
		return fmt.Errorf("marshal relation: %w", err)
	}
	sigJSON, err := json.Marshal(def.Signature)
	if err != nil {
		return fmt.Errorf("marshal signature: %w", err)
	}
	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO managed_resource_types (resource_type, relation, signature, api_service_name, api_version, collection_id, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		def.ResourceType, relJSON, sigJSON,
		string(def.APIServiceName), string(def.APIVersion), string(def.CollectionID),
		def.CreatedAt.UTC().Format(time.RFC3339),
		def.UpdatedAt.UTC().Format(time.RFC3339))
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: resource type %q", domain.ErrAlreadyExists, def.ResourceType)
		}
		return err
	}
	return nil
}

func (r *ManagedResourceRepo) GetType(ctx context.Context, rt domain.ResourceType) (domain.ManagedResourceTypeDef, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT resource_type, relation, signature, api_service_name, api_version, collection_id, created_at, updated_at
		 FROM managed_resource_types WHERE resource_type = $1`, rt)
	return scanTypeDef(row)
}

func (r *ManagedResourceRepo) ListTypes(ctx context.Context) ([]domain.ManagedResourceTypeDef, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT resource_type, relation, signature, api_service_name, api_version, collection_id, created_at, updated_at
		 FROM managed_resource_types ORDER BY resource_type`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var defs []domain.ManagedResourceTypeDef
	for rows.Next() {
		def, err := scanTypeDef(rows)
		if err != nil {
			return nil, err
		}
		defs = append(defs, def)
	}
	return defs, rows.Err()
}

func (r *ManagedResourceRepo) DeleteType(ctx context.Context, rt domain.ResourceType) error {
	res, err := r.DB.ExecContext(ctx,
		`DELETE FROM managed_resource_types WHERE resource_type = $1`, rt)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: resource type %q", domain.ErrNotFound, rt)
	}
	return nil
}

func (r *ManagedResourceRepo) GetIntent(ctx context.Context, rt domain.ResourceType, name domain.ResourceName, version domain.IntentVersion) (domain.ResourceIntent, error) {
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
	ri.Spec = json.RawMessage(specStr)
	if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
		ri.CreatedAt = t
	}
	return ri, nil
}

func (r *ManagedResourceRepo) DeleteIntents(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) error {
	_, err := r.DB.ExecContext(ctx,
		`DELETE FROM resource_intents WHERE resource_type = $1 AND name = $2`,
		rt, name)
	if err != nil {
		return fmt.Errorf("delete intents for managed resource %s/%s: %w", rt, name, err)
	}
	return nil
}

func (r *ManagedResourceRepo) CreateInstance(ctx context.Context, mr *domain.ManagedResource) error {
	s := mr.Snapshot()
	for _, intent := range s.PendingIntents {
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

	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO managed_resources (resource_type, name, uid, current_version, fulfillment_id, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		s.ResourceType, s.Name, s.UID, s.CurrentVersion, string(s.FulfillmentID),
		s.CreatedAt.UTC().Format(time.RFC3339),
		s.UpdatedAt.UTC().Format(time.RFC3339))
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: managed resource %s/%s", domain.ErrAlreadyExists, s.ResourceType, s.Name)
		}
		return err
	}
	mr.DrainPendingIntents()
	return nil
}

func (r *ManagedResourceRepo) GetInstance(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) (*domain.ManagedResource, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT resource_type, name, uid, current_version, fulfillment_id, created_at, updated_at, deleted_at
		 FROM managed_resources WHERE resource_type = $1 AND name = $2 AND deleted_at IS NULL`, rt, name)
	snap, err := scanManagedResourceSnapshot(row)
	if err != nil {
		return nil, err
	}
	mr := domain.ManagedResourceFromSnapshot(snap)
	return mr, nil
}

func (r *ManagedResourceRepo) GetView(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) (domain.ManagedResourceView, error) {
	q := `SELECT
		mr.resource_type, mr.name, mr.uid, mr.current_version, mr.fulfillment_id,
		mr.created_at, mr.updated_at, mr.deleted_at,
		ri.spec, ri.created_at,
		` + fulfillmentColumnsJoined("f") + `
	FROM managed_resources mr
	JOIN resource_intents ri
	  ON ri.resource_type = mr.resource_type AND ri.name = mr.name AND ri.version = mr.current_version
	JOIN fulfillments f ON f.id = mr.fulfillment_id
	` + strategyJoins("f") + `
	WHERE mr.resource_type = $1 AND mr.name = $2 AND mr.deleted_at IS NULL`
	row := r.DB.QueryRowContext(ctx, q, rt, name)
	return scanView(row)
}

func (r *ManagedResourceRepo) ListViewsByType(ctx context.Context, rt domain.ResourceType) ([]domain.ManagedResourceView, error) {
	q := `SELECT
		mr.resource_type, mr.name, mr.uid, mr.current_version, mr.fulfillment_id,
		mr.created_at, mr.updated_at, mr.deleted_at,
		ri.spec, ri.created_at,
		` + fulfillmentColumnsJoined("f") + `
	FROM managed_resources mr
	JOIN resource_intents ri
	  ON ri.resource_type = mr.resource_type AND ri.name = mr.name AND ri.version = mr.current_version
	JOIN fulfillments f ON f.id = mr.fulfillment_id
	` + strategyJoins("f") + `
	WHERE mr.resource_type = $1 AND mr.deleted_at IS NULL ORDER BY mr.name`
	rows, err := r.DB.QueryContext(ctx, q, rt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var views []domain.ManagedResourceView
	for rows.Next() {
		v, err := scanView(rows)
		if err != nil {
			return nil, err
		}
		views = append(views, v)
	}
	return views, rows.Err()
}

func (r *ManagedResourceRepo) DeleteInstance(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) error {
	res, err := r.DB.ExecContext(ctx,
		`DELETE FROM managed_resources WHERE resource_type = $1 AND name = $2`, rt, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: managed resource %s/%s", domain.ErrNotFound, rt, name)
	}
	return nil
}

func scanTypeDef(s interface{ Scan(...any) error }) (domain.ManagedResourceTypeDef, error) {
	var def domain.ManagedResourceTypeDef
	var relJSON, sigJSON []byte
	var apiServiceName, apiVersion, collectionID string
	var createdAt, updatedAt string
	if err := s.Scan(&def.ResourceType, &relJSON, &sigJSON, &apiServiceName, &apiVersion, &collectionID, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ManagedResourceTypeDef{}, domain.ErrNotFound
		}
		return domain.ManagedResourceTypeDef{}, err
	}
	rel, err := domain.UnmarshalFulfillmentRelation(relJSON)
	if err != nil {
		return domain.ManagedResourceTypeDef{}, fmt.Errorf("unmarshal relation: %w", err)
	}
	def.Relation = rel
	if err := json.Unmarshal(sigJSON, &def.Signature); err != nil {
		return domain.ManagedResourceTypeDef{}, fmt.Errorf("unmarshal signature: %w", err)
	}
	def.APIServiceName = domain.ServiceName(apiServiceName)
	def.APIVersion = domain.APIVersion(apiVersion)
	def.CollectionID = domain.CollectionID(collectionID)
	if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
		def.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339, updatedAt); err == nil {
		def.UpdatedAt = t
	}
	return def, nil
}

func scanManagedResourceSnapshot(s interface{ Scan(...any) error }) (domain.ManagedResourceSnapshot, error) {
	var snap domain.ManagedResourceSnapshot
	var fID string
	var createdAt, updatedAt string
	var deletedAt sql.NullString
	if err := s.Scan(&snap.ResourceType, &snap.Name, &snap.UID, &snap.CurrentVersion, &fID,
		&createdAt, &updatedAt, &deletedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return snap, domain.ErrNotFound
		}
		return snap, fmt.Errorf("scan managed resource: %w", err)
	}
	snap.FulfillmentID = domain.FulfillmentID(fID)
	if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
		snap.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339, updatedAt); err == nil {
		snap.UpdatedAt = t
	}
	if deletedAt.Valid {
		if t, err := time.Parse(time.RFC3339, deletedAt.String); err == nil {
			snap.DeletedAt = &t
		}
	}
	return snap, nil
}

func scanView(s interface{ Scan(...any) error }) (domain.ManagedResourceView, error) {
	var v domain.ManagedResourceView
	var mrSnap domain.ManagedResourceSnapshot
	var mrCreatedAt, mrUpdatedAt string
	var mrDeletedAt sql.NullString
	var riSpec, riCreatedAt string

	var fID, rtJSON, stateStr, pauseReason, statusReason, authJSON, fCreatedAt, fUpdatedAt string
	var msSpec, psSpec, rsSpec, provJSON, attestRefJSON sql.NullString
	var msVer, psVer, rsVer, generation, observedGeneration int64
	var activeWorkflowGen sql.NullInt64
	var mrFID string

	if err := s.Scan(
		&mrSnap.ResourceType, &mrSnap.Name, &mrSnap.UID,
		&mrSnap.CurrentVersion, &mrFID,
		&mrCreatedAt, &mrUpdatedAt, &mrDeletedAt,
		&riSpec, &riCreatedAt,
		&fID, &msVer, &msSpec, &psVer, &psSpec, &rsVer, &rsSpec,
		&rtJSON, &stateStr, &pauseReason, &statusReason, &authJSON, &provJSON, &attestRefJSON,
		&generation, &observedGeneration, &activeWorkflowGen,
		&fCreatedAt, &fUpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ManagedResourceView{}, domain.ErrNotFound
		}
		return domain.ManagedResourceView{}, fmt.Errorf("scan managed resource view: %w", err)
	}

	mrSnap.FulfillmentID = domain.FulfillmentID(mrFID)
	if t, err := time.Parse(time.RFC3339, mrCreatedAt); err == nil {
		mrSnap.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339, mrUpdatedAt); err == nil {
		mrSnap.UpdatedAt = t
	}
	if mrDeletedAt.Valid {
		if t, err := time.Parse(time.RFC3339, mrDeletedAt.String); err == nil {
			mrSnap.DeletedAt = &t
		}
	}
	v.ManagedResource = *domain.ManagedResourceFromSnapshot(mrSnap)

	v.Intent = domain.ResourceIntent{
		ResourceType: v.ManagedResource.ResourceType(),
		Name:         v.ManagedResource.Name(),
		Version:      v.ManagedResource.CurrentVersion(),
		Spec:         json.RawMessage(riSpec),
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
		return domain.ManagedResourceView{}, err
	}
	v.Fulfillment = *domain.FulfillmentFromSnapshot(fSnap)

	return v, nil
}
