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
	var schemaBytes []byte
	if def.SpecSchema != nil {
		schemaBytes = []byte(*def.SpecSchema)
	}
	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO managed_resource_types (resource_type, relation, signature, spec_schema, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		def.ResourceType, relJSON, sigJSON, nullStringFromBytes(schemaBytes),
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
		`SELECT resource_type, relation, signature, spec_schema, created_at, updated_at
		 FROM managed_resource_types WHERE resource_type = $1`, rt)
	return scanTypeDef(row)
}

func (r *ManagedResourceRepo) ListTypes(ctx context.Context) ([]domain.ManagedResourceTypeDef, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT resource_type, relation, signature, spec_schema, created_at, updated_at
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

func (r *ManagedResourceRepo) CreateInstance(ctx context.Context, mr *domain.ManagedResource) error {
	pending := mr.DrainPendingIntents()
	for i, intent := range pending {
		if _, err := r.DB.ExecContext(ctx,
			`INSERT INTO resource_intents (resource_type, name, version, spec, created_at)
			 VALUES ($1, $2, $3, $4, $5)`,
			intent.ResourceType, intent.Name, intent.Version, string(intent.Spec),
			intent.CreatedAt.UTC().Format(time.RFC3339)); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("%w: intent %s/%s v%d", domain.ErrAlreadyExists, intent.ResourceType, intent.Name, pending[i].Version)
			}
			return err
		}
	}

	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO managed_resources (resource_type, name, uid, current_version, fulfillment_id, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		mr.ResourceType, mr.Name, mr.UID, mr.CurrentVersion, string(mr.FulfillmentID),
		mr.CreatedAt.UTC().Format(time.RFC3339),
		mr.UpdatedAt.UTC().Format(time.RFC3339))
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: managed resource %s/%s", domain.ErrAlreadyExists, mr.ResourceType, mr.Name)
		}
		return err
	}
	return nil
}

func (r *ManagedResourceRepo) GetInstance(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) (*domain.ManagedResource, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT resource_type, name, uid, current_version, fulfillment_id, created_at, updated_at, deleted_at
		 FROM managed_resources WHERE resource_type = $1 AND name = $2 AND deleted_at IS NULL`, rt, name)
	mr, err := scanInstance(row)
	if err != nil {
		return nil, err
	}
	return &mr, nil
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
	var schemaStr sql.NullString
	var createdAt, updatedAt string
	if err := s.Scan(&def.ResourceType, &relJSON, &sigJSON, &schemaStr, &createdAt, &updatedAt); err != nil {
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
	if schemaStr.Valid {
		s := domain.RawSchema(schemaStr.String)
		def.SpecSchema = &s
	}
	if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
		def.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339, updatedAt); err == nil {
		def.UpdatedAt = t
	}
	return def, nil
}

func scanInstance(s interface{ Scan(...any) error }) (domain.ManagedResource, error) {
	var mr domain.ManagedResource
	var fID string
	var createdAt, updatedAt string
	var deletedAt sql.NullString
	if err := s.Scan(&mr.ResourceType, &mr.Name, &mr.UID, &mr.CurrentVersion, &fID,
		&createdAt, &updatedAt, &deletedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ManagedResource{}, domain.ErrNotFound
		}
		return domain.ManagedResource{}, err
	}
	mr.FulfillmentID = domain.FulfillmentID(fID)
	if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
		mr.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339, updatedAt); err == nil {
		mr.UpdatedAt = t
	}
	if deletedAt.Valid {
		if t, err := time.Parse(time.RFC3339, deletedAt.String); err == nil {
			mr.DeletedAt = &t
		}
	}
	return mr, nil
}

func scanView(s interface{ Scan(...any) error }) (domain.ManagedResourceView, error) {
	var v domain.ManagedResourceView
	var mrFID string
	var mrCreatedAt, mrUpdatedAt string
	var mrDeletedAt sql.NullString
	var riSpec, riCreatedAt string

	var fID, rtJSON, stateStr, statusReason, authJSON, fCreatedAt, fUpdatedAt string
	var msSpec, psSpec, rsSpec, provJSON, attestRefJSON sql.NullString
	var msVer, psVer, rsVer, generation, observedGeneration int64
	var activeWorkflowGen sql.NullInt64

	if err := s.Scan(
		&v.ManagedResource.ResourceType, &v.ManagedResource.Name, &v.ManagedResource.UID,
		&v.ManagedResource.CurrentVersion, &mrFID,
		&mrCreatedAt, &mrUpdatedAt, &mrDeletedAt,
		&riSpec, &riCreatedAt,
		&fID, &msVer, &msSpec, &psVer, &psSpec, &rsVer, &rsSpec,
		&rtJSON, &stateStr, &statusReason, &authJSON, &provJSON, &attestRefJSON,
		&generation, &observedGeneration, &activeWorkflowGen,
		&fCreatedAt, &fUpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ManagedResourceView{}, domain.ErrNotFound
		}
		return domain.ManagedResourceView{}, fmt.Errorf("scan managed resource view: %w", err)
	}

	v.ManagedResource.FulfillmentID = domain.FulfillmentID(mrFID)
	if t, err := time.Parse(time.RFC3339, mrCreatedAt); err == nil {
		v.ManagedResource.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339, mrUpdatedAt); err == nil {
		v.ManagedResource.UpdatedAt = t
	}
	if mrDeletedAt.Valid {
		if t, err := time.Parse(time.RFC3339, mrDeletedAt.String); err == nil {
			v.ManagedResource.DeletedAt = &t
		}
	}

	v.Intent = domain.ResourceIntent{
		ResourceType: v.ManagedResource.ResourceType,
		Name:         v.ManagedResource.Name,
		Version:      v.ManagedResource.CurrentVersion,
		Spec:         json.RawMessage(riSpec),
	}
	if t, err := time.Parse(time.RFC3339, riCreatedAt); err == nil {
		v.Intent.CreatedAt = t
	}

	v.Fulfillment.ID = domain.FulfillmentID(fID)
	v.Fulfillment.ManifestStrategyVersion = domain.StrategyVersion(msVer)
	v.Fulfillment.PlacementStrategyVersion = domain.StrategyVersion(psVer)
	v.Fulfillment.RolloutStrategyVersion = domain.StrategyVersion(rsVer)
	v.Fulfillment.State = domain.FulfillmentState(stateStr)
	v.Fulfillment.StatusReason = statusReason
	v.Fulfillment.Generation = domain.Generation(generation)
	v.Fulfillment.ObservedGeneration = domain.Generation(observedGeneration)
	if activeWorkflowGen.Valid {
		g := domain.Generation(activeWorkflowGen.Int64)
		v.Fulfillment.ActiveWorkflowGen = &g
	}
	if t, err := time.Parse(time.RFC3339, fCreatedAt); err == nil {
		v.Fulfillment.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339, fUpdatedAt); err == nil {
		v.Fulfillment.UpdatedAt = t
	}
	if msSpec.Valid {
		_ = json.Unmarshal([]byte(msSpec.String), &v.Fulfillment.ManifestStrategy)
	}
	if psSpec.Valid {
		_ = json.Unmarshal([]byte(psSpec.String), &v.Fulfillment.PlacementStrategy)
	}
	if rsSpec.Valid {
		v.Fulfillment.RolloutStrategy = &domain.RolloutStrategySpec{}
		_ = json.Unmarshal([]byte(rsSpec.String), v.Fulfillment.RolloutStrategy)
	}
	_ = json.Unmarshal([]byte(rtJSON), &v.Fulfillment.ResolvedTargets)
	if authJSON != "" {
		_ = json.Unmarshal([]byte(authJSON), &v.Fulfillment.Auth)
	}
	if provJSON.Valid {
		v.Fulfillment.Provenance = &domain.Provenance{}
		_ = json.Unmarshal([]byte(provJSON.String), v.Fulfillment.Provenance)
	}
	if attestRefJSON.Valid {
		v.Fulfillment.AttestationRef = &domain.AttestationRef{}
		_ = json.Unmarshal([]byte(attestRefJSON.String), v.Fulfillment.AttestationRef)
	}

	return v, nil
}
