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
	"github.com/google/uuid"
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
	var invJSON sql.NullString
	if snap.Inventory != nil {
		invJSON = sql.NullString{String: "{}", Valid: true}
	}
	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_types
			(service_name, type_name, api_version, collection_id, management, inventory, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		string(snap.ResourceType.ServiceName()), snap.ResourceType.TypeName(),
		string(snap.APIVersion), string(snap.CollectionID),
		nullStringFromBytes(mgmtJSON),
		invJSON,
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
		`SELECT service_name, type_name, api_version, collection_id, management, inventory, created_at, updated_at
		 FROM extension_resource_types WHERE service_name = $1 AND type_name = $2`,
		string(rt.ServiceName()), rt.TypeName())
	return scanExtensionResourceType(row)
}

func (r *ExtensionResourceRepo) ListTypes(ctx context.Context) ([]domain.ExtensionResourceType, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT service_name, type_name, api_version, collection_id, management, inventory, created_at, updated_at
		 FROM extension_resource_types ORDER BY service_name, type_name`)
	if err != nil {
		return nil, err
	}
	return collectRows(rows, scanExtensionResourceType)
}

func (r *ExtensionResourceRepo) DeleteType(ctx context.Context, rt domain.ResourceType) error {
	res, err := r.DB.ExecContext(ctx,
		`DELETE FROM extension_resource_types WHERE service_name = $1 AND type_name = $2`,
		string(rt.ServiceName()), rt.TypeName())
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

	labelsJSON, err := json.Marshal(snap.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}
	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO extension_resources (uid, service_name, type_name, resource_name, labels, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		snap.UID.String(), string(snap.ResourceType.ServiceName()), snap.ResourceType.TypeName(), snap.Name,
		string(labelsJSON),
		snap.CreatedAt.UTC(), snap.UpdatedAt.UTC())
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: extension resource %s/%s", domain.ErrAlreadyExists, snap.ResourceType.ServiceName(), snap.Name)
		}
		return err
	}

	// Flush pending intents to the resource_intents table keyed by UID.
	for _, intent := range snap.PendingIntents {
		if _, err := r.DB.ExecContext(ctx,
			`INSERT INTO resource_intents (extension_resource_uid, version, spec, created_at)
			 VALUES ($1, $2, $3, $4)`,
			intent.ExtensionResourceUID.String(), intent.Version, string(intent.Spec),
			intent.CreatedAt.UTC().Format(time.RFC3339)); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("%w: intent %s v%d", domain.ErrAlreadyExists, intent.ExtensionResourceUID, intent.Version)
			}
			return err
		}
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

	if snap.Inventory != nil {
		if err := r.insertInventory(ctx, snap.UID, snap.Inventory); err != nil {
			return fmt.Errorf("insert inventory state: %w", err)
		}
	}

	return nil
}

func (r *ExtensionResourceRepo) Get(ctx context.Context, name domain.FullResourceName) (*domain.ExtensionResource, error) {
	row := r.DB.QueryRowContext(ctx,
		erSelectColumns+`
		FROM extension_resources er
		LEFT JOIN extension_resource_managed erm ON erm.extension_resource_uid = er.uid
		WHERE er.service_name = $1 AND er.resource_name = $2`,
		string(name.ServiceName()), string(name.ResourceName()))
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
		WHERE er.service_name = $1 AND er.type_name = $2 ORDER BY er.resource_name`,
		string(rt.ServiceName()), rt.TypeName())
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

func (r *ExtensionResourceRepo) Delete(ctx context.Context, name domain.FullResourceName) error {
	res, err := r.DB.ExecContext(ctx,
		`DELETE FROM extension_resources WHERE service_name = $1 AND resource_name = $2`,
		string(name.ServiceName()), string(name.ResourceName()))
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: extension resource %s", domain.ErrNotFound, name)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------

func (r *ExtensionResourceRepo) GetView(ctx context.Context, name domain.FullResourceName) (domain.ExtensionResourceView, error) {
	q := erViewQueryPG + `
		WHERE er.service_name = $1 AND er.resource_name = $2`
	row := r.DB.QueryRowContext(ctx, q, string(name.ServiceName()), string(name.ResourceName()))
	return scanExtensionResourceView(row)
}

func (r *ExtensionResourceRepo) ListViewsByType(ctx context.Context, rt domain.ResourceType) ([]domain.ExtensionResourceView, error) {
	q := erViewQueryPG + `
		WHERE er.service_name = $1 AND er.type_name = $2 ORDER BY er.resource_name`
	rows, err := r.DB.QueryContext(ctx, q, string(rt.ServiceName()), rt.TypeName())
	if err != nil {
		return nil, err
	}
	return collectRows(rows, scanExtensionResourceView)
}

// ---------------------------------------------------------------------------
// Intents (reuses the shared resource_intents table)
// ---------------------------------------------------------------------------

func (r *ExtensionResourceRepo) GetIntent(ctx context.Context, uid domain.ExtensionResourceUID, version domain.IntentVersion) (domain.ResourceIntent, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT extension_resource_uid, version, spec, created_at
		 FROM resource_intents WHERE extension_resource_uid = $1 AND version = $2`,
		uid.String(), version)
	var ri domain.ResourceIntent
	var specStr, createdAt string
	if err := row.Scan(&ri.ExtensionResourceUID, &ri.Version, &specStr, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ResourceIntent{}, fmt.Errorf("%w: intent %s v%d", domain.ErrNotFound, uid, version)
		}
		return domain.ResourceIntent{}, err
	}
	ri.Spec = compactJSONB(specStr)
	if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
		ri.CreatedAt = t
	}
	return ri, nil
}

// ---------------------------------------------------------------------------
// Scan helpers and query fragments
// ---------------------------------------------------------------------------

const erSelectColumns = `SELECT er.uid, er.service_name, er.type_name, er.resource_name, er.labels, er.created_at, er.updated_at,
	erm.current_version, erm.fulfillment_id `

var erViewQueryPG = `SELECT
	er.uid, er.service_name, er.type_name, er.resource_name, er.labels, er.created_at, er.updated_at,
	erm.current_version, erm.fulfillment_id,
	ri.spec, ri.created_at,
	` + fulfillmentColumnsJoined("f") + `,
	inv.labels, inv.observation, inv.observed_at, inv.updated_at,
	(SELECT jsonb_agg(jsonb_build_object(
		'type', c.type,
		'status', c.status,
		'reason', c.reason,
		'message', c.message,
		'last_transition_time', c.last_transition_time
	) ORDER BY c.type)
	 FROM extension_resource_inventory_conditions c
	 WHERE c.extension_resource_uid = er.uid) AS inv_conditions
FROM extension_resources er
LEFT JOIN extension_resource_managed erm ON erm.extension_resource_uid = er.uid
LEFT JOIN resource_intents ri
  ON ri.extension_resource_uid = er.uid AND ri.version = erm.current_version
LEFT JOIN fulfillments f ON f.id = erm.fulfillment_id
` + strategyJoins("f") + `
LEFT JOIN extension_resource_inventory inv ON inv.extension_resource_uid = er.uid
`

func scanExtensionResourceType(s scanner) (domain.ExtensionResourceType, error) {
	var snap domain.ExtensionResourceTypeSnapshot
	var serviceName, typeName, apiVersion, collectionID string
	var mgmtJSON, invJSON sql.NullString

	if err := s.Scan(
		&serviceName, &typeName, &apiVersion, &collectionID,
		&mgmtJSON, &invJSON, &snap.CreatedAt, &snap.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ExtensionResourceType{}, domain.ErrNotFound
		}
		return domain.ExtensionResourceType{}, err
	}

	snap.ResourceType = domain.ResourceType(serviceName + "/" + typeName)
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
	if invJSON.Valid {
		snap.Inventory = &domain.InventoryTypeSnapshot{}
	}

	return domain.ExtensionResourceTypeFromSnapshot(snap), nil
}

func scanExtensionResourceSnapshot(s scanner) (domain.ExtensionResourceSnapshot, error) {
	var snap domain.ExtensionResourceSnapshot
	var serviceName, typeName, labelsStr string
	var currentVersion sql.NullInt64
	var fulfillmentID sql.NullString

	if err := s.Scan(
		&snap.UID, &serviceName, &typeName, &snap.Name, &labelsStr,
		&snap.CreatedAt, &snap.UpdatedAt,
		&currentVersion, &fulfillmentID,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return snap, domain.ErrNotFound
		}
		return snap, fmt.Errorf("scan extension resource: %w", err)
	}

	snap.ResourceType = domain.ResourceType(serviceName + "/" + typeName)

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
	var serviceName, typeName string
	var name domain.ResourceName
	var labelsStr string
	var erCreatedAt, erUpdatedAt time.Time

	var currentVersion sql.NullInt64
	var managedFID sql.NullString

	var riSpec, riCreatedAt sql.NullString

	var fID, rtJSON, stateStr, pauseReason, statusReason, authJSON, fCreatedAt, fUpdatedAt sql.NullString
	var msSpec, psSpec, rsSpec, provJSON, attestRefJSON sql.NullString
	var msVer, psVer, rsVer, generation, observedGeneration sql.NullInt64
	var activeWorkflowGen sql.NullInt64

	// Inventory columns (all nullable)
	var invLabels, invObservation sql.NullString
	var invObservedAt, invUpdatedAt *time.Time
	var invConditionsJSON sql.NullString

	if err := s.Scan(
		&uid, &serviceName, &typeName, &name, &labelsStr,
		&erCreatedAt, &erUpdatedAt,
		&currentVersion, &managedFID,
		&riSpec, &riCreatedAt,
		&fID, &msVer, &msSpec, &psVer, &psSpec, &rsVer, &rsSpec,
		&rtJSON, &stateStr, &pauseReason, &statusReason, &authJSON, &provJSON, &attestRefJSON,
		&generation, &observedGeneration, &activeWorkflowGen,
		&fCreatedAt, &fUpdatedAt,
		&invLabels, &invObservation, &invObservedAt, &invUpdatedAt,
		&invConditionsJSON,
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

	resourceType := domain.ResourceType(serviceName + "/" + typeName)

	erSnap := domain.ExtensionResourceSnapshot{
		UID:          uid,
		ResourceType: resourceType,
		Name:         name,
		Labels:       labels,
		CreatedAt:    erCreatedAt,
		UpdatedAt:    erUpdatedAt,
	}
	if managedFID.Valid {
		erSnap.Managed = &domain.ManagedStateSnapshot{
			CurrentVersion: domain.IntentVersion(currentVersion.Int64),
			FulfillmentID:  domain.FulfillmentID(managedFID.String),
		}
	}
	v.Resource = *domain.ExtensionResourceFromSnapshot(erSnap)

	// Intent and fulfillment are only populated for managed resources.
	if riSpec.Valid {
		v.Intent = &domain.ResourceIntent{
			ExtensionResourceUID: uid,
			Version:              domain.IntentVersion(currentVersion.Int64),
			Spec:                 compactJSONB(riSpec.String),
		}
		if riCreatedAt.Valid {
			if t, err := time.Parse(time.RFC3339, riCreatedAt.String); err == nil {
				v.Intent.CreatedAt = t
			}
		}
	}

	if fID.Valid {
		fSnap, err := fulfillmentSnapshotFromColumns(
			fID.String, msVer.Int64, msSpec, psVer.Int64, psSpec, rsVer.Int64, rsSpec,
			rtJSON.String, stateStr.String, pauseReason.String, statusReason.String, authJSON.String,
			provJSON, attestRefJSON,
			generation.Int64, observedGeneration.Int64, activeWorkflowGen,
			fCreatedAt.String, fUpdatedAt.String,
		)
		if err != nil {
			return domain.ExtensionResourceView{}, err
		}
		v.Fulfillment = domain.FulfillmentFromSnapshot(fSnap)
	}

	// Inventory: build snapshot including conditions, hydrate once.
	if invObservedAt != nil {
		invSnap := domain.InventoryResourceSnapshot{
			Labels:     map[string]string{},
			ObservedAt: *invObservedAt,
		}
		if invLabels.Valid {
			json.Unmarshal([]byte(invLabels.String), &invSnap.Labels)
		}
		if invObservation.Valid {
			invSnap.Observation = compactJSONB(invObservation.String)
		}
		if invUpdatedAt != nil {
			invSnap.UpdatedAt = *invUpdatedAt
		}
		if invConditionsJSON.Valid {
			invSnap.Conditions, _ = unmarshalConditionSnapshots([]byte(invConditionsJSON.String))
		}
		v.Inventory = domain.InventoryResourceFromSnapshot(invSnap)
	}

	return v, nil
}

// conditionRow mirrors the JSON shape produced by the jsonb_agg subquery.
type conditionRow struct {
	Type               string    `json:"type"`
	Status             string    `json:"status"`
	Reason             string    `json:"reason"`
	Message            string    `json:"message"`
	LastTransitionTime time.Time `json:"last_transition_time"`
}

func unmarshalConditionSnapshots(data []byte) ([]domain.ConditionSnapshot, error) {
	var rows []conditionRow
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	snaps := make([]domain.ConditionSnapshot, len(rows))
	for i, r := range rows {
		snaps[i] = domain.ConditionSnapshot{
			Type:               domain.ConditionType(r.Type),
			Status:             domain.ConditionStatus(r.Status),
			Reason:             r.Reason,
			Message:            r.Message,
			LastTransitionTime: r.LastTransitionTime,
		}
	}
	return snaps, nil
}

// ---------------------------------------------------------------------------
// Inventory methods
// ---------------------------------------------------------------------------

func (r *ExtensionResourceRepo) upsertInventoryRow(ctx context.Context, uid domain.ExtensionResourceUID, inv *domain.InventoryResourceSnapshot) error {
	labelsJSON, _ := json.Marshal(inv.Labels)
	obs := inv.Observation
	if obs == nil {
		obs = json.RawMessage("{}")
	}
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_inventory
			(extension_resource_uid, labels, observation, observed_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT(extension_resource_uid) DO UPDATE SET
			labels = EXCLUDED.labels,
			observation = EXCLUDED.observation,
			observed_at = EXCLUDED.observed_at,
			updated_at = EXCLUDED.updated_at`,
		uid.String(), string(labelsJSON), string(obs),
		inv.ObservedAt.UTC(), inv.UpdatedAt.UTC())
	return err
}

func (r *ExtensionResourceRepo) insertInventory(ctx context.Context, uid domain.ExtensionResourceUID, inv *domain.InventoryResourceSnapshot) error {
	if err := r.upsertInventoryRow(ctx, uid, inv); err != nil {
		return err
	}
	return r.recordConditionSnapshots(ctx, uid, inv.Conditions, inv.ObservedAt)
}

func (r *ExtensionResourceRepo) UpsertInventory(ctx context.Context, updates []domain.InventoryUpdate) error {
	for _, u := range updates {
		s := u.Inventory.Snapshot()
		if err := r.upsertInventoryRow(ctx, u.ExtensionResourceUID, &s); err != nil {
			return fmt.Errorf("upsert inventory for %s: %w", u.ExtensionResourceUID, err)
		}
		if err := r.recordConditionSnapshots(ctx, u.ExtensionResourceUID, s.Conditions, s.ObservedAt); err != nil {
			return fmt.Errorf("record conditions for %s: %w", u.ExtensionResourceUID, err)
		}
	}
	return nil
}

// recordConditionSnapshots feeds a set of ConditionSnapshots (from an
// inventory upsert) through the shared condition recording flow.
func (r *ExtensionResourceRepo) recordConditionSnapshots(ctx context.Context, uid domain.ExtensionResourceUID, conds []domain.ConditionSnapshot, observedAt time.Time) error {
	now := time.Now().UTC()
	for _, c := range conds {
		if err := r.recordCondition(ctx, uid,
			c.Type, c.Status, c.Reason, c.Message,
			c.LastTransitionTime, observedAt, now); err != nil {
			return err
		}
	}
	return nil
}

func (r *ExtensionResourceRepo) AppendObservations(ctx context.Context, observations []domain.Observation) error {
	for _, o := range observations {
		obs := o.Observation()
		if obs == nil {
			obs = json.RawMessage("{}")
		}
		_, err := r.DB.ExecContext(ctx,
			`INSERT INTO extension_resource_inventory_observations
				(id, extension_resource_uid, observation, observed_at, created_at)
			 VALUES ($1, $2, $3, $4, $5)`,
			string(o.ID()), o.ExtensionResourceUID().String(),
			string(obs), o.ObservedAt().UTC(), o.CreatedAt().UTC())
		if err != nil {
			return fmt.Errorf("append observation %s: %w", o.ID(), err)
		}
	}
	return nil
}

func (r *ExtensionResourceRepo) ListObservations(ctx context.Context, uid domain.ExtensionResourceUID, limit int) ([]domain.Observation, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT id, extension_resource_uid, observation, observed_at, created_at
		 FROM extension_resource_inventory_observations
		 WHERE extension_resource_uid = $1
		 ORDER BY observed_at DESC
		 LIMIT $2`,
		uid.String(), limit)
	if err != nil {
		return nil, err
	}
	return collectRows(rows, func(s scanner) (domain.Observation, error) {
		var snap domain.ObservationSnapshot
		var idStr, erUID, obsStr string
		if err := s.Scan(&idStr, &erUID, &obsStr, &snap.ObservedAt, &snap.CreatedAt); err != nil {
			return domain.Observation{}, err
		}
		parsedUID, err := domain.ParseExtensionResourceUID(erUID)
		if err != nil {
			return domain.Observation{}, err
		}
		snap.ID = domain.ObservationID(idStr)
		snap.ExtensionResourceUID = parsedUID
		snap.Observation = compactJSONB(obsStr)
		return domain.ObservationFromSnapshot(snap), nil
	})
}

func (r *ExtensionResourceRepo) RecordConditions(ctx context.Context, reports []domain.ConditionReport) error {
	if len(reports) == 0 {
		return nil
	}
	// Ensure a minimal inventory row exists for every unique UID in the
	// batch so conditions can be reported before a full inventory upsert.
	seen := make(map[domain.ExtensionResourceUID]struct{})
	for _, rpt := range reports {
		uid := rpt.ExtensionResourceUID()
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		if err := r.ensureInventoryRow(ctx, uid); err != nil {
			return err
		}
	}
	now := time.Now().UTC()
	for _, rpt := range reports {
		if err := r.recordCondition(ctx, rpt.ExtensionResourceUID(),
			rpt.ConditionType(), rpt.Status(), rpt.Reason(), rpt.Message(),
			rpt.LastTransitionTime(), rpt.ObservedAt(), now); err != nil {
			return err
		}
	}
	return nil
}

// ensureInventoryRow creates a minimal inventory row if one doesn't
// exist. This allows conditions to be recorded before a full inventory
// upsert has been performed.
func (r *ExtensionResourceRepo) ensureInventoryRow(ctx context.Context, uid domain.ExtensionResourceUID) error {
	now := time.Now().UTC()
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_inventory
			(extension_resource_uid, labels, observation, observed_at, updated_at)
		 VALUES ($1, '{}', '{}', $2, $3)
		 ON CONFLICT (extension_resource_uid) DO NOTHING`,
		uid.String(), now, now)
	return err
}

// recordCondition is the shared condition recording path. It:
//  1. UPSERTs the latest condition state (always, for staleness tracking)
//  2. INSERTs a transition record only if the condition actually changed
//
// For Postgres this is a single statement using a CTE: the prev CTE
// captures the state before the UPSERT, and the final INSERT checks
// prev to decide whether a transition occurred.
func (r *ExtensionResourceRepo) recordCondition(
	ctx context.Context,
	uid domain.ExtensionResourceUID,
	condType domain.ConditionType,
	status domain.ConditionStatus,
	reason, message string,
	lastTransitionTime, observedAt, now time.Time,
) error {
	id := uuid.New().String()
	_, err := r.DB.ExecContext(ctx,
		`WITH prev AS (
			SELECT status, reason, message
			FROM extension_resource_inventory_conditions
			WHERE extension_resource_uid = $1 AND type = $2
		),
		upserted AS (
			INSERT INTO extension_resource_inventory_conditions
				(extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (extension_resource_uid, type) DO UPDATE SET
				status = EXCLUDED.status,
				reason = EXCLUDED.reason,
				message = EXCLUDED.message,
				last_transition_time = EXCLUDED.last_transition_time,
				observed_at = EXCLUDED.observed_at,
				updated_at = EXCLUDED.updated_at
			RETURNING 1
		)
		INSERT INTO extension_resource_inventory_condition_events
			(id, extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, created_at)
		SELECT $9, $1, $2, $3, $4, $5, $6, $7, $10
		WHERE NOT EXISTS (
			SELECT 1 FROM prev
			WHERE prev.status = $3 AND prev.reason = $4 AND prev.message = $5
		)`,
		uid.String(), string(condType),
		string(status), reason, message,
		lastTransitionTime.UTC(), observedAt.UTC(), now,
		id, now)
	if err != nil {
		return fmt.Errorf("record condition %s/%s: %w", uid, condType, err)
	}
	return nil
}

func (r *ExtensionResourceRepo) ListConditionTransitions(ctx context.Context, uid domain.ExtensionResourceUID, conditionType *domain.ConditionType, limit int) ([]domain.ConditionTransition, error) {
	var q string
	var args []any
	if conditionType != nil {
		q = `SELECT id, extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, created_at
			 FROM extension_resource_inventory_condition_events
			 WHERE extension_resource_uid = $1 AND type = $2
			 ORDER BY observed_at DESC
			 LIMIT $3`
		args = []any{uid.String(), string(*conditionType), limit}
	} else {
		q = `SELECT id, extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, created_at
			 FROM extension_resource_inventory_condition_events
			 WHERE extension_resource_uid = $1
			 ORDER BY observed_at DESC
			 LIMIT $2`
		args = []any{uid.String(), limit}
	}
	rows, err := r.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return collectRows(rows, func(s scanner) (domain.ConditionTransition, error) {
		var snap domain.ConditionTransitionSnapshot
		var idStr, erUID, ctStr, statusStr string
		if err := s.Scan(&idStr, &erUID, &ctStr, &statusStr,
			&snap.Reason, &snap.Message, &snap.LastTransitionTime, &snap.ObservedAt, &snap.CreatedAt); err != nil {
			return domain.ConditionTransition{}, err
		}
		parsedUID, err := domain.ParseExtensionResourceUID(erUID)
		if err != nil {
			return domain.ConditionTransition{}, err
		}
		snap.ID = domain.ConditionTransitionID(idStr)
		snap.ExtensionResourceUID = parsedUID
		snap.ConditionType = domain.ConditionType(ctStr)
		snap.Status = domain.ConditionStatus(statusStr)
		return domain.ConditionTransitionFromSnapshot(snap), nil
	})
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
