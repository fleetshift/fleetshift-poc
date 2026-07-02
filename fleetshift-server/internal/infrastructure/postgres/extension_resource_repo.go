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
		`INSERT INTO extension_resources (uid, service_name, type_name, collection_name, resource_id, labels, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		snap.UID.String(), string(snap.ResourceType.ServiceName()), snap.ResourceType.TypeName(),
		string(snap.Name.Collection()), string(snap.Name.ID()),
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

// erInstanceFromClause is the shared FROM + JOINs for instance
// aggregate reads. Callers prepend erSelectColumns and append WHERE.
const erInstanceFromClause = `
FROM extension_resources er
LEFT JOIN extension_resource_managed erm ON erm.extension_resource_uid = er.uid
LEFT JOIN extension_resource_inventory inv ON inv.extension_resource_uid = er.uid
`

func (r *ExtensionResourceRepo) Get(ctx context.Context, name domain.FullResourceName) (*domain.ExtensionResource, error) {
	rn := name.ResourceName()
	row := r.DB.QueryRowContext(ctx,
		erSelectColumns+erInstanceFromClause+`WHERE er.service_name = $1 AND er.collection_name = $2 AND er.resource_id = $3`,
		string(name.ServiceName()), string(rn.Collection()), string(rn.ID()))
	snap, err := scanExtensionResourceSnapshot(row)
	if err != nil {
		return nil, err
	}
	return domain.ExtensionResourceFromSnapshot(snap), nil
}

func (r *ExtensionResourceRepo) GetByUID(ctx context.Context, uid domain.ExtensionResourceUID) (*domain.ExtensionResource, error) {
	row := r.DB.QueryRowContext(ctx,
		erSelectColumns+erInstanceFromClause+`WHERE er.uid = $1`,
		uid.String())
	snap, err := scanExtensionResourceSnapshot(row)
	if err != nil {
		return nil, err
	}
	return domain.ExtensionResourceFromSnapshot(snap), nil
}

func (r *ExtensionResourceRepo) ListByResourceType(ctx context.Context, rt domain.ResourceType) ([]*domain.ExtensionResource, error) {
	rows, err := r.DB.QueryContext(ctx,
		erSelectColumns+erInstanceFromClause+`WHERE er.service_name = $1 AND er.type_name = $2 ORDER BY er.collection_name, er.resource_id`,
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
	rn := name.ResourceName()
	res, err := r.DB.ExecContext(ctx,
		`DELETE FROM extension_resources WHERE service_name = $1 AND collection_name = $2 AND resource_id = $3`,
		string(name.ServiceName()), string(rn.Collection()), string(rn.ID()))
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
	rn := name.ResourceName()
	q := erViewQueryPG + `
		WHERE er.service_name = $1 AND er.collection_name = $2 AND er.resource_id = $3`
	row := r.DB.QueryRowContext(ctx, q, string(name.ServiceName()), string(rn.Collection()), string(rn.ID()))
	return scanExtensionResourceView(row)
}

func (r *ExtensionResourceRepo) ListViewsByType(ctx context.Context, rt domain.ResourceType) ([]domain.ExtensionResourceView, error) {
	q := erViewQueryPG + `
		WHERE er.service_name = $1 AND er.type_name = $2 ORDER BY er.collection_name, er.resource_id`
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

// invLabelsSubqueryPG derives the latest inventory label set for an
// extension resource from the normalized
// extension_resource_inventory_labels table rather than a JSONB
// column, so batch label writes can be blind multi-row
// upserts/deletes against a real table (see [ExtensionResourceRepo.batchReplaceLabels]
// etc.) instead of a read-modify-write on a JSON blob. Returns SQL
// NULL (not '{}') when a resource has no labels, matching
// jsonb_object_agg's aggregate-over-empty-set behavior; scan helpers
// already treat a NULL/invalid labels column as an empty map.
const invLabelsSubqueryPG = `(SELECT jsonb_object_agg(l.key, l.value) FROM extension_resource_inventory_labels l WHERE l.extension_resource_uid = er.uid) AS inv_labels`

const erSelectColumns = `SELECT er.uid, er.service_name, er.type_name, er.collection_name, er.resource_id, er.labels, er.created_at, er.updated_at,
	erm.current_version, erm.fulfillment_id,
	` + invLabelsSubqueryPG + `,
	inv.observation, inv.observed_at, inv.updated_at,
	(SELECT jsonb_agg(jsonb_build_object(
		'type', c.type,
		'status', c.status,
		'reason', c.reason,
		'message', c.message,
		'last_transition_time', c.last_transition_time
	) ORDER BY c.type)
	 FROM extension_resource_inventory_conditions c
	 WHERE c.extension_resource_uid = er.uid) AS inv_conditions `

var erViewQueryPG = `SELECT
	er.uid, er.service_name, er.type_name, er.collection_name, er.resource_id, er.labels, er.created_at, er.updated_at,
	erm.current_version, erm.fulfillment_id,
	ri.spec, ri.created_at,
	` + fulfillmentColumnsJoined("f") + `,
	` + invLabelsSubqueryPG + `,
	inv.observation, inv.observed_at, inv.updated_at,
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
	var serviceName, typeName, collectionName, resourceID, labelsStr string
	var currentVersion sql.NullInt64
	var fulfillmentID sql.NullString
	var invLabels, invObservation sql.NullString
	var invObservedAt, invUpdatedAt *time.Time
	var invConditionsJSON sql.NullString

	if err := s.Scan(
		&snap.UID, &serviceName, &typeName, &collectionName, &resourceID, &labelsStr,
		&snap.CreatedAt, &snap.UpdatedAt,
		&currentVersion, &fulfillmentID,
		&invLabels, &invObservation, &invObservedAt, &invUpdatedAt,
		&invConditionsJSON,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return snap, domain.ErrNotFound
		}
		return snap, fmt.Errorf("scan extension resource: %w", err)
	}

	snap.ResourceType = domain.ResourceType(serviceName + "/" + typeName)
	snap.Name = domain.ResourceName(collectionName + "/" + resourceID)

	if err := json.Unmarshal([]byte(labelsStr), &snap.Labels); err != nil {
		return snap, fmt.Errorf("unmarshal labels: %w", err)
	}

	if fulfillmentID.Valid {
		snap.Managed = &domain.ManagedStateSnapshot{
			CurrentVersion: domain.IntentVersion(currentVersion.Int64),
			FulfillmentID:  domain.FulfillmentID(fulfillmentID.String),
		}
	}

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
		snap.Inventory = &invSnap
	}

	return snap, nil
}

func scanExtensionResourceView(s scanner) (domain.ExtensionResourceView, error) {
	var v domain.ExtensionResourceView

	var uid domain.ExtensionResourceUID
	var serviceName, typeName, collectionName, resourceID string
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
		&uid, &serviceName, &typeName, &collectionName, &resourceID, &labelsStr,
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
	name := domain.ResourceName(collectionName + "/" + resourceID)

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

	// Inventory: include in snapshot so ExtensionResourceFromSnapshot
	// hydrates Resource.Inventory().
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
		erSnap.Inventory = &invSnap
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
			LastTransitionTime: r.LastTransitionTime.UTC(),
		}
	}
	return snaps, nil
}

// ---------------------------------------------------------------------------
// Inventory methods
// ---------------------------------------------------------------------------
//
// ReplaceInventory and ApplyInventoryDeltas are each exactly one round
// trip for their *entire* input slice, regardless of batch size: a
// single CTE-chained statement (replaceInventorySQL/
// applyInventoryDeltasSQL below) that resolves-or-creates every
// replacement/delta's extension_resources row by natural key
// (service_name, collection_name, resource_id) -- the input_er/
// resolved_er/er CTEs -- and then has every other write join that
// resolution by natural key instead of depending on a UID resolved by
// the caller ahead of time. See the nameless-platform-identity plan's
// "Why natural-key correlation doesn't require a schema change"
// section for why one extra lookup CTE (er) is preferable to widening
// every inventory table's FK to the natural-key triple.
//
// Every distinct row-shape in the batch (1-per-report for input_er,
// 0..N-per-report flattened for labels/conditions/aliases) gets its
// own UNNEST-backed input CTE -- UNNEST can only produce one shape per
// call. Flattened rows carry the report's position (idx) rather than
// a UID, since the UID doesn't exist yet for a genuinely new resource;
// every later CTE joins its own input against er by idx to get the uid
// it needs for its own (unchanged, uid-keyed) insert.
//
// Where a step needs to know prior state to detect a genuine change
// (observation history, condition transitions, alias conflicts), a
// prev-style CTE captures that state as of the start of the statement
// and is compared against in the same statement that performs the
// write -- Postgres CTEs all see the same pre-statement snapshot
// regardless of execution order, so this removes the read here
// entirely rather than merely batching it.

// normalizeObservation collapses the two "no real observation" input
// shapes -- a nil pointer and a non-nil pointer to the JSON literal
// null -- to a single nil result, so the rest of the repository only
// has to handle one "untouched" case. Per the observation contract
// (see [domain.InventoryReplacement.Observation]), there is no
// explicit "clear" operation; only "untouched" and "replace".
func normalizeObservation(obs *json.RawMessage) *json.RawMessage {
	if obs == nil {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(*obs), []byte("null")) {
		return nil
	}
	return obs
}

// insertInventoryLatestRow inserts a brand-new latest-inventory row
// for a resource that has just been created and cannot yet have one,
// so unlike the merged ReplaceInventory/ApplyInventoryDeltas statement
// this is a plain INSERT: no conflict is possible, and (matching the
// prior behavior of this Create-only path) no observation history row
// is appended for the resource's initial observation.
func (r *ExtensionResourceRepo) insertInventoryLatestRow(ctx context.Context, uid domain.ExtensionResourceUID, observation *json.RawMessage, observedAt, updatedAt time.Time) error {
	var obsArg sql.NullString
	if observation != nil {
		obsArg = sql.NullString{String: string(*observation), Valid: true}
	}
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_inventory (extension_resource_uid, observation, observed_at, updated_at)
		 VALUES ($1, $2, $3, $4)`,
		uid.String(), obsArg, observedAt.UTC(), updatedAt.UTC())
	return err
}

// labelTriple is a flattened (resource, key, value) input row used by
// [ExtensionResourceRepo.insertInventory]'s single-resource label
// insert.
type labelTriple struct {
	uid   domain.ExtensionResourceUID
	key   string
	value string
}

// batchSetLabels guarded-upserts the flattened union of every input
// resource's labels in one round trip. Used only by
// [ExtensionResourceRepo.insertInventory] (a resource's initial
// labels, immediately after its own Create) -- the batch report path
// folds label writes into the merged inventory statement instead (see
// replaceInventorySQL/applyInventoryDeltasSQL).
func (r *ExtensionResourceRepo) batchSetLabels(ctx context.Context, labels []labelTriple) error {
	if len(labels) == 0 {
		return nil
	}
	uids := make([]string, len(labels))
	keys := make([]string, len(labels))
	values := make([]string, len(labels))
	for i, l := range labels {
		uids[i] = l.uid.String()
		keys[i] = l.key
		values[i] = l.value
	}
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_inventory_labels (extension_resource_uid, key, value)
		 SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[])
		 ON CONFLICT (extension_resource_uid, key) DO UPDATE SET value = EXCLUDED.value
		 WHERE extension_resource_inventory_labels.value IS DISTINCT FROM EXCLUDED.value`,
		uids, keys, values)
	if err != nil {
		return fmt.Errorf("batch set inventory labels: %w", err)
	}
	return nil
}

// conditionInput is the flattened per-(resource, condition) input row
// for [ExtensionResourceRepo.batchRecordConditions].
type conditionInput struct {
	uid                   domain.ExtensionResourceUID
	condType              domain.ConditionType
	status                domain.ConditionStatus
	reason, message       string
	lastTransitionTime    time.Time
	observedAt, updatedAt time.Time
}

// batchRecordConditions is the batched form of the single-row
// recordCondition path this replaced, applied to every
// (resource, condition) pair in the input in one round trip. Used
// only by [ExtensionResourceRepo.insertInventory] -- the batch report
// path folds condition writes into the merged inventory statement
// instead (see replaceInventorySQL/applyInventoryDeltasSQL).
func (r *ExtensionResourceRepo) batchRecordConditions(ctx context.Context, items []conditionInput) error {
	if len(items) == 0 {
		return nil
	}
	uids := make([]string, len(items))
	types := make([]string, len(items))
	statuses := make([]string, len(items))
	reasons := make([]string, len(items))
	messages := make([]string, len(items))
	lastTransitionTimes := make([]time.Time, len(items))
	observedAts := make([]time.Time, len(items))
	updatedAts := make([]time.Time, len(items))
	histIDs := make([]string, len(items))
	for i, it := range items {
		uids[i] = it.uid.String()
		types[i] = string(it.condType)
		statuses[i] = string(it.status)
		reasons[i] = it.reason
		messages[i] = it.message
		lastTransitionTimes[i] = it.lastTransitionTime.UTC()
		observedAts[i] = it.observedAt.UTC()
		updatedAts[i] = it.updatedAt.UTC()
		histIDs[i] = uuid.New().String()
	}
	_, err := r.DB.ExecContext(ctx,
		`WITH input(uid, type, status, reason, message, last_transition_time, observed_at, updated_at, hist_id) AS (
			SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::timestamptz[], $7::timestamptz[], $8::timestamptz[], $9::text[])
		),
		prev AS (
			SELECT c.extension_resource_uid, c.type, c.status, c.reason, c.message
			FROM extension_resource_inventory_conditions c
			WHERE (c.extension_resource_uid, c.type) IN (SELECT uid, type FROM input)
		),
		upserted AS (
			INSERT INTO extension_resource_inventory_conditions
				(extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, updated_at)
			SELECT uid, type, status, reason, message, last_transition_time, observed_at, updated_at FROM input
			ON CONFLICT (extension_resource_uid, type) DO UPDATE SET
				status = EXCLUDED.status,
				reason = EXCLUDED.reason,
				message = EXCLUDED.message,
				last_transition_time = EXCLUDED.last_transition_time,
				observed_at = EXCLUDED.observed_at,
				updated_at = EXCLUDED.updated_at
			RETURNING extension_resource_uid
		)
		INSERT INTO extension_resource_inventory_condition_events
			(id, extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, created_at)
		SELECT i.hist_id, i.uid, i.type, i.status, i.reason, i.message, i.last_transition_time, i.observed_at, i.updated_at
		FROM input i
		LEFT JOIN prev p ON p.extension_resource_uid = i.uid AND p.type = i.type
		WHERE p.extension_resource_uid IS NULL
		   OR p.status IS DISTINCT FROM i.status OR p.reason IS DISTINCT FROM i.reason OR p.message IS DISTINCT FROM i.message`,
		uids, types, statuses, reasons, messages, lastTransitionTimes, observedAts, updatedAts, histIDs)
	if err != nil {
		return fmt.Errorf("batch record conditions: %w", err)
	}
	return nil
}

// insertInventory writes a resource's initial inventory state as part
// of [ExtensionResourceRepo.Create]. There's nothing pre-existing to
// reconcile against (the resource itself was just created in the same
// call), so this reuses the guarded-upsert/upsert-conditions batch
// primitives with single-element inputs rather than the merged
// ReplaceInventory/ApplyInventoryDeltas statement's delete-absent path.
func (r *ExtensionResourceRepo) insertInventory(ctx context.Context, uid domain.ExtensionResourceUID, inv *domain.InventoryResourceSnapshot) error {
	var obs *json.RawMessage
	if inv.Observation != nil {
		obs = &inv.Observation
	}
	if err := r.insertInventoryLatestRow(ctx, uid, obs, inv.ObservedAt, inv.UpdatedAt); err != nil {
		return fmt.Errorf("insert latest inventory row: %w", err)
	}
	if len(inv.Labels) > 0 {
		triples := make([]labelTriple, 0, len(inv.Labels))
		for k, v := range inv.Labels {
			triples = append(triples, labelTriple{uid: uid, key: k, value: v})
		}
		if err := r.batchSetLabels(ctx, triples); err != nil {
			return fmt.Errorf("insert inventory labels: %w", err)
		}
	}
	if len(inv.Conditions) > 0 {
		items := make([]conditionInput, len(inv.Conditions))
		for i, c := range inv.Conditions {
			items[i] = conditionInput{
				uid: uid, condType: c.Type, status: c.Status, reason: c.Reason, message: c.Message,
				lastTransitionTime: c.LastTransitionTime, observedAt: inv.ObservedAt, updatedAt: inv.UpdatedAt,
			}
		}
		if err := r.batchRecordConditions(ctx, items); err != nil {
			return fmt.Errorf("insert inventory conditions: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Alias fold-in: shared Go-side helpers for ReplaceInventory/ApplyInventoryDeltas
// ---------------------------------------------------------------------------

// aliasCandidateInput is a flattened (report position, resolved
// target, alias, received-at) input row for the merged inventory
// statement's alias fold-in. receivedAt duplicates the same report's
// input_er.received_at value rather than being looked up from er by
// idx -- see flattenAliasCandidates's doc comment for why.
type aliasCandidateInput struct {
	idx        int
	target     domain.ResourceName
	alias      domain.Alias
	receivedAt time.Time
}

// checkAliasBatchConsistency rejects, in pure Go and with zero SQL,
// aliases that contradict each other purely within this one batch --
// the same two invariants resource_aliases' pair of unique indexes
// enforce against *pre-existing* rows (see the migration's doc
// comment): the same (namespace, key, value) claimed by two different
// targets, or the same target given two different values for the same
// (namespace, key). Neither shape can be left for alias_upsert's own
// ON CONFLICT to sort out: the first shape, reached twice within one
// INSERT's row source, hits Postgres's "ON CONFLICT DO UPDATE command
// cannot affect row a second time" restriction (a hard error, not a
// graceful skip) instead of Postgres's conflict resolution just
// picking a winner the way it would across two separate statements;
// the second shape would abort the *entire* statement with a generic
// constraint-violation error on the second unique index, since
// alias_upsert's ON CONFLICT only names the first one. Catching both
// shapes here, before any array is built, keeps the failure mode
// graceful. See the nameless-platform-identity plan's "Why aliases
// split into two paths, not two write statements" section --
// validateAliasUpsertsConsistent's old role, now scoped to exactly the
// case a unique index can't cover. (alias_upsert's SELECT DISTINCT
// additionally guards against a literal duplicate -- same target,
// same alias, reported twice in one batch -- which isn't a
// contradiction and so isn't rejected here, but would otherwise hit
// that same "affect row twice" restriction.)
func checkAliasBatchConsistency(candidates []aliasCandidateInput) (safe []aliasCandidateInput, conflicts []domain.AliasConflict) {
	type valueKey struct {
		ns    domain.AliasNamespace
		key   domain.AliasKey
		value domain.AliasValue
	}
	type resourceKey struct {
		ns     domain.AliasNamespace
		key    domain.AliasKey
		target domain.ResourceName
	}

	ownerByValue := make(map[valueKey]domain.ResourceName, len(candidates))
	valueByResource := make(map[resourceKey]domain.AliasValue, len(candidates))

	for _, c := range candidates {
		vk := valueKey{c.alias.Namespace, c.alias.Key, c.alias.Value}
		rk := resourceKey{c.alias.Namespace, c.alias.Key, c.target}

		if owner, ok := ownerByValue[vk]; ok && owner != c.target {
			conflicts = append(conflicts, domain.AliasConflict{
				Alias:      c.alias,
				Kind:       domain.AliasConflictValueClaimedByOther,
				TargetName: c.target,
				ActualName: owner,
			})
			continue
		}
		if val, ok := valueByResource[rk]; ok && val != c.alias.Value {
			conflicts = append(conflicts, domain.AliasConflict{
				Alias:       c.alias,
				Kind:        domain.AliasConflictResourceHasDifferentValue,
				TargetName:  c.target,
				ActualValue: val,
			})
			continue
		}

		ownerByValue[vk] = c.target
		valueByResource[rk] = c.alias.Value
		safe = append(safe, c)
	}
	return safe, conflicts
}

// flattenAliasCandidates converts a slice already vetted by
// [checkAliasBatchConsistency] into the parallel arrays
// replaceInventorySQL/applyInventoryDeltasSQL's input_aliases CTE
// expects. collection_name/resource_id/received_at ride along here
// even though they're per-report, not per-alias, values: every one of
// them is already sitting in Go memory (the same rep.Name/d.Name and
// rep.ReceivedAt/d.ReceivedAt that populated input_er's own arrays for
// this idx), so passing them again here is cheaper than making
// input_aliases join er by idx to fetch values er itself only ever
// passed through from input_er unchanged. An EXPLAIN of the version
// that did join er (see inventory_bench_test.go's UpdateWithAlias
// benchmark) showed that join costing ~8ms of a ~70ms statement at
// batch=1000 -- and paying it twice, once for each of
// alias_prev_by_resource/alias_resource_check's references, since a
// plain scan of input_aliases's UNNEST is cheap enough that Postgres
// no longer has any real materialization cost to amortize across
// those two references.
func flattenAliasCandidates(candidates []aliasCandidateInput) (idx []int32, namespaces, keys, values, collectionNames, resourceIDs []string, receivedAts []time.Time) {
	idx = make([]int32, len(candidates))
	namespaces = make([]string, len(candidates))
	keys = make([]string, len(candidates))
	values = make([]string, len(candidates))
	collectionNames = make([]string, len(candidates))
	resourceIDs = make([]string, len(candidates))
	receivedAts = make([]time.Time, len(candidates))
	for i, c := range candidates {
		idx[i] = int32(c.idx)
		namespaces[i] = string(c.alias.Namespace)
		keys[i] = string(c.alias.Key)
		values[i] = string(c.alias.Value)
		collectionNames[i] = string(c.target.Collection())
		resourceIDs[i] = string(c.target.ID())
		receivedAts[i] = c.receivedAt
	}
	return idx, namespaces, keys, values, collectionNames, resourceIDs, receivedAts
}

// scanAliasConflicts converts the merged inventory statement's final
// result set -- alias candidates that lost against *pre-existing*
// resource_aliases state -- into [domain.AliasConflict]s. targetOf
// maps a flattened input row's idx back to its report's resolved
// target name.
func scanAliasConflicts(rows *sql.Rows, targetOf func(idx int) domain.ResourceName) ([]domain.AliasConflict, error) {
	var conflicts []domain.AliasConflict
	for rows.Next() {
		var i int32
		var namespace, key, value string
		var actualCollectionName, actualResourceID, actualValue sql.NullString
		if err := rows.Scan(&i, &namespace, &key, &value, &actualCollectionName, &actualResourceID, &actualValue); err != nil {
			return nil, fmt.Errorf("scan alias conflict: %w", err)
		}
		alias := domain.Alias{Namespace: domain.AliasNamespace(namespace), Key: domain.AliasKey(key), Value: domain.AliasValue(value)}
		conflict := domain.AliasConflict{Alias: alias, TargetName: targetOf(int(i))}
		if actualCollectionName.Valid {
			conflict.Kind = domain.AliasConflictValueClaimedByOther
			conflict.ActualName = domain.ResourceName(actualCollectionName.String + "/" + actualResourceID.String)
		} else {
			conflict.Kind = domain.AliasConflictResourceHasDifferentValue
			conflict.ActualValue = domain.AliasValue(actualValue.String)
		}
		conflicts = append(conflicts, conflict)
	}
	return conflicts, rows.Err()
}

// aliasFoldCTEs is the alias fold-in shared verbatim by
// replaceInventorySQL and applyInventoryDeltasSQL: it must appear
// after a statement's input_aliases CTE and before its final SELECT.
// Unlike the rest of the merged inventory statement, nothing here
// depends on er/resolved_er at all -- see input_aliases's own doc
// comment (next to replaceInventorySQLWithAliases/
// applyInventoryDeltasSQLWithAliases) for why aliases correlate to
// platform_resources/resource_aliases by natural key alone, with no
// need for the extension resource's own uid.
//
// resource_aliases has two unique indexes (see the migration's doc
// comment): (namespace, key, value) and (namespace, key,
// platform_collection_name, platform_resource_id). checkAliasBatchConsistency
// has already ruled out an intra-batch collision against either, so
// every remaining conflict source is a pre-existing resource_aliases
// row. This fold-in classifies every candidate against both indexes,
// by reading, before writing anything -- in an order chosen around
// which outcome is common: most reports re-send the same alias they
// sent last time, and classify-before-write turns that into a single
// read with no write at all, rather than a write whose own conflict
// machinery discovers afterward that it was a no-op.
//
// Phase 1 (alias_prev_by_value) reads by (namespace, key, value) --
// the *reported* value -- for every candidate, answering "does this
// exact alias already exist, and for whom" in one pass. That splits
// candidates three ways: alias_value_conflicts (the value exists, for
// a *different* target -- AliasConflictValueClaimedByOther, resolved
// without ever touching ensure_platform, alias_upsert, or the second
// index); a true no-op (the value exists for *this* target already --
// absent from every CTE from here on, never reaching a write); and
// alias_value_missing (the value doesn't exist anywhere yet), the
// only group phase 2 needs to look at.
//
// Phase 2 (alias_resource_check) reads by (namespace, key,
// platform_collection_name, platform_resource_id) -- this target's
// own row for the key -- but only for alias_value_missing candidates.
// Since phase 1 already proved this exact value doesn't exist
// anywhere, a match here can only be a *different* value already
// registered for this target/key: the other index's conflict shape,
// AliasConflictResourceHasDifferentValue (alias_resource_conflicts).
// No match at all (alias_safe) means the candidate is genuinely new.
//
// Both phases fold their existence check directly into a LEFT JOIN
// LATERAL against the candidate relation (alias_prev_by_value against
// input_aliases, alias_resource_check against alias_value_missing)
// rather than computing the check as its own CTE and re-joining it
// back afterward by idx. That's deliberate, not stylistic: idx
// identifies a *report*, not a single alias -- flattenAliasCandidates
// emits one input_aliases row per (report, alias) pair, so a report
// with two aliases produces two rows sharing one idx. Re-joining two
// already-derived relations "ON x.idx = y.idx" would fan those two
// rows out against each other's results whenever both happen to
// match, silently mixing up which lookup belongs to which alias.
// Folding the check into the same row via LATERAL keeps every derived
// CTE 1:1 with its input, so no re-join -- and no idx collision -- is
// ever needed. (alias_leftover_owner, further down, exists for the
// same reason.)
//
// Only alias_safe (alias_value_missing rows phase 2 found no existing
// row for) ever reaches ensure_platform or alias_upsert: lazily
// upserting the (uid-less) platform_resources row for every name in
// alias_safe -- see resource_identity_repo.go's virtual-resource doc
// -- then the alias itself. alias_upsert still needs its own guarded
// DO UPDATE and post-write diagnosis (alias_leftover,
// alias_leftover_owner) despite phase 1 already having proved this
// value didn't exist at read time: a concurrent session could insert
// the same (namespace, key, value) between that read and this write.
// The guard -- a no-op write-back of the row's own current owner,
// distinguishing "raced but same owner" from "raced, different owner"
// via RETURNING alone, with no read before the INSERT runs --
// together with the leftover-diagnosis tail now exists purely to
// handle that (rare) race, not the common case anymore.
//
// Every LATERAL here uses OFFSET 0, which is load-bearing, not
// decorative: without it, Postgres "de-correlates" a LATERAL whose
// body is just an equality filter back into an ordinary join it's
// once again free to hash-and-scan, defeating the point -- OFFSET 0
// is the standard trick for telling the planner a subquery must
// actually be evaluated as written, one outer row at a time, which is
// what pushes it to a per-candidate indexed nested loop instead of a
// seq-scan-and-hash-join (its row-count guess for a CTE is a flat,
// disconnected-from-real-stats default that otherwise steers it the
// other way at this corpus size).
//
// (An earlier version routed input_aliases through an alias_candidates
// CTE that joined er by idx purely to fetch collection_name/
// resource_id/received_at -- values er itself only ever passed
// through from input_er unchanged, and so already sitting in Go
// memory at the exact call site that builds input_aliases's arrays.
// An EXPLAIN of that version (see inventory_bench_test.go's
// UpdateWithAlias benchmark) showed alias_candidates costing as much
// to scan a second time, in what's now alias_resource_check, as it
// cost to compute in the first place, even though it's a plain
// materialized CTE and Postgres's docs promise a WITH query "is
// evaluated only once ... even if referred to more than once" --
// empirically, on this corpus size, joining er was expensive enough
// that even a single extra reference wasn't free. Passing those three
// columns as part of input_aliases itself, per flattenAliasCandidates's
// doc comment, removed the join -- and the CTE -- entirely rather
// than trying to amortize its cost. A later EXPLAIN of the version
// that came before this one -- a single read-by-resource-first phase,
// with no equivalent of phase 1 above -- showed alias_upsert's
// guarded DO UPDATE doing a real per-row tuple rewrite and index
// update for every steady-state re-report even though nothing had
// changed, accounting for roughly a fifth of the whole statement's
// time on its own. Reading by value first, as phases 1 and 2 above
// now do, is what lets that common case skip the write -- both
// ensure_platform and alias_upsert -- rather than just detecting
// after the fact that it didn't need one.)
const aliasFoldCTEs = `
alias_prev_by_value AS (
	SELECT ac.idx, ac.namespace, ac.key, ac.value, ac.collection_name, ac.resource_id, ac.received_at,
	       ra.actual_collection_name, ra.actual_resource_id
	FROM input_aliases ac
	LEFT JOIN LATERAL (
		SELECT platform_collection_name AS actual_collection_name, platform_resource_id AS actual_resource_id
		FROM resource_aliases
		WHERE namespace = ac.namespace AND key = ac.key AND value = ac.value
		OFFSET 0
	) ra ON true
),
alias_value_conflicts AS (
	SELECT idx, namespace, key, value, actual_collection_name, actual_resource_id
	FROM alias_prev_by_value
	WHERE actual_collection_name IS NOT NULL
	  AND (actual_collection_name <> collection_name OR actual_resource_id <> resource_id)
),
alias_value_missing AS (
	SELECT idx, namespace, key, value, collection_name, resource_id, received_at
	FROM alias_prev_by_value
	WHERE actual_collection_name IS NULL
),
alias_resource_check AS (
	SELECT ac.idx, ac.namespace, ac.key, ac.value, ac.collection_name, ac.resource_id, ac.received_at, ra.existing_value
	FROM alias_value_missing ac
	LEFT JOIN LATERAL (
		SELECT value AS existing_value FROM resource_aliases
		WHERE namespace = ac.namespace AND key = ac.key
		  AND platform_collection_name = ac.collection_name AND platform_resource_id = ac.resource_id
		OFFSET 0
	) ra ON true
),
alias_safe AS (
	SELECT idx, namespace, key, value, collection_name, resource_id, received_at
	FROM alias_resource_check
	WHERE existing_value IS NULL
),
alias_resource_conflicts AS (
	SELECT idx, namespace, key, value, existing_value
	FROM alias_resource_check
	WHERE existing_value IS NOT NULL
),
ensure_platform AS (
	INSERT INTO platform_resources (collection_name, resource_id, labels, created_at, updated_at)
	SELECT DISTINCT collection_name, resource_id, '{}'::jsonb, received_at, received_at
	FROM alias_safe
	ON CONFLICT (collection_name, resource_id) DO NOTHING
	RETURNING 1
),
alias_upsert AS (
	INSERT INTO resource_aliases (namespace, key, value, platform_collection_name, platform_resource_id, created_at)
	SELECT DISTINCT namespace, key, value, collection_name, resource_id, received_at
	FROM alias_safe
	ON CONFLICT (namespace, key, value) DO UPDATE SET
		platform_collection_name = EXCLUDED.platform_collection_name,
		platform_resource_id = EXCLUDED.platform_resource_id
	WHERE resource_aliases.platform_collection_name = EXCLUDED.platform_collection_name
	  AND resource_aliases.platform_resource_id = EXCLUDED.platform_resource_id
	RETURNING namespace, key, value
),
alias_leftover AS (
	SELECT ac.*
	FROM alias_safe ac
	LEFT JOIN alias_upsert au ON au.namespace = ac.namespace AND au.key = ac.key AND au.value = ac.value
	WHERE au.value IS NULL
),
alias_leftover_owner AS (
	SELECT ac.idx, ac.namespace, ac.key, ac.value, ra.actual_collection_name, ra.actual_resource_id
	FROM alias_leftover ac
	JOIN LATERAL (
		SELECT platform_collection_name AS actual_collection_name, platform_resource_id AS actual_resource_id
		FROM resource_aliases
		WHERE namespace = ac.namespace AND key = ac.key AND value = ac.value
		OFFSET 0
	) ra ON true
)`

// aliasConflictSelect is the final SELECT shared verbatim by
// replaceInventorySQL and applyInventoryDeltasSQL, appended after
// aliasFoldCTEs. Three branches, one per conflict source:
// alias_value_conflicts (phase 1, read-by-value) and
// alias_leftover_owner (discovered only after alias_upsert's guarded
// write actually raced) are both AliasConflictValueClaimedByOther;
// alias_resource_conflicts (phase 2, read-by-resource) is
// AliasConflictResourceHasDifferentValue. A candidate matching none
// of the three (the common case) is absent from the result entirely.
const aliasConflictSelect = `
SELECT idx, namespace, key, value,
       actual_collection_name, actual_resource_id, NULL::text AS actual_value
FROM alias_value_conflicts
UNION ALL
SELECT idx, namespace, key, value,
       NULL::text AS actual_collection_name, NULL::text AS actual_resource_id, existing_value AS actual_value
FROM alias_resource_conflicts
UNION ALL
SELECT idx, namespace, key, value,
       actual_collection_name, actual_resource_id, NULL::text AS actual_value
FROM alias_leftover_owner`

// replaceInventoryCoreCTEs is the natural-key resolve-or-create +
// latest-row + label + condition pipeline shared by
// replaceInventorySQLWithAliases and replaceInventorySQLNoAliases --
// everything ReplaceInventory needs regardless of whether this
// chunk's replacements carry any aliases. input_er/resolved_er/er
// resolve-or-create every replacement's extension_resources row by
// natural key (see this section's doc comment); upsert_inv/hist_obs
// write the latest row and its change-guarded observation history;
// del_labels_absent/upsert_labels implement "Labels is the complete
// latest label set"; del_conditions_absent/upsert_conditions/
// hist_conditions implement the same for conditions plus
// change-guarded transition history. Deliberately has no trailing
// comma after hist_conditions's closing paren -- each of the two
// callers below supplies its own continuation (either straight into
// aliasFoldCTEs, or straight into a no-op terminal SELECT).
// Placeholder count is fixed (20) regardless of chunk size -- only
// the array arguments grow.
const replaceInventoryCoreCTEs = `
WITH input_er(idx, service_name, type_name, collection_name, resource_id, candidate_uid, observation, observed_at, received_at, obs_hist_id) AS (
	SELECT * FROM UNNEST($1::int[], $2::text[], $3::text[], $4::text[], $5::text[], $6::uuid[], $7::jsonb[], $8::timestamptz[], $9::timestamptz[], $10::text[])
),
input_labels(idx, key, value) AS (
	SELECT * FROM UNNEST($11::int[], $12::text[], $13::text[])
),
input_conditions(idx, type, status, reason, message, last_transition_time, hist_id) AS (
	SELECT * FROM UNNEST($14::int[], $15::text[], $16::text[], $17::text[], $18::text[], $19::timestamptz[], $20::text[])
),
resolved_er AS (
	INSERT INTO extension_resources (uid, service_name, type_name, collection_name, resource_id, labels, created_at, updated_at)
	SELECT candidate_uid, service_name, type_name, collection_name, resource_id, '{}'::jsonb, received_at, received_at
	FROM input_er
	ON CONFLICT (service_name, collection_name, resource_id) DO NOTHING
	RETURNING uid, service_name, collection_name, resource_id
),
er AS (
	SELECT i.idx, i.collection_name, i.resource_id, i.observation, i.observed_at, i.received_at, i.obs_hist_id,
	       COALESCE(res.uid, ext.uid) AS uid
	FROM input_er i
	LEFT JOIN resolved_er res
		ON res.service_name = i.service_name AND res.collection_name = i.collection_name AND res.resource_id = i.resource_id
	LEFT JOIN extension_resources ext
		ON ext.service_name = i.service_name AND ext.collection_name = i.collection_name AND ext.resource_id = i.resource_id
),
prev_obs AS (
	SELECT extension_resource_uid, observation
	FROM extension_resource_inventory
	WHERE extension_resource_uid IN (SELECT uid FROM er)
),
upsert_inv AS (
	INSERT INTO extension_resource_inventory (extension_resource_uid, observation, observed_at, updated_at)
	SELECT uid, observation, observed_at, received_at FROM er
	ON CONFLICT (extension_resource_uid) DO UPDATE SET
		observation = COALESCE(EXCLUDED.observation, extension_resource_inventory.observation),
		observed_at = EXCLUDED.observed_at,
		updated_at = EXCLUDED.updated_at
	RETURNING extension_resource_uid
),
hist_obs AS (
	INSERT INTO extension_resource_inventory_observations (id, extension_resource_uid, observation, observed_at, created_at)
	SELECT e.obs_hist_id, e.uid, e.observation, e.observed_at, e.received_at
	FROM er e
	LEFT JOIN prev_obs p ON p.extension_resource_uid = e.uid
	WHERE e.observation IS NOT NULL
	  AND (p.extension_resource_uid IS NULL OR p.observation IS DISTINCT FROM e.observation)
	RETURNING 1
),
del_labels_absent AS (
	DELETE FROM extension_resource_inventory_labels l
	USING er e
	WHERE l.extension_resource_uid = e.uid
	  AND NOT EXISTS (SELECT 1 FROM input_labels il WHERE il.idx = e.idx AND il.key = l.key)
	RETURNING 1
),
upsert_labels AS (
	INSERT INTO extension_resource_inventory_labels (extension_resource_uid, key, value)
	SELECT e.uid, il.key, il.value
	FROM input_labels il
	JOIN er e ON e.idx = il.idx
	ON CONFLICT (extension_resource_uid, key) DO UPDATE SET value = EXCLUDED.value
	WHERE extension_resource_inventory_labels.value IS DISTINCT FROM EXCLUDED.value
	RETURNING 1
),
del_conditions_absent AS (
	DELETE FROM extension_resource_inventory_conditions c
	USING er e
	WHERE c.extension_resource_uid = e.uid
	  AND NOT EXISTS (SELECT 1 FROM input_conditions ic WHERE ic.idx = e.idx AND ic.type = c.type)
	RETURNING 1
),
prev_cond AS (
	SELECT c.extension_resource_uid, c.type, c.status, c.reason, c.message
	FROM extension_resource_inventory_conditions c
	WHERE c.extension_resource_uid IN (SELECT uid FROM er)
),
upsert_conditions AS (
	INSERT INTO extension_resource_inventory_conditions
		(extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, updated_at)
	SELECT e.uid, ic.type, ic.status, ic.reason, ic.message, ic.last_transition_time, e.observed_at, e.received_at
	FROM input_conditions ic
	JOIN er e ON e.idx = ic.idx
	ON CONFLICT (extension_resource_uid, type) DO UPDATE SET
		status = EXCLUDED.status, reason = EXCLUDED.reason, message = EXCLUDED.message,
		last_transition_time = EXCLUDED.last_transition_time, observed_at = EXCLUDED.observed_at, updated_at = EXCLUDED.updated_at
	RETURNING extension_resource_uid
),
hist_conditions AS (
	INSERT INTO extension_resource_inventory_condition_events
		(id, extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, created_at)
	SELECT ic.hist_id, e.uid, ic.type, ic.status, ic.reason, ic.message, ic.last_transition_time, e.observed_at, e.received_at
	FROM input_conditions ic
	JOIN er e ON e.idx = ic.idx
	LEFT JOIN prev_cond p ON p.extension_resource_uid = e.uid AND p.type = ic.type
	WHERE p.extension_resource_uid IS NULL
	   OR p.status IS DISTINCT FROM ic.status OR p.reason IS DISTINCT FROM ic.reason OR p.message IS DISTINCT FROM ic.message
	RETURNING 1
)`

// replaceInventorySQLWithAliases is ReplaceInventory's single
// statement for a chunk containing at least one alias to fold in:
// replaceInventoryCoreCTEs, plus an input_aliases CTE (placeholders
// $21-$27) feeding aliasFoldCTEs/aliasConflictSelect. input_aliases
// carries collection_name/resource_id/received_at alongside each
// alias despite those being per-report rather than per-alias values --
// see flattenAliasCandidates's doc comment for why duplicating them
// here, straight from the same Go-side values input_er's own arrays
// use, is cheaper than making aliasFoldCTEs join er by idx to fetch
// them.
const replaceInventorySQLWithAliases = replaceInventoryCoreCTEs + `,
input_aliases(idx, namespace, key, value, collection_name, resource_id, received_at) AS (
	SELECT * FROM UNNEST($21::int[], $22::text[], $23::text[], $24::text[], $25::text[], $26::text[], $27::timestamptz[])
),
` + aliasFoldCTEs + aliasConflictSelect

// replaceInventorySQLNoAliases is ReplaceInventory's single statement
// for a chunk with no aliases left to fold in (either because none of
// its reports supplied any, or because every candidate was already
// rejected in Go by checkAliasBatchConsistency) -- just
// replaceInventoryCoreCTEs, skipping input_aliases/aliasFoldCTEs/
// aliasConflictSelect entirely. This is the overwhelming common case
// (most resource types report no aliases at all), so avoiding five
// CTEs' worth of joins and two conditional inserts against an input
// that would otherwise always be empty is worth a second statement
// variant. The trailing SELECT is a required terminal statement for
// the WITH clause -- ReplaceInventory never scans it -- and is safe
// to leave disconnected from the CTEs above it: per the Postgres
// manual's "Data-Modifying Statements in WITH" section, such
// statements "are executed exactly once, and always to completion,
// independently of whether the primary query reads all (or indeed
// any) of their output," so resolved_er/upsert_inv/hist_obs/etc. all
// still run even though nothing here selects from them.
const replaceInventorySQLNoAliases = replaceInventoryCoreCTEs + `
SELECT 1 WHERE FALSE`

// ReplaceInventory implements [domain.ExtensionResourceRepository.ReplaceInventory]
// as one round trip for the whole chunk: replaceInventorySQLWithAliases
// or replaceInventorySQLNoAliases, depending on whether this chunk has
// any aliases left to fold in after checkAliasBatchConsistency. See
// this section's doc comment for what those statements do and why.
func (r *ExtensionResourceRepo) ReplaceInventory(ctx context.Context, replacements []domain.InventoryReplacement) ([]domain.AliasConflict, error) {
	if len(replacements) == 0 {
		return nil, nil
	}

	n := len(replacements)
	idx := make([]int32, n)
	serviceNames := make([]string, n)
	typeNames := make([]string, n)
	collectionNames := make([]string, n)
	resourceIDs := make([]string, n)
	candidateUIDs := make([]string, n)
	observations := make([]*string, n)
	observedAts := make([]time.Time, n)
	receivedAts := make([]time.Time, n)
	obsHistIDs := make([]string, n)

	var labelIdx []int32
	var labelKeys, labelValues []string
	var condIdx []int32
	var condTypes, condStatuses, condReasons, condMessages []string
	var condLastTransitions []time.Time
	var condHistIDs []string
	var aliasCandidates []aliasCandidateInput

	for i, rep := range replacements {
		idx[i] = int32(i)
		serviceNames[i] = string(rep.ResourceType.ServiceName())
		typeNames[i] = rep.ResourceType.TypeName()
		collectionNames[i] = string(rep.Name.Collection())
		resourceIDs[i] = string(rep.Name.ID())
		candidateUIDs[i] = rep.CandidateUID.String()
		if obs := normalizeObservation(rep.Observation); obs != nil {
			s := string(*obs)
			observations[i] = &s
		}
		observedAts[i] = rep.ObservedAt.UTC()
		receivedAts[i] = rep.ReceivedAt.UTC()
		obsHistIDs[i] = string(domain.NewObservationID())

		for k, v := range rep.Labels {
			labelIdx = append(labelIdx, int32(i))
			labelKeys = append(labelKeys, k)
			labelValues = append(labelValues, v)
		}
		for _, c := range rep.Conditions {
			condIdx = append(condIdx, int32(i))
			condTypes = append(condTypes, string(c.Type()))
			condStatuses = append(condStatuses, string(c.Status()))
			condReasons = append(condReasons, c.Reason())
			condMessages = append(condMessages, c.Message())
			condLastTransitions = append(condLastTransitions, c.LastTransitionTime().UTC())
			condHistIDs = append(condHistIDs, uuid.New().String())
		}
		for _, a := range rep.Aliases {
			aliasCandidates = append(aliasCandidates, aliasCandidateInput{idx: i, target: rep.Name, alias: a, receivedAt: receivedAts[i]})
		}
	}

	safeAliases, conflicts := checkAliasBatchConsistency(aliasCandidates)

	var rows *sql.Rows
	var err error
	if len(safeAliases) > 0 {
		aliasIdx, aliasNamespaces, aliasKeys, aliasValues, aliasCollectionNames, aliasResourceIDs, aliasReceivedAts := flattenAliasCandidates(safeAliases)
		rows, err = r.DB.QueryContext(ctx, replaceInventorySQLWithAliases,
			idx, serviceNames, typeNames, collectionNames, resourceIDs, candidateUIDs, observations, observedAts, receivedAts, obsHistIDs,
			labelIdx, labelKeys, labelValues,
			condIdx, condTypes, condStatuses, condReasons, condMessages, condLastTransitions, condHistIDs,
			aliasIdx, aliasNamespaces, aliasKeys, aliasValues, aliasCollectionNames, aliasResourceIDs, aliasReceivedAts,
		)
	} else {
		rows, err = r.DB.QueryContext(ctx, replaceInventorySQLNoAliases,
			idx, serviceNames, typeNames, collectionNames, resourceIDs, candidateUIDs, observations, observedAts, receivedAts, obsHistIDs,
			labelIdx, labelKeys, labelValues,
			condIdx, condTypes, condStatuses, condReasons, condMessages, condLastTransitions, condHistIDs,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("replace inventory: %w", err)
	}
	defer rows.Close()

	// replaceInventorySQLNoAliases's terminal SELECT always returns
	// zero rows, so scanAliasConflicts's loop body (and thus its
	// column count assumptions) never runs -- safe to call
	// unconditionally regardless of which statement variant ran.
	dbConflicts, err := scanAliasConflicts(rows, func(i int) domain.ResourceName { return replacements[i].Name })
	if err != nil {
		return nil, fmt.Errorf("replace inventory: %w", err)
	}
	return append(conflicts, dbConflicts...), nil
}

// applyInventoryDeltasCoreCTEs is the delta counterpart of
// replaceInventoryCoreCTEs, shared by applyInventoryDeltasSQLWithAliases
// and applyInventoryDeltasSQLNoAliases -- the same shape, except
// labels/conditions are field-level set/delete pairs instead of a
// delete-absent/upsert pair, matching [domain.InventoryDelta]'s
// incremental semantics. upsert_inv still runs for every delta
// (including one with no observation change at all), doubling as
// "ensure a latest inventory row exists" for every uid in the batch --
// see [domain.ExtensionResourceRepository.ApplyInventoryDeltas]'s doc.
// No trailing comma after del_conditions's closing paren -- see
// replaceInventoryCoreCTEs's doc comment for why. Placeholder count is
// fixed (24) regardless of chunk size -- only the array arguments
// grow.
const applyInventoryDeltasCoreCTEs = `
WITH input_er(idx, service_name, type_name, collection_name, resource_id, candidate_uid, observation, observed_at, received_at, obs_hist_id) AS (
	SELECT * FROM UNNEST($1::int[], $2::text[], $3::text[], $4::text[], $5::text[], $6::uuid[], $7::jsonb[], $8::timestamptz[], $9::timestamptz[], $10::text[])
),
input_set_labels(idx, key, value) AS (
	SELECT * FROM UNNEST($11::int[], $12::text[], $13::text[])
),
input_delete_labels(idx, key) AS (
	SELECT * FROM UNNEST($14::int[], $15::text[])
),
input_upsert_conditions(idx, type, status, reason, message, last_transition_time, hist_id) AS (
	SELECT * FROM UNNEST($16::int[], $17::text[], $18::text[], $19::text[], $20::text[], $21::timestamptz[], $22::text[])
),
input_delete_conditions(idx, type) AS (
	SELECT * FROM UNNEST($23::int[], $24::text[])
),
resolved_er AS (
	INSERT INTO extension_resources (uid, service_name, type_name, collection_name, resource_id, labels, created_at, updated_at)
	SELECT candidate_uid, service_name, type_name, collection_name, resource_id, '{}'::jsonb, received_at, received_at
	FROM input_er
	ON CONFLICT (service_name, collection_name, resource_id) DO NOTHING
	RETURNING uid, service_name, collection_name, resource_id
),
er AS (
	SELECT i.idx, i.collection_name, i.resource_id, i.observation, i.observed_at, i.received_at, i.obs_hist_id,
	       COALESCE(res.uid, ext.uid) AS uid
	FROM input_er i
	LEFT JOIN resolved_er res
		ON res.service_name = i.service_name AND res.collection_name = i.collection_name AND res.resource_id = i.resource_id
	LEFT JOIN extension_resources ext
		ON ext.service_name = i.service_name AND ext.collection_name = i.collection_name AND ext.resource_id = i.resource_id
),
prev_obs AS (
	SELECT extension_resource_uid, observation
	FROM extension_resource_inventory
	WHERE extension_resource_uid IN (SELECT uid FROM er)
),
upsert_inv AS (
	INSERT INTO extension_resource_inventory (extension_resource_uid, observation, observed_at, updated_at)
	SELECT uid, observation, observed_at, received_at FROM er
	ON CONFLICT (extension_resource_uid) DO UPDATE SET
		observation = COALESCE(EXCLUDED.observation, extension_resource_inventory.observation),
		observed_at = EXCLUDED.observed_at,
		updated_at = EXCLUDED.updated_at
	RETURNING extension_resource_uid
),
hist_obs AS (
	INSERT INTO extension_resource_inventory_observations (id, extension_resource_uid, observation, observed_at, created_at)
	SELECT e.obs_hist_id, e.uid, e.observation, e.observed_at, e.received_at
	FROM er e
	LEFT JOIN prev_obs p ON p.extension_resource_uid = e.uid
	WHERE e.observation IS NOT NULL
	  AND (p.extension_resource_uid IS NULL OR p.observation IS DISTINCT FROM e.observation)
	RETURNING 1
),
upsert_labels AS (
	INSERT INTO extension_resource_inventory_labels (extension_resource_uid, key, value)
	SELECT e.uid, il.key, il.value
	FROM input_set_labels il
	JOIN er e ON e.idx = il.idx
	ON CONFLICT (extension_resource_uid, key) DO UPDATE SET value = EXCLUDED.value
	WHERE extension_resource_inventory_labels.value IS DISTINCT FROM EXCLUDED.value
	RETURNING 1
),
del_labels AS (
	DELETE FROM extension_resource_inventory_labels l
	USING input_delete_labels dl, er e
	WHERE dl.idx = e.idx AND l.extension_resource_uid = e.uid AND l.key = dl.key
	RETURNING 1
),
prev_cond AS (
	SELECT c.extension_resource_uid, c.type, c.status, c.reason, c.message
	FROM extension_resource_inventory_conditions c
	WHERE c.extension_resource_uid IN (SELECT uid FROM er)
),
upsert_conditions AS (
	INSERT INTO extension_resource_inventory_conditions
		(extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, updated_at)
	SELECT e.uid, ic.type, ic.status, ic.reason, ic.message, ic.last_transition_time, e.observed_at, e.received_at
	FROM input_upsert_conditions ic
	JOIN er e ON e.idx = ic.idx
	ON CONFLICT (extension_resource_uid, type) DO UPDATE SET
		status = EXCLUDED.status, reason = EXCLUDED.reason, message = EXCLUDED.message,
		last_transition_time = EXCLUDED.last_transition_time, observed_at = EXCLUDED.observed_at, updated_at = EXCLUDED.updated_at
	RETURNING extension_resource_uid
),
hist_conditions AS (
	INSERT INTO extension_resource_inventory_condition_events
		(id, extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, created_at)
	SELECT ic.hist_id, e.uid, ic.type, ic.status, ic.reason, ic.message, ic.last_transition_time, e.observed_at, e.received_at
	FROM input_upsert_conditions ic
	JOIN er e ON e.idx = ic.idx
	LEFT JOIN prev_cond p ON p.extension_resource_uid = e.uid AND p.type = ic.type
	WHERE p.extension_resource_uid IS NULL
	   OR p.status IS DISTINCT FROM ic.status OR p.reason IS DISTINCT FROM ic.reason OR p.message IS DISTINCT FROM ic.message
	RETURNING 1
),
del_conditions AS (
	DELETE FROM extension_resource_inventory_conditions c
	USING input_delete_conditions dc, er e
	WHERE dc.idx = e.idx AND c.extension_resource_uid = e.uid AND c.type = dc.type
	RETURNING 1
)`

// applyInventoryDeltasSQLWithAliases is ApplyInventoryDeltas's single
// statement for a chunk containing at least one alias to fold in --
// see replaceInventorySQLWithAliases's doc comment for the shape
// (including why input_aliases carries three report-level columns
// alongside each alias); placeholders $25-$31 here instead of $21-$27
// since applyInventoryDeltasCoreCTEs's input CTEs use four more
// placeholders than replaceInventoryCoreCTEs's do.
const applyInventoryDeltasSQLWithAliases = applyInventoryDeltasCoreCTEs + `,
input_aliases(idx, namespace, key, value, collection_name, resource_id, received_at) AS (
	SELECT * FROM UNNEST($25::int[], $26::text[], $27::text[], $28::text[], $29::text[], $30::text[], $31::timestamptz[])
),
` + aliasFoldCTEs + aliasConflictSelect

// applyInventoryDeltasSQLNoAliases is ApplyInventoryDeltas's single
// statement for a chunk with no aliases left to fold in -- see
// replaceInventorySQLNoAliases's doc comment for why this variant
// exists and why skipping straight to a no-op terminal SELECT is
// safe.
const applyInventoryDeltasSQLNoAliases = applyInventoryDeltasCoreCTEs + `
SELECT 1 WHERE FALSE`

// ApplyInventoryDeltas implements [domain.ExtensionResourceRepository.ApplyInventoryDeltas]
// as one round trip for the whole chunk: applyInventoryDeltasSQLWithAliases
// or applyInventoryDeltasSQLNoAliases, depending on whether this chunk
// has any aliases left to fold in after checkAliasBatchConsistency.
// See this section's doc comment for what those statements do and why.
func (r *ExtensionResourceRepo) ApplyInventoryDeltas(ctx context.Context, deltas []domain.InventoryDelta) ([]domain.AliasConflict, error) {
	if len(deltas) == 0 {
		return nil, nil
	}
	for _, d := range deltas {
		if err := domain.ValidateInventoryDelta(d); err != nil {
			return nil, err
		}
	}

	n := len(deltas)
	idx := make([]int32, n)
	serviceNames := make([]string, n)
	typeNames := make([]string, n)
	collectionNames := make([]string, n)
	resourceIDs := make([]string, n)
	candidateUIDs := make([]string, n)
	observations := make([]*string, n)
	observedAts := make([]time.Time, n)
	receivedAts := make([]time.Time, n)
	obsHistIDs := make([]string, n)

	var setLabelIdx []int32
	var setLabelKeys, setLabelValues []string
	var delLabelIdx []int32
	var delLabelKeys []string
	var upsertCondIdx []int32
	var upsertCondTypes, upsertCondStatuses, upsertCondReasons, upsertCondMessages []string
	var upsertCondLastTransitions []time.Time
	var upsertCondHistIDs []string
	var delCondIdx []int32
	var delCondTypes []string
	var aliasCandidates []aliasCandidateInput

	for i, d := range deltas {
		idx[i] = int32(i)
		serviceNames[i] = string(d.ResourceType.ServiceName())
		typeNames[i] = d.ResourceType.TypeName()
		collectionNames[i] = string(d.Name.Collection())
		resourceIDs[i] = string(d.Name.ID())
		candidateUIDs[i] = d.CandidateUID.String()
		if obs := normalizeObservation(d.Observation); obs != nil {
			s := string(*obs)
			observations[i] = &s
		}
		observedAts[i] = d.ObservedAt.UTC()
		receivedAts[i] = d.ReceivedAt.UTC()
		obsHistIDs[i] = string(domain.NewObservationID())

		for k, v := range d.SetLabels {
			setLabelIdx = append(setLabelIdx, int32(i))
			setLabelKeys = append(setLabelKeys, k)
			setLabelValues = append(setLabelValues, v)
		}
		for _, k := range d.DeleteLabels {
			delLabelIdx = append(delLabelIdx, int32(i))
			delLabelKeys = append(delLabelKeys, k)
		}
		for _, c := range d.UpsertConditions {
			upsertCondIdx = append(upsertCondIdx, int32(i))
			upsertCondTypes = append(upsertCondTypes, string(c.Type()))
			upsertCondStatuses = append(upsertCondStatuses, string(c.Status()))
			upsertCondReasons = append(upsertCondReasons, c.Reason())
			upsertCondMessages = append(upsertCondMessages, c.Message())
			upsertCondLastTransitions = append(upsertCondLastTransitions, c.LastTransitionTime().UTC())
			upsertCondHistIDs = append(upsertCondHistIDs, uuid.New().String())
		}
		for _, t := range d.DeleteConditions {
			delCondIdx = append(delCondIdx, int32(i))
			delCondTypes = append(delCondTypes, string(t))
		}
		for _, a := range d.Aliases {
			aliasCandidates = append(aliasCandidates, aliasCandidateInput{idx: i, target: d.Name, alias: a, receivedAt: receivedAts[i]})
		}
	}

	safeAliases, conflicts := checkAliasBatchConsistency(aliasCandidates)

	var rows *sql.Rows
	var err error
	if len(safeAliases) > 0 {
		aliasIdx, aliasNamespaces, aliasKeys, aliasValues, aliasCollectionNames, aliasResourceIDs, aliasReceivedAts := flattenAliasCandidates(safeAliases)
		rows, err = r.DB.QueryContext(ctx, applyInventoryDeltasSQLWithAliases,
			idx, serviceNames, typeNames, collectionNames, resourceIDs, candidateUIDs, observations, observedAts, receivedAts, obsHistIDs,
			setLabelIdx, setLabelKeys, setLabelValues,
			delLabelIdx, delLabelKeys,
			upsertCondIdx, upsertCondTypes, upsertCondStatuses, upsertCondReasons, upsertCondMessages, upsertCondLastTransitions, upsertCondHistIDs,
			delCondIdx, delCondTypes,
			aliasIdx, aliasNamespaces, aliasKeys, aliasValues, aliasCollectionNames, aliasResourceIDs, aliasReceivedAts,
		)
	} else {
		rows, err = r.DB.QueryContext(ctx, applyInventoryDeltasSQLNoAliases,
			idx, serviceNames, typeNames, collectionNames, resourceIDs, candidateUIDs, observations, observedAts, receivedAts, obsHistIDs,
			setLabelIdx, setLabelKeys, setLabelValues,
			delLabelIdx, delLabelKeys,
			upsertCondIdx, upsertCondTypes, upsertCondStatuses, upsertCondReasons, upsertCondMessages, upsertCondLastTransitions, upsertCondHistIDs,
			delCondIdx, delCondTypes,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("apply inventory deltas: %w", err)
	}
	defer rows.Close()

	// applyInventoryDeltasSQLNoAliases's terminal SELECT always
	// returns zero rows, so scanAliasConflicts's loop body (and thus
	// its column count assumptions) never runs -- safe to call
	// unconditionally regardless of which statement variant ran.
	dbConflicts, err := scanAliasConflicts(rows, func(i int) domain.ResourceName { return deltas[i].Name })
	if err != nil {
		return nil, fmt.Errorf("apply inventory deltas: %w", err)
	}
	return append(conflicts, dbConflicts...), nil
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
