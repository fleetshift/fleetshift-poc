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

// extensionResourceDeleteWithAliasCleanupSQL deletes an extension
// resource and cleans up any resource_alias_claims rows its deletion
// orphans. There is no foreign key from resource_alias_claims to
// extension_resources (a claim can be shared by many contributors, or
// held by the platform itself via platform_owned -- see the
// migration's doc comment), so unlike resource_intents/
// extension_resource_managed/extension_resource_inventory*/
// resource_alias_contributions -- all of which cascade for free off
// extension_resources.uid -- claim cleanup needs this explicit
// follow-up, or a deleted extension resource's aliases would keep
// resolving forever.
//
// This transcribes poc/alias-claims/'s validated
// extensionResourceDeleteWithRefcountCleanupSQL pattern, which beat
// an alternative that explicitly deletes resource_alias_contributions
// first (a planner accident: that shape forces a seq scan on the
// contributor's own rows where relying on ON DELETE CASCADE picks an
// index nested loop instead -- see that function's sibling
// extensionResourceDeleteWithDirectContributionCleanupSQL for the
// discarded comparison). old_contributions is therefore a plain read,
// *not* a delete: it captures exactly the resource_alias_contributions
// rows that ON DELETE CASCADE is about to remove as a side effect of
// deleted_extension_resource's own DELETE, so their claim ids and a
// pre-delete baseline count can still be computed after the cascade
// has run. The `(SELECT count(*) FROM deleted_extension_resource) >=
// 0` clause in deleted_orphan_claims is not a real filter (it's
// always true) -- it exists purely to give the planner an explicit
// data dependency forcing deleted_extension_resource's cascade to
// happen before deleted_orphan_claims runs. Without it, nothing else
// in this statement's CTE graph forces that ordering, and the
// restrictive claim_id FK (see the migration's doc comment) would
// reject deleted_orphan_claims's delete outright if it ran first,
// while the contributions it's trying to clean up after still exist.
const extensionResourceDeleteWithAliasCleanupSQL = `
WITH target_er AS (
	SELECT uid FROM extension_resources
	WHERE service_name = $1 AND collection_name = $2 AND resource_id = $3
),
old_contributions AS (
	SELECT c.claim_id
	FROM target_er t
	JOIN resource_alias_contributions c ON c.source_extension_resource_uid = t.uid
),
touched_claims AS (
	SELECT DISTINCT claim_id FROM old_contributions
),
baseline_contrib_counts AS (
	SELECT tc.claim_id, cc.baseline_ct
	FROM touched_claims tc
	JOIN LATERAL (
		SELECT count(*)::bigint AS baseline_ct
		FROM resource_alias_contributions c
		WHERE c.claim_id = tc.claim_id
	) cc ON true
),
deleted_extension_resource AS (
	DELETE FROM extension_resources
	WHERE service_name = $1 AND collection_name = $2 AND resource_id = $3
	RETURNING uid
),
net_refcount_deltas AS (
	SELECT claim_id, -count(*)::bigint AS delta_refs
	FROM old_contributions
	GROUP BY claim_id
),
remaining_refs AS (
	SELECT tc.claim_id,
	       COALESCE(bcc.baseline_ct, 0) + COALESCE(nrd.delta_refs, 0) AS net_refs
	FROM touched_claims tc
	LEFT JOIN baseline_contrib_counts bcc ON bcc.claim_id = tc.claim_id
	LEFT JOIN net_refcount_deltas nrd ON nrd.claim_id = tc.claim_id
),
deleted_orphan_claims AS (
	DELETE FROM resource_alias_claims cl
	USING remaining_refs rr
	WHERE cl.id = rr.claim_id
	  AND rr.net_refs = 0
	  AND NOT cl.platform_owned
	  AND (SELECT count(*) FROM deleted_extension_resource) >= 0
	RETURNING 1
)
SELECT (SELECT count(*) FROM deleted_extension_resource)`

func (r *ExtensionResourceRepo) Delete(ctx context.Context, name domain.FullResourceName) error {
	rn := name.ResourceName()
	var n int64
	err := r.DB.QueryRowContext(ctx, extensionResourceDeleteWithAliasCleanupSQL,
		string(name.ServiceName()), string(rn.Collection()), string(rn.ID())).Scan(&n)
	if err != nil {
		return err
	}
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
// the same two invariants resource_alias_claims' pair of unique
// constraints enforce against *pre-existing* rows (see the
// migration's doc comment): the same (namespace, key, value) claimed
// by two different targets, or the same target given two different
// values for the same (namespace, key). Neither shape can be left for
// inserted_claims's own INSERT to sort out: the first shape, reached
// twice within one INSERT's row source, would abort the *entire*
// statement with a straight unique-violation error on
// UNIQUE(namespace, key, value); the second would do the same against
// UNIQUE(namespace, key, platform_collection_name,
// platform_resource_id). Catching both shapes here, before any array
// is built, keeps the failure mode graceful -- a [domain.AliasConflict]
// rather than a hard SQL error. See the nameless-platform-identity
// plan's "Why aliases split into two paths, not two write statements"
// section -- validateAliasUpsertsConsistent's old role, now scoped to
// exactly the case a unique index can't cover. (inserted_claims's and
// upserted_contributions's own SELECT DISTINCT/DISTINCT ON
// additionally guard against a literal duplicate -- same target, same
// alias, reported twice in one batch -- which isn't a contradiction
// and so isn't rejected here, but would otherwise hit either the same
// unique-violation error (a plain INSERT can't tolerate two identical
// rows in one statement, ON CONFLICT or not) or, for
// upserted_contributions specifically, Postgres's "ON CONFLICT DO
// UPDATE command cannot affect row a second time" restriction.)
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
// input_aliases join er by idx a second time to fetch values er
// itself only ever passed through from input_er unchanged.
//
// The one value that can't ride along this way is source_uid:
// unlike those three, a candidate's owning extension_resources.uid
// isn't necessarily known in Go memory yet at the time this array is
// built -- resolved_er may still be about to generate it, lazily, for
// a genuinely new resource (see [domain.InventoryReplacement.CandidateUID]'s
// doc for why that's a candidate, not a guarantee) -- so input_aliases
// does still join er by idx once, purely for that column. See
// aliasFoldCTEs's doc comment for why source_uid is needed at all now
// that it wasn't before.
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
// resource_alias_claims state -- into [domain.AliasConflict]s.
// targetOf maps a flattened input row's idx back to its report's
// resolved target name.
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
// after a statement's input_aliases CTE and before its final SELECT
// (replaceInventorySQL appends its own extra fragment after this one
// too -- see aliasRetractAbsentCTE's doc comment). Unlike the rest of
// this file's aliasCandidateInput plumbing, input_aliases *does* join
// er by idx, for source_uid -- see flattenAliasCandidates's doc
// comment for why that join, once deliberately avoided, is now
// required.
//
// This classifies and writes against the split
// resource_alias_claims/resource_alias_contributions schema (see the
// migration's doc comment, and poc/alias-claims/ for the validated
// prototype this transcribes almost verbatim). The two cross-resource
// invariants -- (namespace, key, value) can only ever map to one
// platform resource, and a platform resource can only ever have one
// value for a given (namespace, key) -- are now plain B-tree UNIQUE
// constraints on resource_alias_claims itself, not per-contributor
// EXCLUDE constraints, because a claim is represented exactly once
// regardless of how many extension resources contribute it.
// checkAliasBatchConsistency has already ruled out an intra-batch
// collision against either invariant, so every remaining conflict
// source is a pre-existing resource_alias_claims row held by a
// *different* contributor (or the platform itself -- see
// resource_identity_repo.go's reconcileAliases).
//
// self_claim/changed implement the fast no-op skip this design's
// whole point was to make cheap: for each candidate, look up *this*
// contributor's own current contribution for (namespace, key) --
// resource_alias_contributions is keyed by exactly that, a single
// indexed point lookup -- and compare its claim's (value, target)
// against what's being reported now. A steady-state report (the
// overwhelming common case) matches exactly and drops out of
// `changed` entirely, touching neither resource_alias_claims nor
// resource_alias_contributions again this call.
//
// For everything that *did* change, by_value/by_resource/sibling
// classify by reading, before writing anything, in the same two
// phases the old single-table design used (see this file's git
// history for that version's own doc comment): by_value asks "does
// this exact (namespace, key, value) already exist, and for whom" --
// a match for a *different* target is alias_value_conflicts
// (AliasConflictValueClaimedByOther), resolved without ever touching
// by_resource. by_resource asks "does this target already have *some*
// claim for this key" -- since by_value already proved the reported
// value doesn't exist anywhere, a match here is necessarily a
// *different* value, and sibling asks whether anyone besides this
// candidate's own contributor (or the platform, via platform_owned)
// still holds it: if so, alias_resource_conflicts
// (AliasConflictResourceHasDifferentValue); otherwise this
// contributor is free to move off it.
//
// A closed-form argument -- not just an empirical claim -- shows
// `safe` always splits cleanly into exactly three disjoint write
// shapes, with no case left needing a delete-then-insert dance:
// resource_alias_claims' own UNIQUE(namespace, key,
// platform_collection_name, platform_resource_id) guarantees at most
// one claim exists for (this candidate's fixed target, namespace,
// key) at any instant, and every claim a candidate's own prior
// contribution could possibly point to necessarily *is* that one
// claim (aliases are always self-referential -- a report only ever
// asserts aliases about its own reported Name, which is immutable
// once an extension resource's natural key exists). So:
//
//   - claim_creates: by_value found nothing, by_resource found
//     nothing -- a genuinely new (namespace, key) for this target.
//     Insert a fresh claim.
//   - claim_self_replace: by_value found nothing, by_resource found
//     this target's *own* claim for the key with sibling_holds false
//     -- this candidate is that claim's only holder (or the only
//     extension-resource holder, with the platform not asserting it
//     either), so it's free to change its value in place. This can
//     never be the fold-in's own prior claim moving to a *different*
//     existing claim row -- that would require by_value to find a
//     claim whose target matches this candidate's, but the UNIQUE
//     constraint above already guarantees that claim (if any) *is*
//     by_resource's claim, and by_resource's claim holds this
//     candidate's *old* value, not the new one just proven absent by
//     by_value. So this case is always a plain UPDATE of the existing
//     claim row's value: no new row, no contribution change, and
//     critically no old claim left vacated to clean up.
//   - claim_reuse: by_value found an existing claim whose target
//     matches this candidate's own -- another contributor (or the
//     platform) already asserted this exact value for this same
//     target, and this candidate is newly corroborating it. By the
//     same argument, this can only happen when this candidate had *no*
//     prior claim for the key at all (self_claim_id IS NULL): if it
//     had one, by_resource's lookup would already occupy this
//     target's UNIQUE(namespace, key, target) slot with the
//     candidate's *own*, different-valued claim, making it impossible
//     for by_value's freshly-proven-different-valued claim to *also*
//     have this same target. So joining an existing claim is always a
//     brand-new contribution, never a move that vacates anything.
//
// The upshot: this fold-in alone never orphans a claim. The *only*
// way a claim can lose its last reference is aliasRetractAbsentCTE's
// del_aliases_absent (ReplaceInventory's "absence retracts" rule) or
// a contributing extension resource's own deletion (see
// extensionResourceDeleteWithAliasCleanupSQL) -- both are the sole
// places claim cleanup needs to run, which is why it isn't part of
// this shared fold-in at all.
//
// That argument is also why upserted_contributions is a plain INSERT,
// not an upsert: claim_targets (claim_reuse plus claim_creates) is,
// by the same closed-form argument, only ever populated by
// self_claim_id IS NULL candidates -- a candidate with an existing
// contribution row always routes through claim_self_replace instead,
// which never touches resource_alias_contributions at all. So every
// row this INSERT attempts is, by construction, for a
// (source_extension_resource_uid, namespace, key) with no pre-existing
// row -- ON CONFLICT ... DO UPDATE SET claim_id = EXCLUDED.claim_id
// would only ever fire if that invariant were violated by a future
// bug, and silently moving a contribution's claim_id that way is
// exactly the kind of change that needs compensating cleanup on the
// claim it moved *from* (see this comment's own argument for why the
// fold-in doesn't carry that cleanup). Letting the insert fail loudly
// on a real primary-key collision instead surfaces that bug
// immediately, the same way the restrictive claim_id FK on
// resource_alias_contributions does (see the migration's doc comment)
// -- rather than an upsert quietly orphaning a claim with no error at
// all. SELECT DISTINCT ON above still collapses the one legitimate
// source of an intra-statement duplicate key here -- the same alias
// reported twice within one report (see
// DuplicateAliasWithinReportIsNotAConflict) -- before the INSERT ever
// sees it.
//
// Every LATERAL here uses LIMIT 1 OFFSET 0, which is load-bearing,
// not decorative: without it, Postgres can "de-correlate" a LATERAL
// whose body is just an equality filter back into an ordinary join
// it's free to hash-and-scan, defeating the point of a per-candidate
// indexed lookup -- OFFSET 0 is the standard trick for telling the
// planner a subquery must actually be evaluated as written, one outer
// row at a time.
//
// cand_id (a row_number() surrogate minted once in input_aliases) is
// the correlation key every downstream CTE joins back on -- not idx.
// idx is a *report*'s index, shared by every alias candidate within
// that one report, so two aliases on the same report (a common case:
// a cluster reporting both a provider ID and a vendor GUID) collide
// on idx. Joining by_value/by_resource/sibling back to changed with
// `ON x.idx = ch.idx` fans each of those same-report candidates out
// against every other same-report candidate's own lookup result --
// changed's create path happened to survive that (claim_creates only
// projects columns that come from ch, so inserted_claims's SELECT
// DISTINCT silently absorbs the duplicate fan-out rows), but
// claim_self_replace's resource_claim_id came from the fanned-out br
// side, so a report with two self-replacing aliases could pick up the
// *other* candidate's resource_claim_id and overwrite the wrong claim
// row's value. cand_id being unique per candidate row (not per
// report) closes that: every join below matches exactly one row on
// each side, so no fan-out is possible regardless of how many aliases
// one report carries.
// aliasFingerprintGateCTEs is replaceInventorySQL's own addition,
// spliced in right after replaceInventoryCoreCTEs and before
// input_aliases/aliasFoldCTEs, implementing extension_resources.
// alias_fingerprint's fast path (see the migration's doc comment on
// that column): a repeated report whose complete alias set hasn't
// changed since the last time this resource's aliases were fully,
// successfully applied should skip alias classification entirely
// rather than re-running every lookup in aliasFoldCTEs for no
// behavioral change.
//
// input_report_fingerprints carries $31, a per-report bytea computed
// in Go by [domain.AliasSetFingerprint] over that report's complete
// Aliases -- reusing $1 (idx) rather than minting a fresh placeholder
// array, since it's already the exact same per-report index UNNEST
// elsewhere in this statement needs to join back to. needs_alias_processing
// then compares that against er.stored_alias_fingerprint with IS
// DISTINCT FROM, which treats NULL (a resource with no prior
// successful fingerprint write -- see er's own doc comment) as always
// distinct from any concrete reported hash, so a resource that has
// never yet gotten this far always gets processed.
//
// input_aliases and aliasRetractAbsentCTE's del_aliases_absent both
// join needs_alias_processing to scope themselves to only the reports
// that need it -- critically, del_aliases_absent must be scoped this
// way and not just rely on a fingerprint-matched report supplying zero
// input_aliases rows for it: with no scoping at all, "zero candidates
// this round" is indistinguishable from "this resource now has zero
// aliases", and del_aliases_absent would retract every alias a
// fingerprint-matched (i.e. unchanged) resource still legitimately
// holds.
const aliasFingerprintGateCTEs = `
input_report_fingerprints(idx, reported_alias_fingerprint) AS (
	SELECT * FROM UNNEST($1::int[], $31::bytea[])
),
needs_alias_processing AS (
	SELECT e.idx
	FROM er e
	JOIN input_report_fingerprints irf ON irf.idx = e.idx
	WHERE irf.reported_alias_fingerprint IS DISTINCT FROM e.stored_alias_fingerprint
),
`

const aliasFoldCTEs = `
self_claim AS (
	SELECT ia.cand_id, ia.idx, ia.namespace, ia.key, ia.value, ia.collection_name, ia.resource_id, ia.received_at, ia.source_uid,
	       c.claim_id AS self_claim_id, cl.value AS self_value,
	       cl.platform_collection_name AS self_collection_name, cl.platform_resource_id AS self_resource_id
	FROM input_aliases ia
	LEFT JOIN LATERAL (
		SELECT claim_id
		FROM resource_alias_contributions
		WHERE source_extension_resource_uid = ia.source_uid
		  AND namespace = ia.namespace AND key = ia.key
		LIMIT 1 OFFSET 0
	) c ON true
	LEFT JOIN LATERAL (
		SELECT value, platform_collection_name, platform_resource_id
		FROM resource_alias_claims
		WHERE id = c.claim_id
		LIMIT 1 OFFSET 0
	) cl ON true
),
changed AS (
	SELECT cand_id, idx, namespace, key, value, collection_name, resource_id, received_at, source_uid, self_claim_id
	FROM self_claim
	WHERE self_claim_id IS NULL
	   OR self_value IS DISTINCT FROM value
	   OR self_collection_name IS DISTINCT FROM collection_name
	   OR self_resource_id IS DISTINCT FROM resource_id
),
by_value AS (
	SELECT ch.cand_id, vc.id AS value_claim_id, vc.platform_collection_name AS value_collection_name, vc.platform_resource_id AS value_resource_id
	FROM changed ch
	LEFT JOIN LATERAL (
		SELECT id, platform_collection_name, platform_resource_id
		FROM resource_alias_claims
		WHERE namespace = ch.namespace AND key = ch.key AND value = ch.value
		LIMIT 1 OFFSET 0
	) vc ON true
),
by_resource AS (
	SELECT ch.cand_id, rc.id AS resource_claim_id, rc.value AS resource_value, rc.platform_owned AS resource_platform_owned
	FROM changed ch
	LEFT JOIN LATERAL (
		SELECT id, value, platform_owned
		FROM resource_alias_claims
		WHERE namespace = ch.namespace AND key = ch.key
		  AND platform_collection_name = ch.collection_name AND platform_resource_id = ch.resource_id
		LIMIT 1 OFFSET 0
	) rc ON true
),
sibling AS (
	SELECT br.cand_id,
	       br.resource_claim_id IS NOT NULL
	       AND (
		 br.resource_platform_owned
		 OR EXISTS (
			SELECT 1
			FROM resource_alias_contributions other
			JOIN changed ch ON ch.cand_id = br.cand_id
			WHERE other.claim_id = br.resource_claim_id
			  AND other.source_extension_resource_uid <> ch.source_uid
		 )
	       ) AS sibling_holds
	FROM by_resource br
),
alias_value_conflicts AS (
	SELECT ch.idx, ch.namespace, ch.key, ch.value,
	       bv.value_collection_name AS actual_collection_name, bv.value_resource_id AS actual_resource_id
	FROM changed ch
	JOIN by_value bv ON bv.cand_id = ch.cand_id
	WHERE bv.value_claim_id IS NOT NULL
	  AND (bv.value_collection_name <> ch.collection_name OR bv.value_resource_id <> ch.resource_id)
),
alias_resource_conflicts AS (
	SELECT ch.idx, ch.namespace, ch.key, ch.value, br.resource_value AS existing_value
	FROM changed ch
	JOIN by_value bv ON bv.cand_id = ch.cand_id
	JOIN by_resource br ON br.cand_id = ch.cand_id
	JOIN sibling s ON s.cand_id = ch.cand_id
	WHERE bv.value_claim_id IS NULL AND br.resource_claim_id IS NOT NULL AND s.sibling_holds
),
safe AS (
	SELECT ch.cand_id, ch.namespace, ch.key, ch.value, ch.collection_name, ch.resource_id, ch.received_at, ch.source_uid,
	       bv.value_claim_id, br.resource_claim_id
	FROM changed ch
	JOIN by_value bv ON bv.cand_id = ch.cand_id
	JOIN by_resource br ON br.cand_id = ch.cand_id
	JOIN sibling s ON s.cand_id = ch.cand_id
	WHERE NOT (bv.value_claim_id IS NOT NULL AND (bv.value_collection_name <> ch.collection_name OR bv.value_resource_id <> ch.resource_id))
	  AND NOT (bv.value_claim_id IS NULL AND br.resource_claim_id IS NOT NULL AND s.sibling_holds)
),
claim_creates AS (
	SELECT namespace, key, value, collection_name, resource_id, received_at, source_uid
	FROM safe WHERE value_claim_id IS NULL AND resource_claim_id IS NULL
),
claim_self_replace AS (
	SELECT value, received_at, resource_claim_id
	FROM safe WHERE value_claim_id IS NULL AND resource_claim_id IS NOT NULL
),
claim_reuse AS (
	SELECT namespace, key, received_at, source_uid, value_claim_id
	FROM safe WHERE value_claim_id IS NOT NULL
),
inserted_claims AS (
	INSERT INTO resource_alias_claims (namespace, key, value, platform_collection_name, platform_resource_id, created_at)
	SELECT DISTINCT namespace, key, value, collection_name, resource_id, received_at
	FROM claim_creates
	RETURNING id, namespace, key, value
),
updated_claims AS (
	UPDATE resource_alias_claims cl
	SET value = sr.value
	FROM claim_self_replace sr
	WHERE cl.id = sr.resource_claim_id
	RETURNING 1
),
claim_targets AS (
	SELECT namespace, key, received_at, source_uid, value_claim_id AS claim_id FROM claim_reuse
	UNION ALL
	SELECT cc.namespace, cc.key, cc.received_at, cc.source_uid, ic.id AS claim_id
	FROM claim_creates cc
	JOIN inserted_claims ic ON ic.namespace = cc.namespace AND ic.key = cc.key AND ic.value = cc.value
),
upserted_contributions AS (
	INSERT INTO resource_alias_contributions (source_extension_resource_uid, namespace, key, claim_id, created_at)
	SELECT DISTINCT ON (source_uid, namespace, key) source_uid, namespace, key, claim_id, received_at
	FROM claim_targets
	ORDER BY source_uid, namespace, key
	RETURNING claim_id
)`

// aliasConflictSelect is the final SELECT shared verbatim by
// replaceInventorySQL and applyInventoryDeltasSQL, appended after
// aliasFoldCTEs (and, for replaceInventorySQL, aliasRetractAbsentCTE).
// Two branches, one per conflict source: alias_value_conflicts
// (phase 1, read-by-value) is AliasConflictValueClaimedByOther;
// alias_resource_conflicts (phase 2, read-by-resource) is
// AliasConflictResourceHasDifferentValue. A candidate matching
// neither (the common case) is absent from the result entirely.
const aliasConflictSelect = `
SELECT idx, namespace, key, value,
       actual_collection_name, actual_resource_id, NULL::text AS actual_value
FROM alias_value_conflicts
UNION ALL
SELECT idx, namespace, key, value,
       NULL::text AS actual_collection_name, NULL::text AS actual_resource_id, existing_value AS actual_value
FROM alias_resource_conflicts`

// aliasRetractAbsentCTE is replaceInventorySQL's own addition after
// aliasFoldCTEs, implementing the "absence = removal" half of
// [domain.InventoryReplacement.Aliases]'s per-contributor replace
// contract that applyInventoryDeltasSQL deliberately doesn't share
// for its own UpsertAliases (see [domain.InventoryDelta]'s doc --
// UpsertAliases never retracts; ReplaceAliases would need this same
// treatment, but doesn't have it yet either).
//
// input_reported_alias_keys carries every (idx, namespace,
// key) pair originally in this chunk's reports, *before*
// checkAliasBatchConsistency's Go-side filtering -- not
// input_aliases, which only has the survivors. A key
// checkAliasBatchConsistency rejected as an intra-batch contradiction
// was still reported, just not written; deleting the target's
// existing row for it here, on top of that rejection, would make one
// bad candidate in a batch destroy state a good one never touched --
// exactly the kind of interference PartialConflictInMultiAliasReportStillAppliesTheRest
// and friends exist to rule out. This mirrors del_labels_absent
// (replaceInventoryCoreCTEs) exactly in shape, just scoped by
// (namespace, key) instead of a single label key, and by contributor
// (source_extension_resource_uid) instead of directly by uid -- same
// thing, since a contributor's rows are always its own
// extension_resources.uid, but named for what it means here.
//
// del_aliases_absent is also the *only* place this statement can
// orphan a resource_alias_claims row -- aliasFoldCTEs's own fold-in
// never does (see its doc comment's closed-form argument) -- so the
// refcount-based cleanup pipeline below (touched_claims through
// deleted_orphan_claims) lives entirely in this ReplaceInventory-only
// fragment rather than in the shared aliasFoldCTEs. It transcribes
// poc/alias-claims/'s validated retractWithDirectDeleteRefcountCleanupSQL
// pattern: touched_claims are exactly the claims del_aliases_absent
// just removed a contribution from; baseline_contrib_counts reads
// each one's contribution count as of the start of this statement
// (Postgres CTEs all share that one pre-statement snapshot regardless
// of execution order, so this read is unaffected by del_aliases_absent's
// own delete); net_refcount_deltas nets that baseline against both
// del_aliases_absent's departures (-1 each) *and* upserted_contributions's
// arrivals (+1 each) landing on the same claim id -- the latter is
// what correctly keeps a claim alive when one contributor's retraction
// and a different contributor's fresh corroboration of the very same
// value cross paths within this one batch, rather than the first
// write "winning" and orphaning a claim a sibling write is
// simultaneously still asserting. A claim only gets deleted once its
// net reference count reaches zero and it isn't platform_owned (see
// resource_identity_repo.go's reconcileAliases for that flag's own
// lifecycle) -- the restrictive (non-cascading) claim_id FK on
// resource_alias_contributions (see the migration's doc comment) is
// the backstop if this logic is ever wrong: a claim deletion attempted
// while a live contribution still points at it fails loudly instead
// of silently corrupting that contribution's state.
//
// del_aliases_absent's USING joins needs_alias_processing (see
// aliasFingerprintGateCTEs) on top of er, not just er alone -- a
// fingerprint-matched report supplies zero input_aliases rows for its
// resource the same way a resource with a genuinely empty Aliases
// field would, and without this join the two are indistinguishable:
// del_aliases_absent would retract every alias a fingerprint-matched
// (unchanged) resource still legitimately holds.
const aliasRetractAbsentCTE = `,
input_reported_alias_keys(idx, namespace, key) AS (
	SELECT * FROM UNNEST($28::int[], $29::text[], $30::text[])
),
del_aliases_absent AS (
	DELETE FROM resource_alias_contributions c
	USING er e
	JOIN needs_alias_processing nap ON nap.idx = e.idx
	WHERE c.source_extension_resource_uid = e.uid
	  AND NOT EXISTS (
	    SELECT 1 FROM input_reported_alias_keys irk
	    WHERE irk.idx = e.idx AND irk.namespace = c.namespace AND irk.key = c.key
	  )
	RETURNING c.claim_id
),
touched_claims AS (
	SELECT DISTINCT claim_id FROM del_aliases_absent
),
baseline_contrib_counts AS (
	SELECT tc.claim_id, cc.baseline_ct
	FROM touched_claims tc
	JOIN LATERAL (
		SELECT count(*)::bigint AS baseline_ct
		FROM resource_alias_contributions c
		WHERE c.claim_id = tc.claim_id
	) cc ON true
),
net_refcount_deltas AS (
	SELECT claim_id, sum(delta_refs)::bigint AS delta_refs
	FROM (
		SELECT claim_id, -count(*)::bigint AS delta_refs FROM del_aliases_absent GROUP BY claim_id
		UNION ALL
		SELECT claim_id, count(*)::bigint AS delta_refs FROM upserted_contributions GROUP BY claim_id
	) deltas
	GROUP BY claim_id
),
remaining_refs AS (
	SELECT tc.claim_id,
	       COALESCE(bcc.baseline_ct, 0) + COALESCE(nrd.delta_refs, 0) AS net_refs
	FROM touched_claims tc
	LEFT JOIN baseline_contrib_counts bcc ON bcc.claim_id = tc.claim_id
	LEFT JOIN net_refcount_deltas nrd ON nrd.claim_id = tc.claim_id
),
deleted_orphan_claims AS (
	DELETE FROM resource_alias_claims cl
	USING remaining_refs rr
	WHERE cl.id = rr.claim_id
	  AND rr.net_refs = 0
	  AND NOT cl.platform_owned
	RETURNING 1
)`

// aliasFingerprintWriteCTE is replaceInventorySQL's final addition,
// spliced in after aliasRetractAbsentCTE and before aliasConflictSelect,
// completing the fast path aliasFingerprintGateCTEs started: once a
// report's aliases have been fully classified and retracted-if-absent
// with zero conflicts, remember its reported fingerprint so a later,
// byte-for-byte identical report can skip all of this.
//
// fingerprint_updates is deliberately gated on *zero* conflicts for
// that report's idx, checked against both alias_value_conflicts and
// alias_resource_conflicts -- a report with even one conflicting
// alias must never get its fingerprint written, or a later repeat of
// that exact same still-conflicting report would compare equal to the
// (wrongly) stored fingerprint and skip reprocessing entirely,
// silently swallowing a conflict that's still true. Because a report
// with an unresolved conflict therefore keeps stored_alias_fingerprint
// NULL (or whatever it was before), and NULL is always IS DISTINCT
// FROM any concrete hash, needs_alias_processing naturally keeps
// reprocessing it on every subsequent call for free -- no separate
// "retry" bookkeeping needed. See
// RepeatedReportWithPersistentAliasConflictKeepsReturningConflict.
//
// updated_fingerprints's UPDATE targets extension_resources by uid,
// which per Postgres's WITH semantics reads that table's own
// pre-statement snapshot -- so a brand-new resource resolved_er just
// inserted earlier in this very statement is invisible to this scan,
// and the UPDATE simply matches zero rows for it. That's fine, not a
// bug: the row is already NULL by column default, so "the write
// didn't happen" and "the write set it to NULL-equivalent" are
// indistinguishable in effect, and this resource's *next* report
// (now reading a row that pre-exists the statement) will see it and
// write successfully then.
//
// ApplyInventoryDeltas has no analogous write-back: it has no
// fingerprint fast path of its own to complete (see
// applyInventoryDeltasSQLWithAliases's aliasFingerprintInvalidateCTE
// instead, which only ever clears a fingerprint, never sets one).
const aliasFingerprintWriteCTE = `,
fingerprint_updates AS (
	SELECT nap.idx, e.uid, irf.reported_alias_fingerprint
	FROM needs_alias_processing nap
	JOIN er e ON e.idx = nap.idx
	JOIN input_report_fingerprints irf ON irf.idx = nap.idx
	WHERE NOT EXISTS (SELECT 1 FROM alias_value_conflicts avc WHERE avc.idx = nap.idx)
	  AND NOT EXISTS (SELECT 1 FROM alias_resource_conflicts arc WHERE arc.idx = nap.idx)
),
updated_fingerprints AS (
	UPDATE extension_resources ext
	SET alias_fingerprint = fu.reported_alias_fingerprint
	FROM fingerprint_updates fu
	WHERE ext.uid = fu.uid
	RETURNING 1
)`

// replaceInventoryCoreCTEs is the natural-key resolve-or-create +
// latest-row + label + condition pipeline underlying replaceInventorySQL --
// everything ReplaceInventory needs regardless of whether this
// chunk's replacements carry any aliases (see replaceInventorySQL's
// own doc comment for why, unlike applyInventoryDeltasCoreCTEs below,
// there's no separate alias-free variant here). input_er/resolved_er/er
// resolve-or-create every replacement's extension_resources row by
// natural key (see this section's doc comment); upsert_inv/hist_obs
// write the latest row and its change-guarded observation history;
// del_labels_absent/upsert_labels implement "Labels is the complete
// latest label set"; del_conditions_absent/upsert_conditions/
// hist_conditions implement the same for conditions plus
// change-guarded transition history. er also exposes
// stored_alias_fingerprint (extension_resources.alias_fingerprint as
// of the start of this statement -- NULL both for a resource that
// pre-exists but has never had a successful, fully-applied alias
// write, and for one resolved_er just inserted, since ext's LEFT JOIN
// reads the pre-statement snapshot and never sees resolved_er's own
// insert) for aliasFingerprintGateCTEs to compare against each
// report's freshly computed fingerprint. Deliberately has no trailing
// comma after hist_conditions's closing paren -- replaceInventorySQL
// supplies its own continuation straight into aliasFingerprintGateCTEs/
// input_aliases/aliasFoldCTEs. Placeholder count is fixed (20)
// regardless of chunk size -- only the array arguments grow.
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
	       COALESCE(res.uid, ext.uid) AS uid,
	       ext.alias_fingerprint AS stored_alias_fingerprint
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

// replaceInventorySQL is ReplaceInventory's single statement,
// covering every chunk regardless of whether its reports carry any
// aliases: replaceInventoryCoreCTEs, then aliasFingerprintGateCTEs
// (placeholder $31) deciding per-report which reports actually need
// alias processing, then an input_aliases CTE (placeholders $21-$27)
// joining er by idx for source_uid (see flattenAliasCandidates's doc
// comment for why) and needs_alias_processing to skip reports whose
// fingerprint matched, feeding aliasFoldCTEs, then aliasRetractAbsentCTE
// (placeholders $28-$30) for the "absence retracts" half of the
// replace contract (also scoped to needs_alias_processing -- see its
// own doc comment), then aliasFingerprintWriteCTE to remember each
// successfully-processed report's fingerprint, then aliasConflictSelect.
// Unlike applyInventoryDeltasSQLWithAliases/applyInventoryDeltasSQLNoAliases,
// there is deliberately no alias-free fast-path *statement* variant
// here: aliasRetractAbsentCTE's del_aliases_absent has to run even for
// a chunk whose reports supply zero aliases, since under replace
// semantics an empty Aliases field is itself meaningful -- it means
// "this resource has no aliases now" -- and must still retract
// whatever that resource held before. aliasFingerprintGateCTEs is
// this statement's actual fast path instead: cheap enough to always
// include, since a resource whose reported alias set (empty or not)
// matches what's already stored skips both input_aliases and
// del_aliases_absent's per-report work via needs_alias_processing.
// See aliasRetractAbsentCTE's own doc comment for the rest of that
// reasoning.
const replaceInventorySQL = replaceInventoryCoreCTEs + `,
` + aliasFingerprintGateCTEs + `
input_aliases AS (
	SELECT row_number() OVER () AS cand_id,
	       ac.idx, ac.namespace, ac.key, ac.value, ac.collection_name, ac.resource_id, ac.received_at, e.uid AS source_uid
	FROM UNNEST($21::int[], $22::text[], $23::text[], $24::text[], $25::text[], $26::text[], $27::timestamptz[])
	     AS ac(idx, namespace, key, value, collection_name, resource_id, received_at)
	JOIN er e ON e.idx = ac.idx
	JOIN needs_alias_processing nap ON nap.idx = e.idx
),
` + aliasFoldCTEs + aliasRetractAbsentCTE + aliasFingerprintWriteCTE + aliasConflictSelect

// ReplaceInventory implements [domain.ExtensionResourceRepository.ReplaceInventory]
// as one round trip for the whole chunk via replaceInventorySQL. See
// that constant's doc comment for what it does and why it has no
// alias-free fast path the way ApplyInventoryDeltas does.
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
	// reportedAliasFingerprints feeds aliasFingerprintGateCTEs'
	// input_report_fingerprints -- one hash per report, computed over
	// its complete, as-reported Aliases regardless of what
	// checkAliasBatchConsistency does with them afterward, since the
	// fast path's whole point is comparing "what got reported" against
	// "what's stored", not "what got applied".
	reportedAliasFingerprints := make([][]byte, n)

	var labelIdx []int32
	var labelKeys, labelValues []string
	var condIdx []int32
	var condTypes, condStatuses, condReasons, condMessages []string
	var condLastTransitions []time.Time
	var condHistIDs []string
	var aliasCandidates []aliasCandidateInput
	// reportedAlias{Idx,Namespaces,Keys} feed aliasRetractAbsentCTE's
	// input_reported_alias_keys and, unlike aliasCandidates, are
	// never filtered by checkAliasBatchConsistency -- see
	// aliasRetractAbsentCTE's doc comment for why a candidate this
	// batch goes on to reject must still count as "reported" for
	// retraction purposes.
	var reportedAliasIdx []int32
	var reportedAliasNamespaces, reportedAliasKeys []string

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
		reportedAliasFingerprints[i] = domain.AliasSetFingerprint(rep.Aliases)

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
			reportedAliasIdx = append(reportedAliasIdx, int32(i))
			reportedAliasNamespaces = append(reportedAliasNamespaces, string(a.Namespace))
			reportedAliasKeys = append(reportedAliasKeys, string(a.Key))
		}
	}

	safeAliases, conflicts := checkAliasBatchConsistency(aliasCandidates)
	aliasIdx, aliasNamespaces, aliasKeys, aliasValues, aliasCollectionNames, aliasResourceIDs, aliasReceivedAts := flattenAliasCandidates(safeAliases)

	rows, err := r.DB.QueryContext(ctx, replaceInventorySQL,
		idx, serviceNames, typeNames, collectionNames, resourceIDs, candidateUIDs, observations, observedAts, receivedAts, obsHistIDs,
		labelIdx, labelKeys, labelValues,
		condIdx, condTypes, condStatuses, condReasons, condMessages, condLastTransitions, condHistIDs,
		aliasIdx, aliasNamespaces, aliasKeys, aliasValues, aliasCollectionNames, aliasResourceIDs, aliasReceivedAts,
		reportedAliasIdx, reportedAliasNamespaces, reportedAliasKeys,
		reportedAliasFingerprints,
	)
	if err != nil {
		return nil, fmt.Errorf("replace inventory: %w", err)
	}
	defer rows.Close()

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

// aliasFingerprintInvalidateCTE is applyInventoryDeltasSQLWithAliases's
// own addition after aliasFoldCTEs, the delta-side half of
// extension_resources.alias_fingerprint's bookkeeping:
// replaceInventorySQL's aliasFingerprintWriteCTE is the only place
// that *sets* a fingerprint, so this is the only place that needs to
// *clear* one -- a delta that upserts an alias changes what "this
// resource's complete alias set" truthfully is out from under
// whatever ReplaceInventory last fingerprinted, and a later replace
// reporting that same old set again must not mistake it for
// unchanged (see DeltaAddedAliasIsRetractedByLaterReplaceWithOriginalFingerprintedSet).
//
// Scoped to every distinct source_uid in input_aliases -- i.e. every
// resource with at least one UpsertAliases candidate that survived
// checkAliasBatchConsistency -- regardless of whether that candidate
// goes on to conflict in aliasFoldCTEs. A conflicting upsert is
// rejected and never actually changes the resource's true alias set,
// so invalidating it too is unnecessary, but harmless: it costs that
// resource one extra full reprocess on its next ReplaceInventory call
// rather than a skip, never a correctness gap. Deliberately unconditional
// for the same reason aliasFingerprintWriteCTE is conditioned the
// other way (on zero conflicts) -- an over-eager invalidation is
// always safe, an under-eager one is not.
const aliasFingerprintInvalidateCTE = `,
invalidated_fingerprints AS (
	UPDATE extension_resources ext
	SET alias_fingerprint = NULL
	FROM (SELECT DISTINCT source_uid FROM input_aliases) touched
	WHERE ext.uid = touched.source_uid
	RETURNING 1
)`

// applyInventoryDeltasSQLWithAliases is ApplyInventoryDeltas's single
// statement for a chunk containing at least one alias to fold in --
// see replaceInventorySQL's doc comment for the shape (including why
// input_aliases joins er by idx for source_uid); placeholders $25-$31
// here instead of $21-$27 since applyInventoryDeltasCoreCTEs's input
// CTEs use four more placeholders than replaceInventoryCoreCTEs's do.
// Unlike replaceInventorySQL, there's no aliasRetractAbsentCTE here:
// [domain.InventoryDelta.UpsertAliases] is additive-only by contract,
// so a delta chunk with no UpsertAliases has nothing at all to do
// alias-wise -- see applyInventoryDeltasSQLNoAliases just below.
// (DeleteAliases/ReplaceAliases aren't folded into this statement
// yet -- see the loop building aliasCandidates in ApplyInventoryDeltas
// below. When they are, they'll need the same aliasFingerprintInvalidateCTE
// treatment.)
const applyInventoryDeltasSQLWithAliases = applyInventoryDeltasCoreCTEs + `,
input_aliases AS (
	SELECT row_number() OVER () AS cand_id,
	       ac.idx, ac.namespace, ac.key, ac.value, ac.collection_name, ac.resource_id, ac.received_at, e.uid AS source_uid
	FROM UNNEST($25::int[], $26::text[], $27::text[], $28::text[], $29::text[], $30::text[], $31::timestamptz[])
	     AS ac(idx, namespace, key, value, collection_name, resource_id, received_at)
	JOIN er e ON e.idx = ac.idx
),
` + aliasFoldCTEs + aliasFingerprintInvalidateCTE + aliasConflictSelect

// applyInventoryDeltasSQLNoAliases is ApplyInventoryDeltas's single
// statement for a chunk with no aliases left to fold in (either
// because none of its deltas supplied any, or because every candidate
// was already rejected in Go by checkAliasBatchConsistency) -- just
// applyInventoryDeltasCoreCTEs, skipping input_aliases/aliasFoldCTEs/
// aliasConflictSelect entirely. Safe here in a way it no longer is
// for ReplaceInventory: a delta never retracts (see
// applyInventoryDeltasSQLWithAliases's doc comment), so "no aliases
// left to fold in" really does mean "nothing alias-related to do" for
// this chunk, with no absence-implies-removal case to account for.
// This is also the overwhelming common case (most resource types
// report no aliases at all), so avoiding aliasFoldCTEs' joins and
// conditional inserts against an input that would otherwise always be
// empty is worth a second statement variant. The trailing SELECT is a
// required terminal statement for the WITH clause -- ApplyInventoryDeltas
// never scans it -- and is safe to leave disconnected from the CTEs
// above it: per the Postgres manual's "Data-Modifying Statements in
// WITH" section, such statements "are executed exactly once, and
// always to completion, independently of whether the primary query
// reads all (or indeed any) of their output," so resolved_er/
// upsert_inv/hist_obs/etc. all still run even though nothing here
// selects from them.
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
		// DeleteAliases/ReplaceAliases are not yet folded in here --
		// see [domain.InventoryDelta]'s doc for the target contract
		// and extensionresourcerepotest's delta alias tests, which
		// pin it down ahead of this landing.
		for _, a := range d.UpsertAliases {
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
