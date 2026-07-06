package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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

	labelsJSON, err := json.Marshal(nonNilLabels(snap.Labels))
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
// held by the platform itself via platform_owned -- see
// the migration's doc comment), so unlike resource_intents/
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
//
// Note that inventory reporting (ReplaceInventory/ApplyInventoryDeltas)
// no longer populates resource_alias_claims/resource_alias_contributions
// at all (see those tables' own doc comments), so in practice this
// cleanup only ever has work to do for platform-owned claims added
// via [ResourceIdentityRepository]'s AddAlias path -- kept anyway
// since that path remains reachable and this repository has no way to
// know which mechanism produced a given claim.
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

// erSelectColumns/erViewQueryPG read labels and conditions straight
// off extension_resource_inventory's own JSONB columns (see that
// table's migration doc comment for why they're JSONB rather than
// normalized out into their own tables): a resource with no inventory
// row at all produces SQL NULL for every inv.* column via the LEFT
// JOIN below, which the scan helpers already treat as "no inventory".
//
// er.reported_aliases is this extension resource's own pending,
// unreconciled alias payload -- see
// [domain.InventoryReplacement.Aliases]'s doc. Read paths expose the
// payload itself via [domain.ExtensionResource.ReportedAliases];
// alias_fingerprint remains repository-private.
const erSelectColumns = `SELECT er.uid, er.service_name, er.type_name, er.collection_name, er.resource_id, er.labels, er.reported_aliases, er.created_at, er.updated_at,
	erm.current_version, erm.fulfillment_id,
	inv.labels, inv.observation, inv.observed_at, inv.updated_at, inv.conditions
`

var erViewQueryPG = `SELECT
	er.uid, er.service_name, er.type_name, er.collection_name, er.resource_id, er.labels, er.reported_aliases, er.created_at, er.updated_at,
	erm.current_version, erm.fulfillment_id,
	ri.spec, ri.created_at,
	` + fulfillmentColumnsJoined("f") + `,
	inv.labels, inv.observation, inv.observed_at, inv.updated_at, inv.conditions
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
	var serviceName, typeName, collectionName, resourceID, labelsStr, reportedAliasesStr string
	var currentVersion sql.NullInt64
	var fulfillmentID sql.NullString
	var invLabels, invObservation sql.NullString
	var invObservedAt, invUpdatedAt *time.Time
	var invConditionsJSON sql.NullString

	if err := s.Scan(
		&snap.UID, &serviceName, &typeName, &collectionName, &resourceID, &labelsStr, &reportedAliasesStr,
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
	if err := json.Unmarshal([]byte(reportedAliasesStr), &snap.ReportedAliases); err != nil {
		return snap, fmt.Errorf("unmarshal reported aliases: %w", err)
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
	var labelsStr, reportedAliasesStr string
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
		&uid, &serviceName, &typeName, &collectionName, &resourceID, &labelsStr, &reportedAliasesStr,
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
	var reportedAliases []domain.Alias
	if err := json.Unmarshal([]byte(reportedAliasesStr), &reportedAliases); err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("unmarshal reported aliases: %w", err)
	}

	resourceType := domain.ResourceType(serviceName + "/" + typeName)
	name := domain.ResourceName(collectionName + "/" + resourceID)

	erSnap := domain.ExtensionResourceSnapshot{
		UID:             uid,
		ResourceType:    resourceType,
		Name:            name,
		Labels:          labels,
		CreatedAt:       erCreatedAt,
		UpdatedAt:       erUpdatedAt,
		ReportedAliases: reportedAliases,
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

// ConditionJSON is the JSON shape of a single entry within
// extension_resource_inventory.conditions, which stores a *map* of
// these keyed by condition type rather than an array -- see that
// column's migration doc comment for why. LastTransitionTime
// round-trips through Postgres's timestamptz column and
// encoding/json's default RFC3339Nano time.Time marshaling.
type ConditionJSON struct {
	Status             domain.ConditionStatus `json:"status"`
	Reason             string                 `json:"reason"`
	Message            string                 `json:"message"`
	LastTransitionTime time.Time              `json:"lastTransitionTime"`
}

// conditionsToJSON marshals conds into the JSON object -- keyed by
// condition type -- that extension_resource_inventory.conditions
// stores. A nil or empty conds still marshals to `{}`, never `null`,
// since the column is NOT NULL DEFAULT '{}'.
func conditionsToJSON(conds []domain.Condition) ([]byte, error) {
	byType := make(map[string]ConditionJSON, len(conds))
	for _, c := range conds {
		byType[string(c.Type())] = ConditionJSON{
			Status:             c.Status(),
			Reason:             c.Reason(),
			Message:            c.Message(),
			LastTransitionTime: c.LastTransitionTime(),
		}
	}
	return json.Marshal(byType)
}

// conditionSnapshotsToJSON is conditionsToJSON's
// [domain.ConditionSnapshot] counterpart, used by
// [ExtensionResourceRepo.insertInventory] which works with snapshot
// DTOs rather than the [domain.Condition] value objects
// ReplaceInventory/ApplyInventoryDeltas receive.
func conditionSnapshotsToJSON(conds []domain.ConditionSnapshot) ([]byte, error) {
	byType := make(map[string]ConditionJSON, len(conds))
	for _, c := range conds {
		byType[string(c.Type)] = ConditionJSON{
			Status:             c.Status,
			Reason:             c.Reason,
			Message:            c.Message,
			LastTransitionTime: c.LastTransitionTime,
		}
	}
	return json.Marshal(byType)
}

// unmarshalConditionSnapshots parses the JSON object produced by
// conditionsToJSON back into [domain.ConditionSnapshot]s, sorted by
// type for deterministic ordering (map iteration order is otherwise
// unspecified).
func unmarshalConditionSnapshots(data []byte) ([]domain.ConditionSnapshot, error) {
	var byType map[string]ConditionJSON
	if err := json.Unmarshal(data, &byType); err != nil {
		return nil, err
	}
	snaps := make([]domain.ConditionSnapshot, 0, len(byType))
	for t, c := range byType {
		snaps = append(snaps, domain.ConditionSnapshot{
			Type:               domain.ConditionType(t),
			Status:             c.Status,
			Reason:             c.Reason,
			Message:            c.Message,
			LastTransitionTime: c.LastTransitionTime.UTC(),
		})
	}
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].Type < snaps[j].Type })
	return snaps, nil
}

// nonNilLabels normalizes a nil label map to a non-nil empty one, so
// json.Marshal produces `{}` rather than `null` -- the labels
// columns this feeds are NOT NULL DEFAULT '{}'.
func nonNilLabels(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// nonNilStrings normalizes a nil string slice to a non-nil empty one,
// so json.Marshal produces `[]` rather than `null`.
func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// ---------------------------------------------------------------------------
// Inventory methods
// ---------------------------------------------------------------------------
//
// ReplaceInventory and ApplyInventoryDeltas are each exactly one round
// trip for their *entire* input slice, regardless of batch size (plus,
// for ApplyInventoryDeltas, a second round trip only when at least one
// delta in the batch actually upserts aliases -- see
// applyInventoryDeltasAliasPayloadUpdateSQL's doc comment): a single
// CTE-chained statement (replaceInventorySQL/applyInventoryDeltasSQL
// below) that resolves-or-creates every replacement/delta's
// extension_resources row by natural key (service_name,
// collection_name, resource_id) -- the input_er/resolved_er/er CTEs --
// and then has every other write join that resolution by natural key
// instead of depending on a UID resolved by the caller ahead of time.
//
// labels and conditions are JSONB (see the migration's doc comment on
// extension_resource_inventory), so ReplaceInventory's
// complete-latest-state contract is a single column assignment --
// no delete-absent/upsert pair against a normalized table is needed.
// ApplyInventoryDeltas's field-level set/upsert-plus-delete semantics
// map directly onto the `-` (key-removal) and `||` (merge) jsonb
// operators.
//
// Neither statement writes observation/condition history any more:
// extension_resource_inventory_observations/
// extension_resource_inventory_condition_events are populated by a
// future asynchronous writer, not this hot path -- see those tables'
// own migration doc comments.
//
// Aliases are handled with no synchronous cross-resource conflict
// detection at all -- see [domain.InventoryReplacement.Aliases]'s doc
// -- so there is nothing for either method to report beyond error.
// ReplaceInventory stores the reported set verbatim, skipping the
// write entirely when [domain.AliasSetFingerprint] matches what's
// already stored (needs_alias_payload_write below). ApplyInventoryDeltas's
// UpsertAliases instead merges into the existing payload by
// (namespace, key) -- a genuine read-modify-write that can't be
// pushed into the main statement without knowing the *result* of the
// merge ahead of time, so it becomes a second, smaller round trip
// scoped to only the resources with alias work (see
// applyInventoryDeltasSQL's trailing SELECT and
// applyInventoryDeltasAliasPayloadUpdateSQL).

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

// insertInventory writes a resource's initial inventory state as part
// of [ExtensionResourceRepo.Create]. There's nothing pre-existing to
// reconcile against (the resource itself was just created in the same
// call), so this is a plain INSERT rather than the merged
// ReplaceInventory/ApplyInventoryDeltas statement's upsert.
func (r *ExtensionResourceRepo) insertInventory(ctx context.Context, uid domain.ExtensionResourceUID, inv *domain.InventoryResourceSnapshot) error {
	labelsJSON, err := json.Marshal(nonNilLabels(inv.Labels))
	if err != nil {
		return fmt.Errorf("marshal inventory labels: %w", err)
	}
	conditionsJSON, err := conditionSnapshotsToJSON(inv.Conditions)
	if err != nil {
		return fmt.Errorf("marshal inventory conditions: %w", err)
	}
	var obsArg sql.NullString
	if inv.Observation != nil {
		obsArg = sql.NullString{String: string(inv.Observation), Valid: true}
	}
	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_inventory (extension_resource_uid, observation, labels, conditions, observed_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		uid.String(), obsArg, string(labelsJSON), string(conditionsJSON), inv.ObservedAt.UTC(), inv.UpdatedAt.UTC())
	return err
}

// replaceInventorySQL implements [ExtensionResourceRepository.ReplaceInventory]
// as a single statement for the entire batch.
//
// raw_reports/er are MATERIALIZED since each is read by more than one
// downstream CTE and UNNEST's output is cheap to compute once and
// reuse rather than risk Postgres inlining (and thus recomputing) it
// at each reference.
//
// upsert_inv and updated_alias_payloads are both data-modifying CTEs
// that the trailing `SELECT 1` never reads from -- this is
// intentional, not an oversight: Postgres executes every
// data-modifying WITH-clause entry to completion exactly once,
// independently of whether the primary query reads all (or indeed
// any) of its output, so this has no dummy RETURNING/aggregation to
// thread through purely to satisfy the top-level SELECT. See
// https://www.postgresql.org/docs/current/queries-with.html's
// "Data-Modifying Statements in WITH".
//
// needs_alias_payload_write's IS DISTINCT FROM treats a NULL stored
// fingerprint (a resource that has never had a successful alias
// payload write) as always distinct from any concrete reported hash,
// so a resource's first report always writes its alias payload
// regardless of whether it reports any aliases at all.
//
// updated_alias_payloads excludes rows already handled by resolved_er
// (a genuinely new resource): resolved_er's own INSERT already wrote
// the correct alias_fingerprint/reported_aliases at creation time, and
// ext's LEFT JOIN inside er reads extension_resources as of the
// statement's start -- a snapshot that, per the same CTE semantics
// cited above, cannot see rows resolved_er itself just inserted --
// so without this exclusion a brand-new resource's own fingerprint
// would look NULL (not yet stored) and needs_alias_payload_write
// would try to write it a second, redundant time.
const replaceInventorySQL = `
WITH raw_reports(idx, service_name, type_name, collection_name, resource_id, candidate_uid, observation, labels, conditions, observed_at, received_at, reported_alias_fingerprint, reported_aliases) AS MATERIALIZED (
	SELECT * FROM UNNEST($1::int[], $2::text[], $3::text[], $4::text[], $5::text[], $6::uuid[], $7::jsonb[], $8::jsonb[], $9::jsonb[], $10::timestamptz[], $11::timestamptz[], $12::bytea[], $13::jsonb[])
),
resolved_er AS (
	INSERT INTO extension_resources (uid, service_name, type_name, collection_name, resource_id, labels, alias_fingerprint, reported_aliases, created_at, updated_at)
	SELECT candidate_uid, service_name, type_name, collection_name, resource_id, '{}'::jsonb, reported_alias_fingerprint, reported_aliases, received_at, received_at
	FROM raw_reports
	ON CONFLICT (service_name, collection_name, resource_id) DO NOTHING
	RETURNING uid, service_name, collection_name, resource_id
),
er AS MATERIALIZED (
	SELECT rr.idx, COALESCE(res.uid, ext.uid) AS uid,
	       rr.observation, rr.labels, rr.conditions, rr.observed_at, rr.received_at,
	       rr.reported_alias_fingerprint, rr.reported_aliases,
	       ext.alias_fingerprint AS stored_alias_fingerprint
	FROM raw_reports rr
	LEFT JOIN resolved_er res
	  ON res.service_name = rr.service_name AND res.collection_name = rr.collection_name AND res.resource_id = rr.resource_id
	LEFT JOIN extension_resources ext
	  ON ext.service_name = rr.service_name AND ext.collection_name = rr.collection_name AND ext.resource_id = rr.resource_id
),
upsert_inv AS (
	INSERT INTO extension_resource_inventory (extension_resource_uid, observation, labels, conditions, observed_at, updated_at)
	SELECT uid, observation, labels, conditions, observed_at, received_at FROM er
	ON CONFLICT (extension_resource_uid) DO UPDATE SET
		observation = COALESCE(EXCLUDED.observation, extension_resource_inventory.observation),
		labels = EXCLUDED.labels,
		conditions = EXCLUDED.conditions,
		observed_at = EXCLUDED.observed_at,
		updated_at = EXCLUDED.updated_at
	RETURNING 1
),
needs_alias_payload_write AS (
	SELECT * FROM er WHERE reported_alias_fingerprint IS DISTINCT FROM stored_alias_fingerprint
),
updated_alias_payloads AS (
	UPDATE extension_resources ext
	SET alias_fingerprint = nw.reported_alias_fingerprint,
	    reported_aliases = nw.reported_aliases,
	    updated_at = nw.received_at
	FROM needs_alias_payload_write nw
	WHERE ext.uid = nw.uid
	  AND NOT EXISTS (SELECT 1 FROM resolved_er res WHERE res.uid = nw.uid)
	RETURNING 1
)
SELECT 1`

func (r *ExtensionResourceRepo) ReplaceInventory(ctx context.Context, replacements []domain.InventoryReplacement) error {
	if len(replacements) == 0 {
		return nil
	}

	n := len(replacements)
	idx := make([]int32, n)
	serviceNames := make([]string, n)
	typeNames := make([]string, n)
	collectionNames := make([]string, n)
	resourceIDs := make([]string, n)
	candidateUIDs := make([]string, n)
	observations := make([]*string, n)
	labels := make([]string, n)
	conditions := make([]string, n)
	observedAts := make([]time.Time, n)
	receivedAts := make([]time.Time, n)
	aliasFingerprints := make([][]byte, n)
	reportedAliases := make([]string, n)

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
		labelsJSON, err := json.Marshal(nonNilLabels(rep.Labels))
		if err != nil {
			return fmt.Errorf("marshal labels: %w", err)
		}
		labels[i] = string(labelsJSON)
		conditionsJSON, err := conditionsToJSON(rep.Conditions)
		if err != nil {
			return fmt.Errorf("marshal conditions: %w", err)
		}
		conditions[i] = string(conditionsJSON)
		observedAts[i] = rep.ObservedAt.UTC()
		receivedAts[i] = rep.ReceivedAt.UTC()
		aliasFingerprints[i] = domain.AliasSetFingerprint(rep.Aliases)
		aliasesJSON, err := domain.CanonicalAliasPayload(rep.Aliases)
		if err != nil {
			return fmt.Errorf("marshal reported aliases: %w", err)
		}
		reportedAliases[i] = string(aliasesJSON)
	}

	_, err := r.DB.ExecContext(ctx, replaceInventorySQL,
		idx, serviceNames, typeNames, collectionNames, resourceIDs, candidateUIDs,
		observations, labels, conditions, observedAts, receivedAts,
		aliasFingerprints, reportedAliases,
	)
	if err != nil {
		return fmt.Errorf("replace inventory: %w", err)
	}
	return nil
}

// applyInventoryDeltasSQL implements the field-level counterpart of
// replaceInventorySQL: set_labels/delete_labels/upsert_conditions/
// delete_conditions carry one JSON value *per delta* (a JSON
// object/array, empty when the delta doesn't touch that field) rather
// than one flattened row per key, since Go already has the complete
// per-delta shape in memory and building it once there is simpler
// than an extra per-key UNNEST input CTE.
//
// resolved_er seeds a brand-new row's alias_fingerprint with $14, the
// empty-set fingerprint ([domain.AliasSetFingerprint] of nil) computed
// once in Go, rather than leaving the column NULL -- see
// [ExtensionResourceRepo.ApplyInventoryDeltas]'s doc for why: it keeps
// this backend consistent with sqlite's resolveOrCreateExtensionResources,
// which seeds the same concrete value, and avoids a spurious
// IS DISTINCT FROM NULL mismatch that would otherwise force a
// redundant alias payload rewrite the very next time this resource is
// reported with (still) no aliases.
//
// updated_inv/new_inv replace what used to be a single upsert_inv CTE
// that merged each delta against a *pre-read* prev_inv snapshot (taken
// once, at this statement's start) and wrote the merged result via
// `ON CONFLICT DO UPDATE SET labels = EXCLUDED.labels`. That was
// unsafe under concurrent writers: if two ApplyInventoryDeltas calls
// touching different keys on the same resource overlap, the second to
// reach the conflict has to wait for the first to commit, but then
// applies `EXCLUDED.labels` -- a value fixed by its own SELECT before
// it ever waited, still computed against the pre-first-commit
// snapshot -- clobbering the first transaction's change instead of
// composing with it. The fix is to never precompute the merge from a
// snapshot at all: updated_inv is a plain `UPDATE ... FROM er`, which
// (per Postgres's normal EvalPlanQual behavior for UPDATE, the same
// mechanism that makes `UPDATE t SET n = n + 1` safe under
// concurrency) always merges against inv's own current row once its
// lock is acquired -- i.e. against whatever the other transaction
// just committed, not a stale read. updated_inv only touches resources
// that already have an inventory row; new_inv's INSERT covers the
// rest (deleting from an empty `{}` is a no-op, so the initial value
// is just set_labels/upsert_conditions directly), with its own
// ON CONFLICT DO UPDATE as a rare fallback for the narrow window where
// a concurrent transaction created the row between updated_inv running
// and this INSERT executing -- that fallback re-applies the same
// current-row-based merge, using a small correlated subquery against
// er (scoped to this one row's own uid via EXCLUDED, not a scan of the
// whole batch) since ON CONFLICT DO UPDATE can only see EXCLUDED and
// the target row, not other CTEs directly. This still runs for every
// delta in the batch, including one with no label/condition/
// observation change at all, which is what gives a heartbeat delta
// (see [domain.InventoryDelta]'s doc) its "still bumps freshness"
// behavior.
//
// The trailing SELECT is not a result the caller cares about in the
// same sense replaceInventorySQL's is (that one returns nothing at
// all) -- it returns exactly the (idx, uid) pairs ApplyInventoryDeltas
// needs to drive its second-phase alias merge (see
// [ExtensionResourceRepo.ApplyInventoryDeltas]'s doc): one row per
// delta that actually set UpsertAliases (the $15 join). It
// deliberately does *not* also return current reported_aliases the
// way an earlier version of this query did -- see that method's doc
// for why the current-aliases read now happens in its own locked
// statement instead. A delta with no alias work contributes no row
// here at all, and the whole second phase is skipped when the result
// set is empty -- see [domain.InventoryDelta]'s doc for why omitting
// all three alias fields must do no alias work whatsoever, not even a
// no-op rewrite.
const applyInventoryDeltasSQL = `
WITH input_er(idx, service_name, type_name, collection_name, resource_id, candidate_uid, observation, set_labels, delete_labels, upsert_conditions, delete_conditions, observed_at, received_at) AS MATERIALIZED (
	SELECT * FROM UNNEST($1::int[], $2::text[], $3::text[], $4::text[], $5::text[], $6::uuid[], $7::jsonb[], $8::jsonb[], $9::jsonb[], $10::jsonb[], $11::jsonb[], $12::timestamptz[], $13::timestamptz[])
),
resolved_er AS (
	INSERT INTO extension_resources (uid, service_name, type_name, collection_name, resource_id, labels, reported_aliases, alias_fingerprint, created_at, updated_at)
	SELECT candidate_uid, service_name, type_name, collection_name, resource_id, '{}'::jsonb, '[]'::jsonb, $14::bytea, received_at, received_at
	FROM input_er
	ON CONFLICT (service_name, collection_name, resource_id) DO NOTHING
	RETURNING uid, service_name, collection_name, resource_id
),
er AS MATERIALIZED (
	SELECT i.idx, COALESCE(res.uid, ext.uid) AS uid,
	       i.observation, i.set_labels, i.delete_labels, i.upsert_conditions, i.delete_conditions,
	       i.observed_at, i.received_at
	FROM input_er i
	LEFT JOIN resolved_er res
	  ON res.service_name = i.service_name AND res.collection_name = i.collection_name AND res.resource_id = i.resource_id
	LEFT JOIN extension_resources ext
	  ON ext.service_name = i.service_name AND ext.collection_name = i.collection_name AND ext.resource_id = i.resource_id
),
updated_inv AS (
	UPDATE extension_resource_inventory inv
	SET
		observation = COALESCE(e.observation, inv.observation),
		labels = (inv.labels - ARRAY(SELECT jsonb_array_elements_text(e.delete_labels))) || e.set_labels,
		conditions = (inv.conditions - ARRAY(SELECT jsonb_array_elements_text(e.delete_conditions))) || e.upsert_conditions,
		observed_at = e.observed_at,
		updated_at = e.received_at
	FROM er e
	WHERE inv.extension_resource_uid = e.uid
	RETURNING inv.extension_resource_uid
),
new_inv AS (
	INSERT INTO extension_resource_inventory (extension_resource_uid, observation, labels, conditions, observed_at, updated_at)
	SELECT e.uid, e.observation, e.set_labels, e.upsert_conditions, e.observed_at, e.received_at
	FROM er e
	WHERE NOT EXISTS (SELECT 1 FROM updated_inv u WHERE u.extension_resource_uid = e.uid)
	ON CONFLICT (extension_resource_uid) DO UPDATE SET
		observation = COALESCE(EXCLUDED.observation, extension_resource_inventory.observation),
		labels = (extension_resource_inventory.labels - ARRAY(
			SELECT jsonb_array_elements_text(e2.delete_labels) FROM er e2 WHERE e2.uid = EXCLUDED.extension_resource_uid
		)) || EXCLUDED.labels,
		conditions = (extension_resource_inventory.conditions - ARRAY(
			SELECT jsonb_array_elements_text(e3.delete_conditions) FROM er e3 WHERE e3.uid = EXCLUDED.extension_resource_uid
		)) || EXCLUDED.conditions,
		observed_at = EXCLUDED.observed_at,
		updated_at = EXCLUDED.updated_at
	RETURNING 1
)
SELECT e.idx, e.uid
FROM er e
JOIN UNNEST($15::int[]) aw(idx) ON aw.idx = e.idx`

// applyInventoryDeltasCurrentAliasesSQL is ApplyInventoryDeltas's
// dedicated second-phase read: for exactly the uids applyInventoryDeltasSQL
// flagged as having alias work, it reads and, via FOR UPDATE, locks
// each extension_resources row's current reported_aliases until this
// call's transaction commits. Every uid passed in is guaranteed to
// already exist in extension_resources by now -- either it predates
// this batch, or applyInventoryDeltasSQL's own resolved_er just
// created it earlier in this same transaction -- so this can be a
// plain equality lookup with no outer join, unlike
// applyInventoryDeltasSQL's own trailing SELECT (which does need a
// LEFT JOIN, and so could not add FOR UPDATE to it directly: Postgres
// rejects locking clauses on the nullable side of an outer join).
// Holding the lock across the gap until applyInventoryDeltasAliasPayloadUpdateSQL
// writes is what closes the lost-update race described in
// [ExtensionResourceRepo.ApplyInventoryDeltas]'s doc: any other
// transaction trying to read or write these same rows' aliases blocks
// until this one commits, instead of interleaving with it.
const applyInventoryDeltasCurrentAliasesSQL = `
SELECT uid, reported_aliases FROM extension_resources WHERE uid = ANY($1::uuid[]) FOR UPDATE`

// applyInventoryDeltasAliasPayloadUpdateSQL is ApplyInventoryDeltas's
// third-phase batched write: one row per resource whose merged
// reported_aliases/alias_fingerprint (computed in Go by
// domain.MergeAliases against applyInventoryDeltasCurrentAliasesSQL's
// locked read) needs to be persisted. Only ever executed when that
// read returned at least one row. Relies on the caller running within
// a single transaction spanning all three phases -- true for every
// production caller (see [domain.Store.Begin]) -- so
// applyInventoryDeltasCurrentAliasesSQL's row locks are still held
// here.
const applyInventoryDeltasAliasPayloadUpdateSQL = `
UPDATE extension_resources ext
SET reported_aliases = u.reported_aliases,
    alias_fingerprint = u.alias_fingerprint,
    updated_at = u.updated_at
FROM UNNEST($1::uuid[], $2::jsonb[], $3::bytea[], $4::timestamptz[]) AS u(uid, reported_aliases, alias_fingerprint, updated_at)
WHERE ext.uid = u.uid`

// ApplyInventoryDeltas implements [domain.ExtensionResourceRepository.ApplyInventoryDeltas]
// as three phases against Postgres, run within the caller's
// transaction (see [domain.Store.Begin] -- every production caller
// wraps the whole call in one, which the phases below depend on):
//
//  1. applyInventoryDeltasSQL resolves/creates each resource and
//     merges labels/conditions directly against each row's current
//     state (not a pre-read snapshot -- see that SQL's doc comment for
//     why that distinction is what makes this safe under concurrent
//     ApplyInventoryDeltas calls touching the same resource), returning
//     which deltas have UpsertAliases work left to do.
//  2. applyInventoryDeltasCurrentAliasesSQL reads and locks (FOR
//     UPDATE) exactly those resources' current reported_aliases.
//  3. Go merges each locked row's current aliases with its delta's
//     UpsertAliases and applyInventoryDeltasAliasPayloadUpdateSQL
//     writes the result back, still holding phase 2's locks.
//
// Phases 2-3 exist as their own read-then-write pair (rather than
// folding the merge into phase 1's single statement the way labels/
// conditions do) because a plain UPDATE's EvalPlanQual re-evaluation
// -- phase 1's fix for labels/conditions -- only protects a single
// statement computing its own merge inline. The alias merge
// (domain.MergeAliases) runs in Go, not SQL, so nothing re-evaluates
// it against a fresher row if the read and write were two unlocked
// statements; explicit FOR UPDATE locking is what makes a Go-side
// read-modify-write safe across that gap instead.
func (r *ExtensionResourceRepo) ApplyInventoryDeltas(ctx context.Context, deltas []domain.InventoryDelta) error {
	if len(deltas) == 0 {
		return nil
	}
	for _, d := range deltas {
		if err := domain.ValidateInventoryDelta(d); err != nil {
			return err
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
	setLabels := make([]string, n)
	deleteLabels := make([]string, n)
	upsertConditions := make([]string, n)
	deleteConditions := make([]string, n)
	observedAts := make([]time.Time, n)
	receivedAts := make([]time.Time, n)
	var aliasWorkIdx []int32

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

		setLabelsJSON, err := json.Marshal(nonNilLabels(d.SetLabels))
		if err != nil {
			return fmt.Errorf("marshal set labels: %w", err)
		}
		setLabels[i] = string(setLabelsJSON)

		deleteLabelsJSON, err := json.Marshal(nonNilStrings(d.DeleteLabels))
		if err != nil {
			return fmt.Errorf("marshal delete labels: %w", err)
		}
		deleteLabels[i] = string(deleteLabelsJSON)

		upsertConditionsJSON, err := conditionsToJSON(d.UpsertConditions)
		if err != nil {
			return fmt.Errorf("marshal upsert conditions: %w", err)
		}
		upsertConditions[i] = string(upsertConditionsJSON)

		deleteConditionTypes := make([]string, len(d.DeleteConditions))
		for j, t := range d.DeleteConditions {
			deleteConditionTypes[j] = string(t)
		}
		deleteConditionsJSON, err := json.Marshal(deleteConditionTypes)
		if err != nil {
			return fmt.Errorf("marshal delete conditions: %w", err)
		}
		deleteConditions[i] = string(deleteConditionsJSON)

		observedAts[i] = d.ObservedAt.UTC()
		receivedAts[i] = d.ReceivedAt.UTC()

		if len(d.UpsertAliases) > 0 {
			aliasWorkIdx = append(aliasWorkIdx, int32(i))
		}
	}

	emptyAliasFingerprint := domain.AliasSetFingerprint(nil)

	rows, err := r.DB.QueryContext(ctx, applyInventoryDeltasSQL,
		idx, serviceNames, typeNames, collectionNames, resourceIDs, candidateUIDs,
		observations, setLabels, deleteLabels, upsertConditions, deleteConditions,
		observedAts, receivedAts,
		emptyAliasFingerprint,
		aliasWorkIdx,
	)
	if err != nil {
		return fmt.Errorf("apply inventory deltas: %w", err)
	}

	var workIdx []int32
	var workUIDs []domain.ExtensionResourceUID
	for rows.Next() {
		var rowIdx int32
		var uid domain.ExtensionResourceUID
		if err := rows.Scan(&rowIdx, &uid); err != nil {
			rows.Close()
			return fmt.Errorf("apply inventory deltas: scan alias work row: %w", err)
		}
		workIdx = append(workIdx, rowIdx)
		workUIDs = append(workUIDs, uid)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("apply inventory deltas: %w", err)
	}
	rows.Close()

	if len(workUIDs) == 0 {
		return nil
	}

	// Second phase: read this resource's current reported_aliases
	// under FOR UPDATE, holding the lock until the third phase's write
	// below commits -- see applyInventoryDeltasCurrentAliasesSQL's doc
	// for why this closes the lost-update race a single combined
	// read-then-write would otherwise leave open.
	workUIDStrs := make([]string, len(workUIDs))
	for i, u := range workUIDs {
		workUIDStrs[i] = u.String()
	}
	currentRows, err := r.DB.QueryContext(ctx, applyInventoryDeltasCurrentAliasesSQL, workUIDStrs)
	if err != nil {
		return fmt.Errorf("apply inventory deltas: lock current aliases: %w", err)
	}
	current := make(map[domain.ExtensionResourceUID][]domain.Alias, len(workUIDs))
	for currentRows.Next() {
		var uid domain.ExtensionResourceUID
		var currentAliasesJSON string
		if err := currentRows.Scan(&uid, &currentAliasesJSON); err != nil {
			currentRows.Close()
			return fmt.Errorf("apply inventory deltas: scan current aliases: %w", err)
		}
		var aliases []domain.Alias
		if err := json.Unmarshal([]byte(currentAliasesJSON), &aliases); err != nil {
			currentRows.Close()
			return fmt.Errorf("apply inventory deltas: unmarshal current aliases: %w", err)
		}
		current[uid] = aliases
	}
	if err := currentRows.Err(); err != nil {
		currentRows.Close()
		return fmt.Errorf("apply inventory deltas: %w", err)
	}
	currentRows.Close()

	// Third phase: merge in Go and write back while still holding the
	// locks the second phase took.
	updateUIDs := make([]string, len(workUIDs))
	updatePayloads := make([]string, len(workUIDs))
	updateFingerprints := make([][]byte, len(workUIDs))
	updateTimes := make([]time.Time, len(workUIDs))
	for i, rowIdx := range workIdx {
		uid := workUIDs[i]
		merged := domain.MergeAliases(current[uid], deltas[rowIdx].UpsertAliases)
		payload, err := domain.CanonicalAliasPayload(merged)
		if err != nil {
			return fmt.Errorf("apply inventory deltas: marshal merged aliases: %w", err)
		}
		updateUIDs[i] = uid.String()
		updatePayloads[i] = string(payload)
		updateFingerprints[i] = domain.AliasSetFingerprint(merged)
		updateTimes[i] = deltas[rowIdx].ReceivedAt.UTC()
	}
	if _, err := r.DB.ExecContext(ctx, applyInventoryDeltasAliasPayloadUpdateSQL,
		updateUIDs, updatePayloads, updateFingerprints, updateTimes,
	); err != nil {
		return fmt.Errorf("apply inventory deltas: write merged alias payloads: %w", err)
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
