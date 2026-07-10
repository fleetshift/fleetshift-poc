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

func (r *ExtensionResourceRepo) UpdateType(ctx context.Context, def domain.ExtensionResourceType) error {
	snap := def.Snapshot()
	mgmtJSON, err := marshalManagementSnapshot(snap.Management)
	if err != nil {
		return fmt.Errorf("marshal management: %w", err)
	}
	var invJSON sql.NullString
	if snap.Inventory != nil {
		invJSON = sql.NullString{String: "{}", Valid: true}
	}
	res, err := r.DB.ExecContext(ctx,
		`UPDATE extension_resource_types
		 SET management = $1, inventory = $2, updated_at = $3
		 WHERE service_name = $4 AND type_name = $5`,
		nullStringFromBytes(mgmtJSON),
		invJSON,
		snap.UpdatedAt.UTC(),
		string(snap.ResourceType.ServiceName()), snap.ResourceType.TypeName())
	if err != nil {
		return fmt.Errorf("update extension resource type: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: resource type %q", domain.ErrNotFound, snap.ResourceType)
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
// [domain.InventoryReplacement.Aliases]'s doc. Postgres stores that
// payload as a JSONB object for SQL-native delta merging, but read
// paths decode it back into [domain.ExtensionResource.ReportedAliases].
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
	reportedAliases, err := unmarshalReportedAliasesPayload([]byte(reportedAliasesStr))
	if err != nil {
		return snap, fmt.Errorf("unmarshal reported aliases: %w", err)
	}
	snap.ReportedAliases = reportedAliases

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

	return extensionResourceViewFromColumns(
		uid, serviceName, typeName, collectionName, resourceID, labelsStr, reportedAliasesStr,
		erCreatedAt, erUpdatedAt,
		currentVersion, managedFID,
		riSpec, riCreatedAt,
		fID, msVer, msSpec, psVer, psSpec, rsVer, rsSpec,
		rtJSON, stateStr, pauseReason, statusReason, authJSON, provJSON, attestRefJSON,
		generation, observedGeneration, activeWorkflowGen,
		fCreatedAt, fUpdatedAt,
		invLabels, invObservation, invObservedAt, invUpdatedAt,
		invConditionsJSON,
	)
}

// extensionResourceViewFromColumns builds a [domain.ExtensionResourceView]
// from erViewQueryPG's already-scanned column values. Factored out of
// scanExtensionResourceView so the query repository's extension-only
// projection (query_repo.go) can reuse the exact same construction
// logic against a row it scanned itself, without hydrating each
// result with a follow-up per-row GetView call.
func extensionResourceViewFromColumns(
	uid domain.ExtensionResourceUID,
	serviceName, typeName, collectionName, resourceID string,
	labelsStr, reportedAliasesStr string,
	erCreatedAt, erUpdatedAt time.Time,
	currentVersion sql.NullInt64,
	managedFID sql.NullString,
	riSpec, riCreatedAt sql.NullString,
	fID sql.NullString, msVer sql.NullInt64, msSpec sql.NullString,
	psVer sql.NullInt64, psSpec sql.NullString,
	rsVer sql.NullInt64, rsSpec sql.NullString,
	rtJSON, stateStr, pauseReason, statusReason, authJSON, provJSON, attestRefJSON sql.NullString,
	generation, observedGeneration, activeWorkflowGen sql.NullInt64,
	fCreatedAt, fUpdatedAt sql.NullString,
	invLabels, invObservation sql.NullString,
	invObservedAt, invUpdatedAt *time.Time,
	invConditionsJSON sql.NullString,
) (domain.ExtensionResourceView, error) {
	var v domain.ExtensionResourceView

	var labels map[string]string
	if err := json.Unmarshal([]byte(labelsStr), &labels); err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("unmarshal labels: %w", err)
	}
	reportedAliases, err := unmarshalReportedAliasesPayload([]byte(reportedAliasesStr))
	if err != nil {
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
// trip for their *entire* input slice, regardless of batch size: a
// single CTE-chained statement (replaceInventorySQL/applyInventoryDeltasSQL
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
// ApplyInventoryDeltas's ReplaceLabels/ReplaceConditions use the same
// whole-column assign; DeleteLabels and UpsertConditions/
// DeleteConditions map onto the `-` (key-removal) and `||` (merge)
// jsonb operators when replace is absent.
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
// ReplaceInventory stores the reported set as this backend's JSONB
// object payload, skipping the write entirely when that payload is
// unchanged. ApplyInventoryDeltas's UpsertAliases uses the same object
// shape so it can merge in SQL with JSONB `||`, avoiding a separate
// current-alias read and Go-side merge.

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

// reportedAliasObjectPayload encodes an extension resource's pending
// aliases in the Postgres storage shape: a JSON object keyed by the
// JSON-encoded [namespace, key] pair, with the alias value as the JSON
// string value. The repository derives this from the domain's
// canonical [domain.AliasSet] snapshot rather than storing any
// repository-local JSON metadata on the domain object itself.
// repository-local shape exists so ApplyInventoryDeltas can merge
// UpsertAliases with a single JSONB `||` in SQL instead of doing a
// read/merge/write round trip in Go.
func reportedAliasObjectPayload(aliases domain.AliasSet) ([]byte, error) {
	payload := make(map[string]string, aliases.Len())
	for alias := range aliases.All() {
		key, err := reportedAliasObjectKey(alias.Namespace(), alias.Key())
		if err != nil {
			return nil, err
		}
		payload[key] = string(alias.Value())
	}
	return json.Marshal(payload)
}

func reportedAliasObjectKey(namespace domain.AliasNamespace, key domain.AliasKey) (string, error) {
	encoded, err := json.Marshal([2]string{string(namespace), string(key)})
	if err != nil {
		return "", fmt.Errorf("marshal alias object key: %w", err)
	}
	return string(encoded), nil
}

func unmarshalReportedAliasesPayload(payload []byte) (domain.AliasSetSnapshot, error) {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 || bytes.Equal(payload, []byte("null")) {
		return domain.AliasSetSnapshot{}, nil
	}
	if payload[0] != '{' {
		return domain.AliasSetSnapshot{}, fmt.Errorf("reported aliases payload must be JSON object")
	}
	var encoded map[string]string
	if err := json.Unmarshal(payload, &encoded); err != nil {
		return nil, err
	}
	aliases := make(domain.AliasSetSnapshot, 0, len(encoded))
	for encodedKey, value := range encoded {
		var parts [2]string
		if err := json.Unmarshal([]byte(encodedKey), &parts); err != nil {
			return nil, fmt.Errorf("unmarshal alias object key %q: %w", encodedKey, err)
		}
		aliases = append(aliases, domain.AliasSnapshot{
			Namespace: domain.AliasNamespace(parts[0]),
			Key:       domain.AliasKey(parts[1]),
			Value:     domain.AliasValue(value),
		})
	}
	return aliases, nil
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
// updated_alias_payloads excludes rows already handled by resolved_er
// (a genuinely new resource): resolved_er's own INSERT already wrote
// the correct reported_aliases at creation time, and ext's LEFT JOIN
// inside er reads extension_resources as of the statement's start --
// a snapshot that, per the same CTE semantics cited above, cannot see
// rows resolved_er itself just inserted -- so without this exclusion a
// brand-new resource would look like its current payload is missing
// and needs_alias_payload_write would try to write it a second,
// redundant time.
const replaceInventorySQL = `
WITH raw_reports(idx, service_name, type_name, collection_name, resource_id, candidate_uid, observation, labels, conditions, observed_at, received_at, reported_aliases) AS MATERIALIZED (
	SELECT * FROM UNNEST($1::int[], $2::text[], $3::text[], $4::text[], $5::text[], $6::uuid[], $7::jsonb[], $8::jsonb[], $9::jsonb[], $10::timestamptz[], $11::timestamptz[], $12::jsonb[])
),
resolved_er AS (
	INSERT INTO extension_resources (uid, service_name, type_name, collection_name, resource_id, labels, reported_aliases, created_at, updated_at)
	SELECT candidate_uid, service_name, type_name, collection_name, resource_id, '{}'::jsonb, reported_aliases, received_at, received_at
	FROM raw_reports
	ON CONFLICT (service_name, collection_name, resource_id) DO NOTHING
	RETURNING uid, service_name, collection_name, resource_id
),
er AS MATERIALIZED (
	SELECT rr.idx, COALESCE(res.uid, ext.uid) AS uid,
	       rr.observation, rr.labels, rr.conditions, rr.observed_at, rr.received_at,
	       rr.reported_aliases, ext.reported_aliases AS stored_reported_aliases
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
	SELECT * FROM er WHERE reported_aliases IS DISTINCT FROM stored_reported_aliases
),
updated_alias_payloads AS (
	UPDATE extension_resources ext
	SET reported_aliases = nw.reported_aliases,
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
		aliasesJSON, err := reportedAliasObjectPayload(rep.Aliases)
		if err != nil {
			return fmt.Errorf("marshal reported aliases: %w", err)
		}
		reportedAliases[i] = string(aliasesJSON)
	}

	_, err := r.DB.ExecContext(ctx, replaceInventorySQL,
		idx, serviceNames, typeNames, collectionNames, resourceIDs, candidateUIDs,
		observations, labels, conditions, observedAts, receivedAts,
		reportedAliases,
	)
	if err != nil {
		return fmt.Errorf("replace inventory: %w", err)
	}
	return nil
}

// applyInventoryDeltasSQL implements the field-level counterpart of
// replaceInventorySQL. Labels support ReplaceLabels (whole-column
// assign when replace_labels IS NOT NULL) or DeleteLabels (jsonb key
// removal). Conditions support ReplaceConditions (whole-column assign
// when replace_conditions IS NOT NULL) or UpsertConditions/
// DeleteConditions (jsonb `-`/`||` merge). Replace and incremental
// ops on the same field are mutually exclusive
// ([domain.ValidateInventoryDelta]); Go passes NULL for an omitted
// replace so empty `{}` can still mean "replace with empty".
//
// resolved_er seeds a brand-new row with this backend's JSONB object
// alias payload: either the delta's UpsertAliases patch when the
// delta has alias work, or `{}` when it does not.
//
// updated_inv_* / new_inv replace what used to be a single upsert_inv CTE
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
// snapshot at all: the updated_inv_* branches are plain
// `UPDATE ... FROM er` statements which (per Postgres's normal
// EvalPlanQual behavior for UPDATE, the same mechanism that makes
// `UPDATE t SET n = n + 1` safe under concurrency) always merge
// against inv's own current row once its lock is acquired -- i.e.
// against whatever the other transaction just committed, not a stale
// read. The branches are mutually exclusive so each row is updated at
// most once in this statement, and so observation-only heartbeat
// deltas do not list the GIN-indexed labels/conditions columns in the
// UPDATE target list at all. That keeps the cheap heartbeat path from
// paying avoidable label/condition index-maintenance cost.
//
// ReplaceLabels/ReplaceConditions are whole-column assignments (same
// shape as ReplaceInventory), not per-key loops. Incremental
// DeleteLabels / UpsertConditions / DeleteConditions keep the jsonb
// `-`/`||` path when the corresponding replace is absent.
//
// new_inv's INSERT covers resources with no inventory row yet
// (deleting from an empty `{}` is a no-op; replace uses the provided
// JSON directly), with its own ON CONFLICT DO UPDATE as a rare
// fallback for the narrow window where a concurrent transaction
// created the row between the updated_inv_* branches running and this
// INSERT executing -- that fallback re-applies the same
// current-row-based merge/replace, using a small correlated subquery
// against er (scoped to this one row's own uid via EXCLUDED, not a
// scan of the whole batch) since ON CONFLICT DO UPDATE can only see
// EXCLUDED and the target row, not other CTEs directly.
// This still runs for every delta in the batch, including one with no
// label/condition/observation change at all, which is what gives a
// heartbeat delta (see [domain.InventoryDelta]'s doc) its "still bumps
// freshness" behavior.
//
// updated_alias_payloads folds UpsertAliases into the same statement
// as the rest of the delta. The JSONB object shape makes the merge a
// plain `reported_aliases || upsert_aliases`; Postgres re-evaluates
// that expression against the current row after waiting on any
// concurrent updater, so sibling alias deltas compose instead of
// losing one writer's value. The WHERE clause also skips unchanged
// alias upserts, keeping extension_resources.updated_at stable when
// the reported alias object is already identical.
const applyInventoryDeltasSQL = `
WITH input_er(idx, service_name, type_name, collection_name, resource_id, candidate_uid, observation, replace_labels, delete_labels, replace_conditions, upsert_conditions, delete_conditions, observed_at, received_at, upsert_aliases, has_alias_work) AS MATERIALIZED (
	SELECT * FROM UNNEST($1::int[], $2::text[], $3::text[], $4::text[], $5::text[], $6::uuid[], $7::jsonb[], $8::jsonb[], $9::jsonb[], $10::jsonb[], $11::jsonb[], $12::jsonb[], $13::timestamptz[], $14::timestamptz[], $15::jsonb[], $16::boolean[])
),
resolved_er AS (
	INSERT INTO extension_resources (uid, service_name, type_name, collection_name, resource_id, labels, reported_aliases, created_at, updated_at)
	SELECT candidate_uid, service_name, type_name, collection_name, resource_id, '{}'::jsonb,
	       CASE WHEN has_alias_work THEN upsert_aliases ELSE '{}'::jsonb END,
	       received_at, received_at
	FROM input_er
	ON CONFLICT (service_name, collection_name, resource_id) DO NOTHING
	RETURNING uid, service_name, collection_name, resource_id
),
er AS MATERIALIZED (
	SELECT i.idx, COALESCE(res.uid, ext.uid) AS uid,
	       i.observation, i.replace_labels, i.delete_labels, i.replace_conditions, i.upsert_conditions, i.delete_conditions,
	       i.observed_at, i.received_at, i.upsert_aliases, i.has_alias_work,
	       (i.replace_labels IS NOT NULL OR i.delete_labels <> '[]'::jsonb) AS has_label_work,
	       (i.replace_conditions IS NOT NULL OR i.upsert_conditions <> '{}'::jsonb OR i.delete_conditions <> '[]'::jsonb) AS has_condition_work
	FROM input_er i
	LEFT JOIN resolved_er res
	  ON res.service_name = i.service_name AND res.collection_name = i.collection_name AND res.resource_id = i.resource_id
	LEFT JOIN extension_resources ext
	  ON ext.service_name = i.service_name AND ext.collection_name = i.collection_name AND ext.resource_id = i.resource_id
),
updated_inv_base AS (
	UPDATE extension_resource_inventory inv
	SET
		observation = COALESCE(e.observation, inv.observation),
		observed_at = e.observed_at,
		updated_at = e.received_at
	FROM er e
	WHERE inv.extension_resource_uid = e.uid
	  AND NOT e.has_label_work
	  AND NOT e.has_condition_work
	RETURNING inv.extension_resource_uid
),
updated_inv_labels AS (
	UPDATE extension_resource_inventory inv
	SET
		observation = COALESCE(e.observation, inv.observation),
		labels = CASE
			WHEN e.replace_labels IS NOT NULL THEN e.replace_labels
			ELSE inv.labels - ARRAY(SELECT jsonb_array_elements_text(e.delete_labels))
		END,
		observed_at = e.observed_at,
		updated_at = e.received_at
	FROM er e
	WHERE inv.extension_resource_uid = e.uid
	  AND e.has_label_work
	  AND NOT e.has_condition_work
	RETURNING inv.extension_resource_uid
),
updated_inv_conditions AS (
	UPDATE extension_resource_inventory inv
	SET
		observation = COALESCE(e.observation, inv.observation),
		conditions = CASE
			WHEN e.replace_conditions IS NOT NULL THEN e.replace_conditions
			ELSE (inv.conditions - ARRAY(SELECT jsonb_array_elements_text(e.delete_conditions))) || e.upsert_conditions
		END,
		observed_at = e.observed_at,
		updated_at = e.received_at
	FROM er e
	WHERE inv.extension_resource_uid = e.uid
	  AND NOT e.has_label_work
	  AND e.has_condition_work
	RETURNING inv.extension_resource_uid
),
updated_inv_labels_conditions AS (
	UPDATE extension_resource_inventory inv
	SET
		observation = COALESCE(e.observation, inv.observation),
		labels = CASE
			WHEN e.replace_labels IS NOT NULL THEN e.replace_labels
			ELSE inv.labels - ARRAY(SELECT jsonb_array_elements_text(e.delete_labels))
		END,
		conditions = CASE
			WHEN e.replace_conditions IS NOT NULL THEN e.replace_conditions
			ELSE (inv.conditions - ARRAY(SELECT jsonb_array_elements_text(e.delete_conditions))) || e.upsert_conditions
		END,
		observed_at = e.observed_at,
		updated_at = e.received_at
	FROM er e
	WHERE inv.extension_resource_uid = e.uid
	  AND e.has_label_work
	  AND e.has_condition_work
	RETURNING inv.extension_resource_uid
),
updated_inv AS (
	SELECT extension_resource_uid FROM updated_inv_base
	UNION ALL
	SELECT extension_resource_uid FROM updated_inv_labels
	UNION ALL
	SELECT extension_resource_uid FROM updated_inv_conditions
	UNION ALL
	SELECT extension_resource_uid FROM updated_inv_labels_conditions
),
new_inv AS (
	INSERT INTO extension_resource_inventory (extension_resource_uid, observation, labels, conditions, observed_at, updated_at)
	SELECT e.uid, e.observation,
	       COALESCE(e.replace_labels, '{}'::jsonb),
	       COALESCE(e.replace_conditions, e.upsert_conditions),
	       e.observed_at, e.received_at
	FROM er e
	WHERE NOT EXISTS (SELECT 1 FROM updated_inv u WHERE u.extension_resource_uid = e.uid)
	ON CONFLICT (extension_resource_uid) DO UPDATE SET
		observation = COALESCE(EXCLUDED.observation, extension_resource_inventory.observation),
		labels = CASE
			WHEN (SELECT e2.replace_labels FROM er e2 WHERE e2.uid = EXCLUDED.extension_resource_uid) IS NOT NULL
				THEN (SELECT e2.replace_labels FROM er e2 WHERE e2.uid = EXCLUDED.extension_resource_uid)
			ELSE extension_resource_inventory.labels - ARRAY(
				SELECT jsonb_array_elements_text(e2.delete_labels) FROM er e2 WHERE e2.uid = EXCLUDED.extension_resource_uid
			)
		END,
		conditions = CASE
			WHEN (SELECT e3.replace_conditions FROM er e3 WHERE e3.uid = EXCLUDED.extension_resource_uid) IS NOT NULL
				THEN (SELECT e3.replace_conditions FROM er e3 WHERE e3.uid = EXCLUDED.extension_resource_uid)
			ELSE (extension_resource_inventory.conditions - ARRAY(
				SELECT jsonb_array_elements_text(e3.delete_conditions) FROM er e3 WHERE e3.uid = EXCLUDED.extension_resource_uid
			)) || EXCLUDED.conditions
		END,
		observed_at = EXCLUDED.observed_at,
		updated_at = EXCLUDED.updated_at
	RETURNING 1
),
updated_alias_payloads AS (
	UPDATE extension_resources ext
	SET reported_aliases = ext.reported_aliases || e.upsert_aliases,
	    updated_at = e.received_at
	FROM er e
	WHERE ext.uid = e.uid
	  AND e.has_alias_work
	  AND NOT EXISTS (SELECT 1 FROM resolved_er res WHERE res.uid = e.uid)
	  AND ext.reported_aliases IS DISTINCT FROM (ext.reported_aliases || e.upsert_aliases)
	RETURNING 1
)
SELECT 1`

// ApplyInventoryDeltas implements [domain.ExtensionResourceRepository.ApplyInventoryDeltas]
// in one statement against Postgres. ReplaceLabels/ReplaceConditions
// are whole-column JSONB assignments; DeleteLabels and
// UpsertConditions/DeleteConditions merge inside SQL against each
// row's current state, so concurrent incremental writers compose
// through Postgres row locking and EvalPlanQual re-evaluation instead
// of through a Go-side read-modify-write.
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
	replaceLabels := make([]*string, n)
	deleteLabels := make([]string, n)
	replaceConditions := make([]*string, n)
	upsertConditions := make([]string, n)
	deleteConditions := make([]string, n)
	observedAts := make([]time.Time, n)
	receivedAts := make([]time.Time, n)
	upsertAliases := make([]string, n)
	hasAliasWork := make([]bool, n)

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

		if d.ReplaceLabels != nil {
			replaceLabelsJSON, err := json.Marshal(nonNilLabels(d.ReplaceLabels))
			if err != nil {
				return fmt.Errorf("marshal replace labels: %w", err)
			}
			s := string(replaceLabelsJSON)
			replaceLabels[i] = &s
		}

		deleteLabelsJSON, err := json.Marshal(nonNilStrings(d.DeleteLabels))
		if err != nil {
			return fmt.Errorf("marshal delete labels: %w", err)
		}
		deleteLabels[i] = string(deleteLabelsJSON)

		if d.ReplaceConditions != nil {
			replaceConditionsJSON, err := conditionsToJSON(d.ReplaceConditions)
			if err != nil {
				return fmt.Errorf("marshal replace conditions: %w", err)
			}
			s := string(replaceConditionsJSON)
			replaceConditions[i] = &s
		}

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

		aliasesJSON, err := reportedAliasObjectPayload(d.UpsertAliases)
		if err != nil {
			return fmt.Errorf("marshal upsert aliases: %w", err)
		}
		upsertAliases[i] = string(aliasesJSON)
		if d.UpsertAliases.Len() > 0 {
			hasAliasWork[i] = true
		}
	}

	_, err := r.DB.ExecContext(ctx, applyInventoryDeltasSQL,
		idx, serviceNames, typeNames, collectionNames, resourceIDs, candidateUIDs,
		observations, replaceLabels, deleteLabels, replaceConditions, upsertConditions, deleteConditions,
		observedAts, receivedAts,
		upsertAliases, hasAliasWork,
	)
	if err != nil {
		return fmt.Errorf("apply inventory deltas: %w", err)
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
