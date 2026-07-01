package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

// erInstanceFromClause is the shared FROM + JOINs for instance
// aggregate reads. Callers prepend erSelectColumns and append WHERE.
const erInstanceFromClause = `
FROM extension_resources er
LEFT JOIN extension_resource_managed erm ON erm.extension_resource_uid = er.uid
LEFT JOIN extension_resource_inventory inv ON inv.extension_resource_uid = er.uid
`

func (r *ExtensionResourceRepo) Get(ctx context.Context, name domain.FullResourceName) (*domain.ExtensionResource, error) {
	row := r.DB.QueryRowContext(ctx,
		erSelectColumns+erInstanceFromClause+`WHERE er.service_name = $1 AND er.resource_name = $2`,
		string(name.ServiceName()), string(name.ResourceName()))
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
		erSelectColumns+erInstanceFromClause+`WHERE er.service_name = $1 AND er.type_name = $2 ORDER BY er.resource_name`,
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
	erm.current_version, erm.fulfillment_id,
	inv.labels, inv.observation, inv.observed_at, inv.updated_at,
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
	var invLabels, invObservation sql.NullString
	var invObservedAt, invUpdatedAt *time.Time
	var invConditionsJSON sql.NullString

	if err := s.Scan(
		&snap.UID, &serviceName, &typeName, &snap.Name, &labelsStr,
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

// upsertInventoryLatestRow is the single low-level "write latest
// inventory row" primitive shared by [ExtensionResourceRepo.Create],
// [ExtensionResourceRepo.ReplaceInventory], and
// [ExtensionResourceRepo.ApplyInventoryDeltas]. observation == nil
// means "untouched": the ON CONFLICT clause COALESCEs the observation
// column so an untouched observation preserves whatever is already
// latest, entirely at the SQL level. Callers are expected to have
// already normalized away a null-literal observation (see
// [normalizeObservation]) before calling this, since a non-nil
// pointer to the JSON literal null would otherwise be written as a
// literal jsonb null rather than preserving latest.
func (r *ExtensionResourceRepo) upsertInventoryLatestRow(
	ctx context.Context,
	uid domain.ExtensionResourceUID,
	labels map[string]string,
	observation *json.RawMessage,
	observedAt, updatedAt time.Time,
) error {
	labelsJSON, err := marshalLabels(labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}
	var obsArg sql.NullString
	if observation != nil {
		obsArg = sql.NullString{String: string(*observation), Valid: true}
	}
	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_inventory
			(extension_resource_uid, labels, observation, observed_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT(extension_resource_uid) DO UPDATE SET
			labels = EXCLUDED.labels,
			observation = COALESCE(EXCLUDED.observation, extension_resource_inventory.observation),
			observed_at = EXCLUDED.observed_at,
			updated_at = EXCLUDED.updated_at`,
		uid.String(), labelsJSON, obsArg,
		observedAt.UTC(), updatedAt.UTC())
	return err
}

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

// marshalLabels normalizes a nil label map to an empty JSON object so
// "no labels supplied" and "empty label set" both persist the same way.
func marshalLabels(labels map[string]string) (string, error) {
	if labels == nil {
		labels = map[string]string{}
	}
	b, err := json.Marshal(labels)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (r *ExtensionResourceRepo) upsertInventoryRow(ctx context.Context, uid domain.ExtensionResourceUID, inv *domain.InventoryResourceSnapshot) error {
	var obs *json.RawMessage
	if inv.Observation != nil {
		obs = &inv.Observation
	}
	return r.upsertInventoryLatestRow(ctx, uid, inv.Labels, obs, inv.ObservedAt, inv.UpdatedAt)
}

func (r *ExtensionResourceRepo) insertInventory(ctx context.Context, uid domain.ExtensionResourceUID, inv *domain.InventoryResourceSnapshot) error {
	if err := r.upsertInventoryRow(ctx, uid, inv); err != nil {
		return err
	}
	for _, c := range inv.Conditions {
		if err := r.recordCondition(ctx, uid,
			c.Type, c.Status, c.Reason, c.Message,
			c.LastTransitionTime, inv.ObservedAt, inv.UpdatedAt); err != nil {
			return err
		}
	}
	return nil
}

// ensureInventoryRowExists creates a latest-inventory row with empty
// labels and no observation if one doesn't already exist for uid. The
// INSERT enforces the extension_resources foreign key, so an unknown
// UID fails here rather than the repository silently creating an
// extension resource (the behavior the report contract rework
// removed). observedAt/updatedAt only matter for a brand-new row;
// ApplyInventoryDeltas immediately overwrites them via
// [ExtensionResourceRepo.upsertInventoryLatestRow] regardless.
func (r *ExtensionResourceRepo) ensureInventoryRowExists(ctx context.Context, uid domain.ExtensionResourceUID, observedAt, updatedAt time.Time) error {
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_inventory (extension_resource_uid, labels, observation, observed_at, updated_at)
		 VALUES ($1, '{}', NULL, $2, $3)
		 ON CONFLICT (extension_resource_uid) DO NOTHING`,
		uid.String(), observedAt.UTC(), updatedAt.UTC())
	return err
}

// currentInventoryLabels reads the current latest labels for uid, for
// the read-modify-write merge that
// [ExtensionResourceRepo.ApplyInventoryDeltas] needs. Callers must
// ensure the row exists first.
func (r *ExtensionResourceRepo) currentInventoryLabels(ctx context.Context, uid domain.ExtensionResourceUID) (map[string]string, error) {
	var labelsJSON string
	if err := r.DB.QueryRowContext(ctx,
		`SELECT labels FROM extension_resource_inventory WHERE extension_resource_uid = $1`,
		uid.String()).Scan(&labelsJSON); err != nil {
		return nil, fmt.Errorf("read latest inventory labels for %s: %w", uid, err)
	}
	labels := map[string]string{}
	if labelsJSON != "" {
		if err := json.Unmarshal([]byte(labelsJSON), &labels); err != nil {
			return nil, fmt.Errorf("unmarshal labels: %w", err)
		}
	}
	return labels, nil
}

// observationDiffersFromLatest reports whether obs (already
// normalized via [normalizeObservation]) is a real value that differs
// from the current latest observation for uid, i.e. whether it should
// append a new observation history row. A nil obs always reports
// false without reading anything. The comparison happens entirely in
// SQL via jsonb's IS DISTINCT FROM, using Postgres's native JSONB
// semantic equality (insensitive to key order and whitespace) rather
// than a Go-side byte comparison -- important since JSONB storage
// doesn't preserve the original textual form.
func (r *ExtensionResourceRepo) observationDiffersFromLatest(ctx context.Context, uid domain.ExtensionResourceUID, obs *json.RawMessage) (bool, error) {
	if obs == nil {
		return false, nil
	}
	var differs bool
	err := r.DB.QueryRowContext(ctx,
		`SELECT observation IS DISTINCT FROM $2::jsonb
		 FROM extension_resource_inventory WHERE extension_resource_uid = $1`,
		uid.String(), string(*obs)).Scan(&differs)
	if errors.Is(err, sql.ErrNoRows) {
		// No latest-inventory row yet: nothing to dedup against.
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("compare current observation for %s: %w", uid, err)
	}
	return differs, nil
}

// mergeLabels applies set/delete deltas onto a base label map without
// mutating it, returning the merged result.
func mergeLabels(current, set map[string]string, deleteKeys []string) map[string]string {
	merged := make(map[string]string, len(current)+len(set))
	for k, v := range current {
		merged[k] = v
	}
	for k, v := range set {
		merged[k] = v
	}
	for _, k := range deleteKeys {
		delete(merged, k)
	}
	return merged
}

// deleteConditionsAbsentFrom removes latest condition rows whose type
// is not present in keep, implementing ReplaceInventory's "the
// replacement is the complete current condition set" semantics. No
// transition row is recorded for the removal (per the rework doc's
// condition transition rules).
func (r *ExtensionResourceRepo) deleteConditionsAbsentFrom(ctx context.Context, uid domain.ExtensionResourceUID, conditions []domain.Condition) error {
	if len(conditions) == 0 {
		_, err := r.DB.ExecContext(ctx,
			`DELETE FROM extension_resource_inventory_conditions WHERE extension_resource_uid = $1`,
			uid.String())
		return err
	}
	args := make([]any, 0, len(conditions)+1)
	args = append(args, uid.String())
	placeholders := make([]string, len(conditions))
	for i, c := range conditions {
		args = append(args, string(c.Type()))
		placeholders[i] = fmt.Sprintf("$%d", i+2)
	}
	_, err := r.DB.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM extension_resource_inventory_conditions
			WHERE extension_resource_uid = $1 AND type NOT IN (%s)`, strings.Join(placeholders, ", ")),
		args...)
	return err
}

// deleteConditionsByType removes the named latest condition rows. No
// transition row is recorded (per the rework doc's condition
// transition rules: deletion isn't a transition in this pass).
func (r *ExtensionResourceRepo) deleteConditionsByType(ctx context.Context, uid domain.ExtensionResourceUID, types []domain.ConditionType) error {
	for _, t := range types {
		if _, err := r.DB.ExecContext(ctx,
			`DELETE FROM extension_resource_inventory_conditions WHERE extension_resource_uid = $1 AND type = $2`,
			uid.String(), string(t)); err != nil {
			return err
		}
	}
	return nil
}

// appendObservationHistory inserts an append-only observation history
// row, generating its ID internally (reporters never supply one).
func (r *ExtensionResourceRepo) appendObservationHistory(ctx context.Context, uid domain.ExtensionResourceUID, observation json.RawMessage, observedAt, receivedAt time.Time) error {
	id := domain.NewObservationID()
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_inventory_observations
			(id, extension_resource_uid, observation, observed_at, created_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		string(id), uid.String(), string(observation),
		observedAt.UTC(), receivedAt.UTC())
	return err
}

// ReplaceInventory implements [domain.ExtensionResourceRepository.ReplaceInventory].
// See the "ReplaceInventory Repository Semantics" section of the
// inventory report contract rework plan for the per-replacement steps.
func (r *ExtensionResourceRepo) ReplaceInventory(ctx context.Context, replacements []domain.InventoryReplacement) error {
	for _, rep := range replacements {
		if err := r.replaceInventoryOne(ctx, rep); err != nil {
			return fmt.Errorf("replace inventory for %s: %w", rep.ExtensionResourceUID, err)
		}
	}
	return nil
}

func (r *ExtensionResourceRepo) replaceInventoryOne(ctx context.Context, rep domain.InventoryReplacement) error {
	uid := rep.ExtensionResourceUID
	obs := normalizeObservation(rep.Observation)

	appendHistory, err := r.observationDiffersFromLatest(ctx, uid, obs)
	if err != nil {
		return err
	}

	// The INSERT here enforces the extension_resources foreign key, so
	// an unknown UID fails before any conditions/history are touched.
	if err := r.upsertInventoryLatestRow(ctx, uid, rep.Labels, obs, rep.ObservedAt, rep.ReceivedAt); err != nil {
		return fmt.Errorf("upsert latest inventory: %w", err)
	}

	if err := r.deleteConditionsAbsentFrom(ctx, uid, rep.Conditions); err != nil {
		return fmt.Errorf("delete absent conditions: %w", err)
	}
	for _, c := range rep.Conditions {
		if err := r.recordCondition(ctx, uid,
			c.Type(), c.Status(), c.Reason(), c.Message(),
			c.LastTransitionTime(), rep.ObservedAt, rep.ReceivedAt); err != nil {
			return fmt.Errorf("upsert condition: %w", err)
		}
	}

	if appendHistory {
		if err := r.appendObservationHistory(ctx, uid, *obs, rep.ObservedAt, rep.ReceivedAt); err != nil {
			return fmt.Errorf("append observation history: %w", err)
		}
	}
	return nil
}

// ApplyInventoryDeltas implements [domain.ExtensionResourceRepository.ApplyInventoryDeltas].
// See the "ApplyInventoryDeltas Repository Semantics" section of the
// inventory report contract rework plan for the per-delta steps.
func (r *ExtensionResourceRepo) ApplyInventoryDeltas(ctx context.Context, deltas []domain.InventoryDelta) error {
	for _, d := range deltas {
		if err := r.applyInventoryDeltaOne(ctx, d); err != nil {
			return fmt.Errorf("apply inventory delta for %s: %w", d.ExtensionResourceUID, err)
		}
	}
	return nil
}

func (r *ExtensionResourceRepo) applyInventoryDeltaOne(ctx context.Context, d domain.InventoryDelta) error {
	uid := d.ExtensionResourceUID
	obs := normalizeObservation(d.Observation)

	// The INSERT here enforces the extension_resources foreign key, so
	// an unknown UID fails before any conditions/history are touched.
	if err := r.ensureInventoryRowExists(ctx, uid, d.ObservedAt, d.ReceivedAt); err != nil {
		return fmt.Errorf("ensure latest inventory row: %w", err)
	}

	labels, err := r.currentInventoryLabels(ctx, uid)
	if err != nil {
		return err
	}
	labels = mergeLabels(labels, d.SetLabels, d.DeleteLabels)

	appendHistory, err := r.observationDiffersFromLatest(ctx, uid, obs)
	if err != nil {
		return err
	}

	if err := r.upsertInventoryLatestRow(ctx, uid, labels, obs, d.ObservedAt, d.ReceivedAt); err != nil {
		return fmt.Errorf("upsert latest inventory: %w", err)
	}
	if appendHistory {
		if err := r.appendObservationHistory(ctx, uid, *obs, d.ObservedAt, d.ReceivedAt); err != nil {
			return fmt.Errorf("append observation history: %w", err)
		}
	}

	for _, c := range d.UpsertConditions {
		if err := r.recordCondition(ctx, uid,
			c.Type(), c.Status(), c.Reason(), c.Message(),
			c.LastTransitionTime(), d.ObservedAt, d.ReceivedAt); err != nil {
			return fmt.Errorf("upsert condition: %w", err)
		}
	}
	if err := r.deleteConditionsByType(ctx, uid, d.DeleteConditions); err != nil {
		return fmt.Errorf("delete conditions: %w", err)
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
