package sqlite

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

	var invJSON sql.NullString
	if snap.Inventory != nil {
		invJSON = sql.NullString{String: "{}", Valid: true}
	}

	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_types (service_name, type_name, api_version, collection_id, management, inventory, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		string(snap.ResourceType.ServiceName()), snap.ResourceType.TypeName(),
		string(snap.APIVersion),
		string(snap.CollectionID),
		mgmtJSON,
		invJSON,
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
		`SELECT service_name, type_name, api_version, collection_id, management, inventory, created_at, updated_at
		 FROM extension_resource_types WHERE service_name = ? AND type_name = ?`,
		string(rt.ServiceName()), rt.TypeName())
	return r.scanType(row)
}

func (r *ExtensionResourceRepo) ListTypes(ctx context.Context) ([]domain.ExtensionResourceType, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT service_name, type_name, api_version, collection_id, management, inventory, created_at, updated_at
		 FROM extension_resource_types ORDER BY service_name, type_name`)
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
		`DELETE FROM extension_resource_types WHERE service_name = ? AND type_name = ?`,
		string(rt.ServiceName()), rt.TypeName())
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

	labelsJSON, err := json.Marshal(s.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO extension_resources (uid, service_name, type_name, resource_name, labels, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		s.UID.String(), string(s.ResourceType.ServiceName()), s.ResourceType.TypeName(), s.Name,
		string(labelsJSON),
		s.CreatedAt.UTC().Format(time.RFC3339Nano),
		s.UpdatedAt.UTC().Format(time.RFC3339Nano))
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: extension resource %s/%s", domain.ErrAlreadyExists, s.ResourceType.ServiceName(), s.Name)
		}
		return err
	}

	// Flush pending intents keyed by extension resource UID.
	for _, intent := range s.PendingIntents {
		if _, err := r.DB.ExecContext(ctx,
			`INSERT INTO resource_intents (extension_resource_uid, version, spec, created_at)
			 VALUES (?, ?, ?, ?)`,
			intent.ExtensionResourceUID.String(), intent.Version, string(intent.Spec),
			intent.CreatedAt.UTC().Format(time.RFC3339Nano)); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("%w: intent %s v%d", domain.ErrAlreadyExists, intent.ExtensionResourceUID, intent.Version)
			}
			return err
		}
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

	// Insert inventory state row if present.
	if s.Inventory != nil {
		if err := r.insertInventory(ctx, s.UID, s.Inventory); err != nil {
			return fmt.Errorf("insert inventory state: %w", err)
		}
	}

	return nil
}

// erInstanceQuerySQLite is the shared SELECT + FROM + JOINs for
// instance aggregate reads. Callers append a WHERE clause.
var erInstanceQuerySQLite = `SELECT er.uid, er.service_name, er.type_name, er.resource_name, er.labels, er.created_at, er.updated_at,
	m.current_version, m.fulfillment_id,
	inv.labels, inv.observation, inv.observed_at, inv.updated_at,
	(SELECT json_group_array(json_object(
		'type', c.type,
		'status', c.status,
		'reason', c.reason,
		'message', c.message,
		'last_transition_time', c.last_transition_time
	))
	 FROM (SELECT type, status, reason, message, last_transition_time
	       FROM extension_resource_inventory_conditions
	       WHERE extension_resource_uid = er.uid
	       ORDER BY type) c) AS inv_conditions
FROM extension_resources er
LEFT JOIN extension_resource_managed m ON m.extension_resource_uid = er.uid
LEFT JOIN extension_resource_inventory inv ON inv.extension_resource_uid = er.uid
`

func (r *ExtensionResourceRepo) Get(ctx context.Context, name domain.FullResourceName) (*domain.ExtensionResource, error) {
	row := r.DB.QueryRowContext(ctx,
		erInstanceQuerySQLite+`WHERE er.service_name = ? AND er.resource_name = ?`,
		string(name.ServiceName()), string(name.ResourceName()))
	return r.scanInstance(row)
}

func (r *ExtensionResourceRepo) GetByUID(ctx context.Context, uid domain.ExtensionResourceUID) (*domain.ExtensionResource, error) {
	row := r.DB.QueryRowContext(ctx,
		erInstanceQuerySQLite+`WHERE er.uid = ?`, uid.String())
	return r.scanInstance(row)
}

func (r *ExtensionResourceRepo) ListByResourceType(ctx context.Context, rt domain.ResourceType) ([]*domain.ExtensionResource, error) {
	rows, err := r.DB.QueryContext(ctx,
		erInstanceQuerySQLite+`WHERE er.service_name = ? AND er.type_name = ? ORDER BY er.resource_name`,
		string(rt.ServiceName()), rt.TypeName())
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

func (r *ExtensionResourceRepo) Delete(ctx context.Context, name domain.FullResourceName) error {
	res, err := r.DB.ExecContext(ctx,
		`DELETE FROM extension_resources WHERE service_name = ? AND resource_name = ?`,
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
	q := erViewQuerySQLite + `
	WHERE er.service_name = ? AND er.resource_name = ?`
	row := r.DB.QueryRowContext(ctx, q, string(name.ServiceName()), string(name.ResourceName()))
	return r.scanView(row)
}

func (r *ExtensionResourceRepo) ListViewsByType(ctx context.Context, rt domain.ResourceType) ([]domain.ExtensionResourceView, error) {
	q := erViewQuerySQLite + `
	WHERE er.service_name = ? AND er.type_name = ? ORDER BY er.resource_name`
	rows, err := r.DB.QueryContext(ctx, q, string(rt.ServiceName()), rt.TypeName())
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

var erViewQuerySQLite = `SELECT
	er.uid, er.service_name, er.type_name, er.resource_name, er.labels, er.created_at, er.updated_at,
	m.current_version, m.fulfillment_id,
	ri.spec, ri.created_at,
	` + fulfillmentColumnsJoined("f") + `,
	inv.labels, inv.observation, inv.observed_at, inv.updated_at,
	(SELECT json_group_array(json_object(
		'type', c.type,
		'status', c.status,
		'reason', c.reason,
		'message', c.message,
		'last_transition_time', c.last_transition_time
	))
	 FROM (SELECT type, status, reason, message, last_transition_time
	       FROM extension_resource_inventory_conditions
	       WHERE extension_resource_uid = er.uid
	       ORDER BY type) c) AS inv_conditions
FROM extension_resources er
LEFT JOIN extension_resource_managed m ON m.extension_resource_uid = er.uid
LEFT JOIN resource_intents ri
  ON ri.extension_resource_uid = er.uid AND ri.version = m.current_version
LEFT JOIN fulfillments f ON f.id = m.fulfillment_id
` + strategyJoins("f") + `
LEFT JOIN extension_resource_inventory inv ON inv.extension_resource_uid = er.uid
`

// ---------------------------------------------------------------------------
// Intents
// ---------------------------------------------------------------------------

func (r *ExtensionResourceRepo) GetIntent(ctx context.Context, uid domain.ExtensionResourceUID, version domain.IntentVersion) (domain.ResourceIntent, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT extension_resource_uid, version, spec, created_at
		 FROM resource_intents WHERE extension_resource_uid = ? AND version = ?`,
		uid.String(), version)
	var ri domain.ResourceIntent
	var uidStr, specStr, createdAt string
	if err := row.Scan(&uidStr, &ri.Version, &specStr, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ResourceIntent{}, fmt.Errorf("%w: intent %s v%d", domain.ErrNotFound, uid, version)
		}
		return domain.ResourceIntent{}, err
	}
	parsedUID, err := domain.ParseExtensionResourceUID(uidStr)
	if err != nil {
		return domain.ResourceIntent{}, err
	}
	ri.ExtensionResourceUID = parsedUID
	ri.Spec = json.RawMessage(specStr)
	if t, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
		ri.CreatedAt = t
	}
	return ri, nil
}

// ---------------------------------------------------------------------------
// Scan helpers
// ---------------------------------------------------------------------------

func (r *ExtensionResourceRepo) scanType(s interface{ Scan(...any) error }) (domain.ExtensionResourceType, error) {
	var serviceName, typeName, apiVersion, collectionID, createdAt, updatedAt string
	var mgmtJSON, invJSON sql.NullString
	if err := s.Scan(&serviceName, &typeName, &apiVersion, &collectionID, &mgmtJSON, &invJSON, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ExtensionResourceType{}, domain.ErrNotFound
		}
		return domain.ExtensionResourceType{}, err
	}

	snap := domain.ExtensionResourceTypeSnapshot{
		ResourceType: domain.ResourceType(serviceName + "/" + typeName),
		APIVersion:   domain.APIVersion(apiVersion),
		CollectionID: domain.CollectionID(collectionID),
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
	if invJSON.Valid {
		snap.Inventory = &domain.InventoryTypeSnapshot{}
	}
	return domain.ExtensionResourceTypeFromSnapshot(snap), nil
}

func (r *ExtensionResourceRepo) scanInstance(s interface{ Scan(...any) error }) (*domain.ExtensionResource, error) {
	var uidStr, serviceName, typeName, nameStr, labelsJSON, createdAt, updatedAt string
	var mVersion sql.NullInt64
	var mFulfillmentID sql.NullString
	var invLabels, invObservation, invObservedAt, invUpdatedAt sql.NullString
	var invConditionsJSON sql.NullString

	if err := s.Scan(&uidStr, &serviceName, &typeName, &nameStr, &labelsJSON, &createdAt, &updatedAt,
		&mVersion, &mFulfillmentID,
		&invLabels, &invObservation, &invObservedAt, &invUpdatedAt,
		&invConditionsJSON); err != nil {
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
		ResourceType: domain.ResourceType(serviceName + "/" + typeName),
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
	if invObservedAt.Valid {
		invSnap := domain.InventoryResourceSnapshot{
			Labels: map[string]string{},
		}
		if invLabels.Valid {
			json.Unmarshal([]byte(invLabels.String), &invSnap.Labels)
		}
		if invObservation.Valid {
			invSnap.Observation = json.RawMessage(invObservation.String)
		}
		if t, err := time.Parse(time.RFC3339Nano, invObservedAt.String); err == nil {
			invSnap.ObservedAt = t
		}
		if invUpdatedAt.Valid {
			if t, err := time.Parse(time.RFC3339Nano, invUpdatedAt.String); err == nil {
				invSnap.UpdatedAt = t
			}
		}
		if invConditionsJSON.Valid {
			invSnap.Conditions, _ = unmarshalConditionSnapshots([]byte(invConditionsJSON.String))
		}
		snap.Inventory = &invSnap
	}
	return domain.ExtensionResourceFromSnapshot(snap), nil
}

func (r *ExtensionResourceRepo) scanView(s interface{ Scan(...any) error }) (domain.ExtensionResourceView, error) {
	var uidStr, serviceName, typeName, nameStr, labelsJSON, erCreatedAt, erUpdatedAt string
	var mVersion sql.NullInt64
	var mFulfillmentID sql.NullString
	var riSpec, riCreatedAt sql.NullString
	var fCols nullableFulfillmentScanColumns

	// Inventory columns (all nullable)
	var invLabels, invObservation, invObservedAt, invUpdatedAt sql.NullString
	var invConditionsJSON sql.NullString

	if err := s.Scan(append(append([]any{
		&uidStr, &serviceName, &typeName, &nameStr, &labelsJSON, &erCreatedAt, &erUpdatedAt,
		&mVersion, &mFulfillmentID,
		&riSpec, &riCreatedAt,
	}, fCols.dests()...),
		&invLabels, &invObservation, &invObservedAt, &invUpdatedAt,
		&invConditionsJSON,
	)...); err != nil {
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
		ResourceType: domain.ResourceType(serviceName + "/" + typeName),
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

	// Inventory: include in snapshot so ExtensionResourceFromSnapshot
	// hydrates Resource.Inventory().
	if invObservedAt.Valid {
		invSnap := domain.InventoryResourceSnapshot{
			Labels: map[string]string{},
		}
		if invLabels.Valid {
			json.Unmarshal([]byte(invLabels.String), &invSnap.Labels)
		}
		if invObservation.Valid {
			invSnap.Observation = json.RawMessage(invObservation.String)
		}
		if t, err := time.Parse(time.RFC3339Nano, invObservedAt.String); err == nil {
			invSnap.ObservedAt = t
		}
		if invUpdatedAt.Valid {
			if t, err := time.Parse(time.RFC3339Nano, invUpdatedAt.String); err == nil {
				invSnap.UpdatedAt = t
			}
		}
		if invConditionsJSON.Valid {
			invSnap.Conditions, _ = unmarshalConditionSnapshots([]byte(invConditionsJSON.String))
		}
		snap.Inventory = &invSnap
	}

	resource := domain.ExtensionResourceFromSnapshot(snap)

	var v domain.ExtensionResourceView
	v.Resource = *resource

	// Intent and fulfillment are only populated for managed resources.
	if riSpec.Valid {
		intent := &domain.ResourceIntent{
			ExtensionResourceUID: resource.UID(),
			Spec:                 json.RawMessage(riSpec.String),
		}
		if resource.Managed() != nil {
			intent.Version = resource.Managed().CurrentVersion()
		}
		if riCreatedAt.Valid {
			if t, err := time.Parse(time.RFC3339Nano, riCreatedAt.String); err == nil {
				intent.CreatedAt = t
			}
		}
		v.Intent = intent
	}

	if fCols.isPresent() {
		fs, err := fCols.snapshot()
		if err != nil {
			return domain.ExtensionResourceView{}, err
		}
		v.Fulfillment = domain.FulfillmentFromSnapshot(fs)
	}

	return v, nil
}

// conditionRow mirrors the JSON shape produced by the json_group_array subquery.
type conditionRow struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	Reason             string `json:"reason"`
	Message            string `json:"message"`
	LastTransitionTime string `json:"last_transition_time"`
}

func unmarshalConditionSnapshots(data []byte) ([]domain.ConditionSnapshot, error) {
	var rows []conditionRow
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	snaps := make([]domain.ConditionSnapshot, len(rows))
	for i, r := range rows {
		snaps[i] = domain.ConditionSnapshot{
			Type:    domain.ConditionType(r.Type),
			Status:  domain.ConditionStatus(r.Status),
			Reason:  r.Reason,
			Message: r.Message,
		}
		if t, err := time.Parse(time.RFC3339Nano, r.LastTransitionTime); err == nil {
			snaps[i].LastTransitionTime = t
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
// literal string rather than preserving latest.
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
	var obsArg any
	if observation != nil {
		obsArg = string(*observation)
	}
	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_inventory
			(extension_resource_uid, labels, observation, observed_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(extension_resource_uid) DO UPDATE SET
			labels = excluded.labels,
			observation = COALESCE(excluded.observation, extension_resource_inventory.observation),
			observed_at = excluded.observed_at,
			updated_at = excluded.updated_at`,
		uid.String(), labelsJSON, obsArg,
		observedAt.UTC().Format(time.RFC3339Nano),
		updatedAt.UTC().Format(time.RFC3339Nano))
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
		 VALUES (?, '{}', NULL, ?, ?)
		 ON CONFLICT (extension_resource_uid) DO NOTHING`,
		uid.String(), observedAt.UTC().Format(time.RFC3339Nano), updatedAt.UTC().Format(time.RFC3339Nano))
	return err
}

// currentInventoryLabels reads the current latest labels for uid, for
// the read-modify-write merge that
// [ExtensionResourceRepo.ApplyInventoryDeltas] needs. Callers must
// ensure the row exists first.
func (r *ExtensionResourceRepo) currentInventoryLabels(ctx context.Context, uid domain.ExtensionResourceUID) (map[string]string, error) {
	var labelsJSON string
	if err := r.DB.QueryRowContext(ctx,
		`SELECT labels FROM extension_resource_inventory WHERE extension_resource_uid = ?`,
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

// currentObservation reads the current latest observation for uid, so
// [ExtensionResourceRepo.replaceInventoryOne] and
// [ExtensionResourceRepo.applyInventoryDeltaOne] can dedup a supplied
// observation against it before appending history. A missing row
// (uid not yet given a latest-inventory row) and a NULL observation
// column both report as no current observation.
func (r *ExtensionResourceRepo) currentObservation(ctx context.Context, uid domain.ExtensionResourceUID) (*json.RawMessage, error) {
	var obs sql.NullString
	err := r.DB.QueryRowContext(ctx,
		`SELECT observation FROM extension_resource_inventory WHERE extension_resource_uid = ?`,
		uid.String()).Scan(&obs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read current observation for %s: %w", uid, err)
	}
	if !obs.Valid {
		return nil, nil
	}
	raw := json.RawMessage(obs.String)
	return &raw, nil
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
			`DELETE FROM extension_resource_inventory_conditions WHERE extension_resource_uid = ?`,
			uid.String())
		return err
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(conditions)), ",")
	args := make([]any, 0, len(conditions)+1)
	args = append(args, uid.String())
	for _, c := range conditions {
		args = append(args, string(c.Type()))
	}
	_, err := r.DB.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM extension_resource_inventory_conditions
			WHERE extension_resource_uid = ? AND type NOT IN (%s)`, placeholders),
		args...)
	return err
}

// deleteConditionsByType removes the named latest condition rows. No
// transition row is recorded (per the rework doc's condition
// transition rules: deletion isn't a transition in this pass).
func (r *ExtensionResourceRepo) deleteConditionsByType(ctx context.Context, uid domain.ExtensionResourceUID, types []domain.ConditionType) error {
	for _, t := range types {
		if _, err := r.DB.ExecContext(ctx,
			`DELETE FROM extension_resource_inventory_conditions WHERE extension_resource_uid = ? AND type = ?`,
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
		 VALUES (?, ?, ?, ?, ?)`,
		string(id), uid.String(), string(observation),
		observedAt.UTC().Format(time.RFC3339Nano),
		receivedAt.UTC().Format(time.RFC3339Nano))
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

// observationDiffersFromLatest reports whether obs (already
// normalized via [normalizeObservation]) is a real value that differs
// from the current latest observation for uid, i.e. whether it should
// append a new observation history row. A nil obs always reports
// false without reading anything.
func (r *ExtensionResourceRepo) observationDiffersFromLatest(ctx context.Context, uid domain.ExtensionResourceUID, obs *json.RawMessage) (bool, error) {
	if obs == nil {
		return false, nil
	}
	current, err := r.currentObservation(ctx, uid)
	if err != nil {
		return false, err
	}
	return current == nil || !bytes.Equal(*current, *obs), nil
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
		 WHERE extension_resource_uid = ?
		 ORDER BY observed_at DESC
		 LIMIT ?`,
		uid.String(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.Observation
	for rows.Next() {
		var idStr, erUID, obsJSON, observedAt, createdAt string
		if err := rows.Scan(&idStr, &erUID, &obsJSON, &observedAt, &createdAt); err != nil {
			return nil, err
		}
		parsedUID, err := domain.ParseExtensionResourceUID(erUID)
		if err != nil {
			return nil, err
		}
		snap := domain.ObservationSnapshot{
			ID:                   domain.ObservationID(idStr),
			ExtensionResourceUID: parsedUID,
			Observation:          json.RawMessage(obsJSON),
		}
		if t, err := time.Parse(time.RFC3339Nano, observedAt); err == nil {
			snap.ObservedAt = t
		}
		if t, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
			snap.CreatedAt = t
		}
		result = append(result, domain.ObservationFromSnapshot(snap))
	}
	return result, rows.Err()
}

// recordCondition is the shared condition recording path. It:
//  1. Reads the current latest condition state
//  2. UPSERTs the latest condition state (always, for staleness tracking)
//  3. INSERTs a transition record only if the condition actually changed
//
// SQLite does not support writable CTEs, so this is a multi-step
// approach with the same semantics as the Postgres CTE version.
func (r *ExtensionResourceRepo) recordCondition(
	ctx context.Context,
	uid domain.ExtensionResourceUID,
	condType domain.ConditionType,
	status domain.ConditionStatus,
	reason, message string,
	lastTransitionTime, observedAt, now time.Time,
) error {
	uidStr := uid.String()
	ctStr := string(condType)
	statusStr := string(status)
	nowStr := now.Format(time.RFC3339Nano)
	lttStr := lastTransitionTime.UTC().Format(time.RFC3339Nano)
	obsStr := observedAt.UTC().Format(time.RFC3339Nano)

	// Step 1: Read current state
	var prevStatus, prevReason, prevMessage string
	err := r.DB.QueryRowContext(ctx,
		`SELECT status, reason, message FROM extension_resource_inventory_conditions
		 WHERE extension_resource_uid = ? AND type = ?`,
		uidStr, ctStr).Scan(&prevStatus, &prevReason, &prevMessage)
	changed := err == sql.ErrNoRows || prevStatus != statusStr || prevReason != reason || prevMessage != message
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("read latest condition %s/%s: %w", uid, condType, err)
	}

	// Step 2: Always UPSERT latest state
	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_inventory_conditions
			(extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (extension_resource_uid, type) DO UPDATE SET
			status = excluded.status,
			reason = excluded.reason,
			message = excluded.message,
			last_transition_time = excluded.last_transition_time,
			observed_at = excluded.observed_at,
			updated_at = excluded.updated_at`,
		uidStr, ctStr, statusStr, reason, message, lttStr, obsStr, nowStr)
	if err != nil {
		return fmt.Errorf("upsert condition %s/%s: %w", uid, condType, err)
	}

	// Step 3: Insert transition only if state changed
	if changed {
		id := uuid.New().String()
		_, err = r.DB.ExecContext(ctx,
			`INSERT INTO extension_resource_inventory_condition_events
				(id, extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id, uidStr, ctStr, statusStr, reason, message, lttStr, obsStr, nowStr)
		if err != nil {
			return fmt.Errorf("insert condition transition %s/%s: %w", uid, condType, err)
		}
	}
	return nil
}

func (r *ExtensionResourceRepo) ListConditionTransitions(ctx context.Context, uid domain.ExtensionResourceUID, conditionType *domain.ConditionType, limit int) ([]domain.ConditionTransition, error) {
	var q string
	var args []any
	if conditionType != nil {
		q = `SELECT id, extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, created_at
			 FROM extension_resource_inventory_condition_events
			 WHERE extension_resource_uid = ? AND type = ?
			 ORDER BY observed_at DESC
			 LIMIT ?`
		args = []any{uid.String(), string(*conditionType), limit}
	} else {
		q = `SELECT id, extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, created_at
			 FROM extension_resource_inventory_condition_events
			 WHERE extension_resource_uid = ?
			 ORDER BY observed_at DESC
			 LIMIT ?`
		args = []any{uid.String(), limit}
	}
	rows, err := r.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.ConditionTransition
	for rows.Next() {
		var idStr, erUID, ctStr, statusStr, reason, message, ltt, observedAt, createdAt string
		if err := rows.Scan(&idStr, &erUID, &ctStr, &statusStr, &reason, &message, &ltt, &observedAt, &createdAt); err != nil {
			return nil, err
		}
		parsedUID, err := domain.ParseExtensionResourceUID(erUID)
		if err != nil {
			return nil, err
		}
		snap := domain.ConditionTransitionSnapshot{
			ID:                   domain.ConditionTransitionID(idStr),
			ExtensionResourceUID: parsedUID,
			ConditionType:        domain.ConditionType(ctStr),
			Status:               domain.ConditionStatus(statusStr),
			Reason:               reason,
			Message:              message,
		}
		if t, err := time.Parse(time.RFC3339Nano, ltt); err == nil {
			snap.LastTransitionTime = t
		}
		if t, err := time.Parse(time.RFC3339Nano, observedAt); err == nil {
			snap.ObservedAt = t
		}
		if t, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
			snap.CreatedAt = t
		}
		result = append(result, domain.ConditionTransitionFromSnapshot(snap))
	}
	return result, rows.Err()
}

// nullableFulfillmentScanColumns is like [fulfillmentScanColumns] but
// uses sql.Null* types for all fields so it can handle LEFT JOIN rows
// where the fulfillment is NULL.
type nullableFulfillmentScanColumns struct {
	id, rtJSON, stateStr, pauseReason, statusReason, authJSON, createdAtStr, updatedAtStr sql.NullString
	msSpec, psSpec, rsSpec, provJSON, attestRefJSON                                       sql.NullString
	msVer, psVer, rsVer, generation, observedGeneration                                   sql.NullInt64
	activeWorkflowGen                                                                     sql.NullInt64
}

func (c *nullableFulfillmentScanColumns) dests() []any {
	return []any{
		&c.id, &c.msVer, &c.msSpec, &c.psVer, &c.psSpec, &c.rsVer, &c.rsSpec,
		&c.rtJSON, &c.stateStr, &c.pauseReason, &c.statusReason, &c.authJSON, &c.provJSON, &c.attestRefJSON,
		&c.generation, &c.observedGeneration, &c.activeWorkflowGen,
		&c.createdAtStr, &c.updatedAtStr,
	}
}

func (c *nullableFulfillmentScanColumns) isPresent() bool {
	return c.id.Valid
}

func (c *nullableFulfillmentScanColumns) snapshot() (domain.FulfillmentSnapshot, error) {
	fc := fulfillmentScanColumns{
		id:                 c.id.String,
		rtJSON:             c.rtJSON.String,
		stateStr:           c.stateStr.String,
		pauseReason:        c.pauseReason.String,
		statusReason:       c.statusReason.String,
		authJSON:           c.authJSON.String,
		createdAtStr:       c.createdAtStr.String,
		updatedAtStr:       c.updatedAtStr.String,
		msSpec:             c.msSpec,
		psSpec:             c.psSpec,
		rsSpec:             c.rsSpec,
		provJSON:           c.provJSON,
		attestRefJSON:      c.attestRefJSON,
		msVer:              c.msVer.Int64,
		psVer:              c.psVer.Int64,
		rsVer:              c.rsVer.Int64,
		generation:         c.generation.Int64,
		observedGeneration: c.observedGeneration.Int64,
		activeWorkflowGen:  c.activeWorkflowGen,
	}
	return fc.snapshot()
}
