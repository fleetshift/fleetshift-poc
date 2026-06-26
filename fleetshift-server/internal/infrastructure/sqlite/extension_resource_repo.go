package sqlite

import (
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
		`INSERT INTO extension_resource_types (resource_type, api_version, collection_id, management, inventory, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		snap.ResourceType,
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
		`SELECT resource_type, api_version, collection_id, management, inventory, created_at, updated_at
		 FROM extension_resource_types WHERE resource_type = ?`, rt)
	return r.scanType(row)
}

func (r *ExtensionResourceRepo) ListTypes(ctx context.Context) ([]domain.ExtensionResourceType, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT resource_type, api_version, collection_id, management, inventory, created_at, updated_at
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

	// Insert inventory state row if present.
	if s.Inventory != nil {
		if err := r.insertInventory(ctx, s.UID, s.Inventory); err != nil {
			return fmt.Errorf("insert inventory state: %w", err)
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
	q := erViewQuerySQLite + `
	WHERE er.resource_type = ? AND er.resource_name = ?`
	row := r.DB.QueryRowContext(ctx, q, rt, name)
	return r.scanView(row)
}

func (r *ExtensionResourceRepo) ListViewsByType(ctx context.Context, rt domain.ResourceType) ([]domain.ExtensionResourceView, error) {
	q := erViewQuerySQLite + `
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

var erViewQuerySQLite = `SELECT
	er.uid, er.resource_type, er.resource_name, er.labels, er.created_at, er.updated_at,
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
  ON ri.resource_type = er.resource_type AND ri.name = er.resource_name AND ri.version = m.current_version
LEFT JOIN fulfillments f ON f.id = m.fulfillment_id
` + strategyJoins("f") + `
LEFT JOIN extension_resource_inventory inv ON inv.extension_resource_uid = er.uid
`

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
	var rtStr, apiVersion, collectionID, createdAt, updatedAt string
	var mgmtJSON, invJSON sql.NullString
	if err := s.Scan(&rtStr, &apiVersion, &collectionID, &mgmtJSON, &invJSON, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ExtensionResourceType{}, domain.ErrNotFound
		}
		return domain.ExtensionResourceType{}, err
	}

	snap := domain.ExtensionResourceTypeSnapshot{
		ResourceType: domain.ResourceType(rtStr),
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
	var riSpec, riCreatedAt sql.NullString
	var fCols nullableFulfillmentScanColumns

	// Inventory columns (all nullable)
	var invLabels, invObservation, invObservedAt, invUpdatedAt sql.NullString
	var invConditionsJSON sql.NullString

	if err := s.Scan(append(append([]any{
		&uidStr, &rtStr, &nameStr, &labelsJSON, &erCreatedAt, &erUpdatedAt,
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

	var v domain.ExtensionResourceView
	v.Resource = *resource

	// Intent and fulfillment are only populated for managed resources.
	if riSpec.Valid {
		intent := &domain.ResourceIntent{
			ResourceType: resource.ResourceType(),
			Name:         resource.Name(),
			Spec:         json.RawMessage(riSpec.String),
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

	// Inventory: build snapshot including conditions, hydrate once.
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
		v.Inventory = domain.InventoryResourceFromSnapshot(invSnap)
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

func (r *ExtensionResourceRepo) upsertInventoryRow(ctx context.Context, uid domain.ExtensionResourceUID, inv *domain.InventoryResourceSnapshot) error {
	labelsJSON, _ := json.Marshal(inv.Labels)
	obs := inv.Observation
	if obs == nil {
		obs = json.RawMessage("{}")
	}
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_inventory
			(extension_resource_uid, labels, observation, observed_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(extension_resource_uid) DO UPDATE SET
			labels = excluded.labels,
			observation = excluded.observation,
			observed_at = excluded.observed_at,
			updated_at = excluded.updated_at`,
		uid.String(), string(labelsJSON), string(obs),
		inv.ObservedAt.UTC().Format(time.RFC3339Nano),
		inv.UpdatedAt.UTC().Format(time.RFC3339Nano))
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
			 VALUES (?, ?, ?, ?, ?)`,
			string(o.ID()), o.ExtensionResourceUID().String(),
			string(obs),
			o.ObservedAt().UTC().Format(time.RFC3339Nano),
			o.CreatedAt().UTC().Format(time.RFC3339Nano))
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

func (r *ExtensionResourceRepo) RecordConditions(ctx context.Context, reports []domain.ConditionReport) error {
	if len(reports) == 0 {
		return nil
	}
	// Ensure a minimal inventory row exists so conditions can be
	// reported before a full inventory upsert.
	first := reports[0]
	if err := r.ensureInventoryRow(ctx, first.ExtensionResourceUID()); err != nil {
		return err
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
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_inventory
			(extension_resource_uid, labels, observation, observed_at, updated_at)
		 VALUES (?, '{}', '{}', ?, ?)
		 ON CONFLICT (extension_resource_uid) DO NOTHING`,
		uid.String(), now, now)
	return err
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
